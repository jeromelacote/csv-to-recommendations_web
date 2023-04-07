
from pymysql.converters import escape_string
import pymysql.cursors

import pandas as pd

import io
from datetime import datetime
import zipfile
import os
from PIL import Image, UnidentifiedImageError

import cloudinary
import cloudinary.uploader

import streamlit as st
import tqdm
from stqdm import stqdm

import shutil

def delete_folder(folder_path):
    """
    Deletes a folder and its contents.
    """
    try:
        shutil.rmtree(folder_path)
        print(f"The folder at {folder_path} has been deleted.")
    except OSError as e:
        print(f"Error deleting the folder at {folder_path}: {e}")


pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

CATEGORY_TO_IDX = {'hotel & accdn': 1, 'food & drinks': 2,
                   'experiences': 3, 'services': 4, 'product': 5}


def success_msg(
    msg): return f"<p class='small-font'><span class='{'success'}'>{msg}</span></p>"
def failure_msg(
    msg): return f"<p class='small-font'><span class='{'failure'}'>{msg}</span></p>"


def log_function(s): return st.markdown(s, unsafe_allow_html=True)


class DBService:

    def __init__(self,
                 host: str,
                 port: int,
                 username: str,
                 password: str,
                 db_name: str,
                 ) -> None:
        self.cursor = None
        self.conn = None
        self.configure_db(
            host,
            port,
            username,
            password,
            db_name,
        )

    def configure_db(self,
                     host,
                     port,
                     username,
                     password,
                     db_name,
                     ):

        if self.conn and self.cursor:
            return

        # DB setup
        self.conn = pymysql.connect(
            host=host,
            port=port,
            user=username,
            passwd=password,
            db=db_name,
            autocommit=True,
        )
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

    def insert_recommendation(self, data: tuple) -> bool:
        sql = "INSERT INTO `tbl_recommendation_v2` (`title`, `visit_date`, `date_added`, `item_id`, `user_id`, `category`, `sub_category`, `theme_name`, `type`, `reviewer_name`, `reviewer_photo_url`, `thumbnail_photo_url`, `url_link`, `address`, `filter_data`) VALUES ('%s', '%s', '%s', '%s', '1', '%s', '%s', 'Classic', 'external', '%s', '', '%s', '%s', '%s', '%s')" % data
        return self.cursor.execute(sql)

    def recommendation_exists(self, title: str) -> bool:
        select_sql = f" select title from tbl_recommendation_v2 where title like '%{title.strip()}%';"
        self.cursor.execute(select_sql)
        recommendation_row = self.cursor.fetchone()

        return bool(recommendation_row)


def resize_image(path: str):

    # Open the image
    img = Image.open(path)

    # Set the new size of the image
    new_size = (1000, 1000)

    # Resize the image
    resized_img = img.resize(new_size)

    new_path = f"resized_{path}"
    # Save the resized image
    resized_img.save(new_path)

    return new_path


def upload_to_cloudinary(path):

    # Upload image to Cloudinary
    result = cloudinary.uploader.upload(path,  upload_preset=ASSETS_CLOUD_UPLOAD_PRESET,
                                        unsigned=True)

    return result['secure_url']


def upload_image(picture_path: str):

    global images_list
    path: str = ""
    # Open image file in binary mode and add it to the data dictionary as a file object

    if pd.isna(picture_path):
        return ""

    for img_path in images_list:
        if picture_path.lower() in img_path.lower():
            path = f"images/{img_path.strip()}"
            break

    if path == "":
        return ""

    path = resize_image(path)
    url = upload_to_cloudinary(path)

    return url


def load_csv(filename: str = 'data.csv'):
    df = pd.read_csv(filename, dtype='category')
    df.columns = df.columns.str.replace(' ', '')

    df['index'] = df.index

    return df


def add_recommendation(row: dict) -> None:

    def escape_str(s): return escape_string(s) if s and type(s) == str else s
    def remove_nan(s): return '' if pd.isna(s) or s == 'nan' else s
    def parse_column(s): return remove_nan(escape_str(s))

    category: str = parse_column(row['Category'])
    sub_category: str = parse_column(row['SubCategory2'])
    filter_data: str = parse_column(row['Linkedto'])
    country: str = parse_column(row['Country'])
    title: str = parse_column(row['Title'])
    url: str = parse_column(row['URL'])
    author: str = parse_column(row['Author'])
    picture_path = row['picture-file-name.jpg']
    picture: str = parse_column(row['picture-file-name.jpg'])
    index: int = row["index"]
    if not title or not url:
        return

    if "http" not in url:
        log_function(failure_msg(
            f' Invalid url for "{title}", row number {index}'))
        return
    is_valid_row = any([not (pd.isna(v) or not v) for v in [
        category, sub_category, filter_data, country, title, url, author, picture
    ]])

    if not is_valid_row:
        return

    category_idx = CATEGORY_TO_IDX[category.lower()]

    recommendation_exists = db.recommendation_exists(title)

    if recommendation_exists:
        log_function(failure_msg(
            f' Recommendation with title: "{title}" already exists !, row number {index}'))
        return

    picture_url = upload_image(picture_path)

    if not picture_url:
        log_function(failure_msg(
            f' Invalid or missing picture path for "{title}" , row number {index}'))
        return

    current_date = datetime.now()

    data = (
        title,
        current_date.strftime("%Y-%m-%d"),
        int(current_date.timestamp()),
        0,
        category_idx,
        sub_category,
        author,
        picture_url,
        url,
        f"{filter_data}, {country}" if category_idx in [1, 2, 3, 4] else '',
        filter_data
    )

    if db.insert_recommendation(data):
        log_function(success_msg(
            f'  Recommendation with title: "{title}" was added to the db !, row number {index}'))


# Define function to extract images from compressed file
def extract_images(file):
    image_filenames = []
    with zipfile.ZipFile(file) as z:
        for name in z.namelist():
            with z.open(name) as f:
                try:
                    img = Image.open(io.BytesIO(f.read()))
                except UnidentifiedImageError as e:
                    log_function(failure_msg(
                    f'{name}: corrupted image!'))
                    continue
                image_filenames.append(name.split('/')[-1])
                # Save the image to a folder called 'image'
                img.save(f'images/{name.split("/")[-1]}')
    return image_filenames

# Define main function


def main():
    global images_list
    # Set page title
    st.set_page_config(page_title='CSV and Image Reader')
    st.markdown("""
    <style>
    .success {
        color: 	#198754;
        font-size: 14px;
    }
    .failure {
        color: 	#ed2939;
        font-size: 14px;
    }
    </style> """, unsafe_allow_html=True)
    # Define sidebar components
    csv_file = st.sidebar.file_uploader('Upload the CSV file containing the data', type='csv')
    img_file = st.sidebar.file_uploader(
        'Upload images zip file', type='zip')

    df: pd.DataFrame | None = None
    # Read CSV file using pandas
    if csv_file:
        df = load_csv(csv_file)

    # Extract images from compressed file
    if img_file:
        # Create a folder called 'image'
        if os.path.exists('images'):
            delete_folder('images')
        os.mkdir('images')
        

        if os.path.exists('resized_images'):
            delete_folder('resized_images')
        os.mkdir('resized_images')
        
        images_list = extract_images(img_file)

    if df is not None:
        st.write(df)

    if df is not None and len(images_list) > 0:
        create_recommendations_btn = st.sidebar.button(
            'Create recommendations')

        if create_recommendations_btn:
            stqdm.pandas(st_container=st.sidebar)
            # Do some work here that takes a while
            df.progress_apply(add_recommendation, axis=1)


# Run main function
if __name__ == '__main__':
    host = '184.168.107.73'
    port = 3306
    username = 'blaqbook_user'
    password = 'n!Ps^+,*L[vI'
    db_name = 'db_blaqbook_dev_v5'

    tqdm.tqdm.pandas()
    db = DBService(
        host,
        port,
        username,
        password,
        db_name,
    )
    images_list = []
    ASSETS_CLOUD_NAME = "dcdjempjd"
    ASSETS_CLOUD_UPLOAD_PRESET = 'unsigned_preset'
    # Set Cloudinary configuration
    cloudinary.config(
        cloud_name=ASSETS_CLOUD_NAME,
    )
    main()
