from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
import os
from airflow.models import Variable
from dotenv import load_dotenv, dotenv_values
import mysql.connector


# Load environment variables
env = dotenv_values('/home/daffaalfahryan/.env')

default_args = {
    'owner': 'Daffa',
    'start_date': datetime(2024, 6, 23),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# Define the DAG
dag = DAG(
    dag_id='ETL_RecyThing',
    default_args=default_args,
    description='ETL process for recything data and load to GCS and BigQuery',
    schedule_interval=timedelta(days=1),
)
def extract_all_tables_to_csv():
    try:
        with mysql.connector.connect(
            host = env["host"],
            port = 3306,
            user="root",
            password= env["password"],
            database="recything_db"
        ) as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            for table in tables:
                table_name = table[0]
                query = f"SELECT * FROM {table_name}"
                cursor.execute(query)
                rows = cursor.fetchall()

                if rows:
                    columns = [column[0] for column in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    data_dir = "/home/daffaalfahryan/data_database"
                    os.makedirs(data_dir, exist_ok=True)
                    file_path = os.path.join(data_dir, f"{table_name}.csv")
                    df.to_csv(file_path, index=False)
                else:
                    print(f"Table '{table_name}' is empty. No CSV file created.")

            print("Data extraction completed successfully")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

extract_all_tables_to_csv()

def transform_data():
    # Load all CSV files
    data_files = [
    ("about_us_images.csv", pd.read_csv("/home/daffaalfahryan/data_database/about_us_images.csv")),
    ("about_us.csv", pd.read_csv("/home/daffaalfahryan/data_database/about_us.csv")),
    ("achievements.csv", pd.read_csv("/home/daffaalfahryan/data_database/achievements.csv")),
    ("admins.csv", pd.read_csv("/home/daffaalfahryan/data_database/admins.csv")),
    ("article_categories.csv", pd.read_csv("/home/daffaalfahryan/data_database/article_categories.csv")),
    ("article_comments.csv", pd.read_csv("/home/daffaalfahryan/data_database/article_comments.csv")),
    ("article_sections.csv", pd.read_csv("/home/daffaalfahryan/data_database/article_sections.csv")),
    ("articles.csv", pd.read_csv("/home/daffaalfahryan/data_database/articles.csv")),
    ("comments.csv", pd.read_csv("/home/daffaalfahryan/data_database/comments.csv")),
    ("content_categories.csv", pd.read_csv("/home/daffaalfahryan/data_database/content_categories.csv")),
    ("custom_data.csv", pd.read_csv("/home/daffaalfahryan/data_database/custom_data.csv")),
    ("faqs.csv", pd.read_csv("/home/daffaalfahryan/data_database/faqs.csv")),
    ("report_images.csv", pd.read_csv("/home/daffaalfahryan/data_database/report_images.csv")),
    ("report_waste_materials.csv", pd.read_csv("/home/daffaalfahryan/data_database/report_waste_materials.csv")),
    ("reports.csv", pd.read_csv("/home/daffaalfahryan/data_database/reports.csv")),
    ("task_challenges.csv", pd.read_csv("/home/daffaalfahryan/data_database/task_challenges.csv")),
    ("task_steps.csv", pd.read_csv("/home/daffaalfahryan/data_database/task_steps.csv")),
    ("user_task_challenges.csv", pd.read_csv("/home/daffaalfahryan/data_database/user_task_challenges.csv")),
    ("user_task_images.csv", pd.read_csv("/home/daffaalfahryan/data_database/user_task_images.csv")),
    ("user_task_steps.csv", pd.read_csv("/home/daffaalfahryan/data_database/user_task_steps.csv")),
    ("users.csv", pd.read_csv("/home/daffaalfahryan/data_database/users.csv")),
    ("video_categories.csv", pd.read_csv("/home/daffaalfahryan/data_database/video_categories.csv")),
    ("videos.csv", pd.read_csv("/home/daffaalfahryan/data_database/videos.csv")),
    ("waste_categories.csv", pd.read_csv("/home/daffaalfahryan/data_database/waste_categories.csv")),
    ("waste_materials.csv", pd.read_csv("/home/daffaalfahryan/data_database/waste_materials.csv")),
    ]

    # Transform each dataset
    for (file_name, df) in data_files:
        # Convert date columns to datetime
        date_columns = ['created_at', 'updated_at', 'deleted_at']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Specific transformations for each table
        if file_name == "report_waste_materials.csv":
            mapping = {
                'MTR01': 'plastik', 'MTR02': 'kaca', 'MTR03': 'kayu', 'MTR04': 'kertas',
                'MTR05': 'baterai', 'MTR06': 'besi', 'MTR07': 'limbah berbahaya',
                'MTR08': 'limbah beracun', 'MTR09': 'sisa makanan', 'MTR10': 'tak terdeteksi'
            }
        if 'waste_material_id' in df.columns:
            df['waste_material'] = df['waste_material_id'].map(mapping).fillna(df['waste_material_id'])
            df.rename(columns={'waste_material_id': 'waste_material'}, inplace=True)

        elif file_name == "reports.csv":
            df['waste_type'] = df['waste_type'].str.replace(',', ', ')

        elif file_name == "task_challenges.csv":
            df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
            df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')

        elif file_name == "users.csv":
            df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce').dt.strftime('%Y-%m-%d')
            badge_mapping = {
                'https://res.cloudinary.com/dymhvau8n/image/upload/v1718189121/user_badge/htaemsjtlhfof7ww01ss.png': 'classic',
                'https://res.cloudinary.com/dymhvau8n/image/upload/v1718189221/user_badge/oespnjdgoynkairlutbk.png': 'silver',
                'https://res.cloudinary.com/dymhvau8n/image/upload/v1718189184/user_badge/jshs1s2fwevahgtvjkgj.png': 'gold',
                'https://res.cloudinary.com/dymhvau8n/image/upload/v1718188250/user_badge/icureiapdvtzyu5b99zu.png': 'platinum'
            }
            df['badge'] = df['badge'].map(badge_mapping)

        # Handle missing values
        if file_name in ["article_sections.csv", "articles.csv"]:
            df['title'] = df['title'].fillna('Unknown')
            df = df[df['title'].str.split().str.len() >= 4]

        if file_name == "reports.csv":
            df['waste_type'] = df['waste_type'].str.replace(',', ', ')
            df['reason'] = df['reason'].fillna('No reason provided')
            df['title'] = df['title'].fillna('Unknown')
            df['title_word_count'] = df['title'].apply(lambda x: len(str(x).split()))
            df = df[df['title_word_count'] >= 4]
            df = df.drop('title_word_count', axis=1)

        if file_name == "user_task_challenges.csv":
            df['description_image'] = df['description_image'].fillna('No description provided')
            df['reason'] = df['reason'].fillna('No reason provided')

        if file_name == "task_steps.csv" or file_name == "task_challenges.csv":
            df = df[df['description'].str.split().str.len() >= 3]

        if file_name == "users.csv":
            for col in ['gender', 'birth_date', 'address', 'picture_url']:
                df[col] = df[col].fillna('Tidak diketahui')

        # Detect and handle duplicates
        initial_rows = df.shape[0]
        df.drop_duplicates(inplace=True)
        new_rows = df.shape[0]

        # Displays information about duplicates
        if new_rows < initial_rows:
            print(f"\nDuplikasi data di {file_name} ditemukan dan dihapus.")
            print(f"Jumlah baris sebelum: {initial_rows}, setelah: {new_rows}.")
        else:
            print(f"\nTidak ada duplikasi data di {file_name}.")

        # Display missing values after transformation
        print(f"\nMissing values after transformation for {file_name}:")
        print(df.isnull().sum())

        # Save transformed data
        df.to_csv(f"/home/daffaalfahryan/staging_area/{file_name}", index=False)

    # Create fact tables
    create_fact_tables()

def create_fact_tables():
    # fact_reporting
    reports = pd.read_csv('/home/daffaalfahryan/staging_area/reports.csv')
    report_waste_materials = pd.read_csv('/home/daffaalfahryan/staging_area/report_waste_materials.csv')
    df_fact_reporting = (
        reports
        .merge(report_waste_materials, left_on='id', right_on='report_id')
        [['id_x', 'id_y', 'author_id', 'report_type']]
        .rename(columns={'id_x': 'id', 'id_y': 'report_waste_materials_id', 'author_id': 'user_id'})
    )
    df_fact_reporting.to_csv('/home/daffaalfahryan/data_warehouse/fact_reporting.csv', index=False)

    # fact_challenge
    task_challenges = pd.read_csv('/home/daffaalfahryan/staging_area/task_challenges.csv')
    user_task_challenges = pd.read_csv('/home/daffaalfahryan/staging_area/user_task_challenges.csv')
    df_fact_challenge = (
        task_challenges
        .merge(user_task_challenges, left_on='id', right_on='task_challenge_id')
        [['id_x', 'id_y', 'user_id', 'status_accept']]
        .rename(columns={'id_x': 'id', 'id_y': 'user_task_challenges_id'})
    )
    df_fact_challenge.to_csv('/home/daffaalfahryan/data_warehouse/fact_challange.csv', index=False)

    # fact_videos_comment
    videos = pd.read_csv('/home/daffaalfahryan/staging_area/videos.csv')
    comments = pd.read_csv('/home/daffaalfahryan/staging_area/comments.csv')
    df_fact_videos_comment = (
        videos
        .merge(comments, left_on='id', right_on='video_id')
        [['id_x', 'user_id', 'id_y', 'comment']]
        .rename(columns={'id_x': 'id', 'id_y': 'videos_comments_id'})
    )
    df_fact_videos_comment.to_csv('/home/daffaalfahryan/data_warehouse/fact_videos_comment.csv', index=False)

    # fact_articles_comment
    articles = pd.read_csv('/home/daffaalfahryan/staging_area/articles.csv')
    article_comments = pd.read_csv('/home/daffaalfahryan/staging_area/article_comments.csv')
    df_fact_articles_comment = (
        articles
        .merge(article_comments, left_on='id', right_on='article_id')
        [['id_x', 'user_id', 'id_y', 'comment']]
        .rename(columns={'id_x': 'id', 'id_y': 'article_comments_id'})
    )
    df_fact_articles_comment.to_csv('/home/daffaalfahryan/data_warehouse/fact_articles_comment.csv', index=False)

    # Create fact tables
    create_dim_tables()

def create_dim_tables():
    # dim_users
    users = pd.read_csv('/home/daffaalfahryan/staging_area/users.csv')
    dim_users_columns = ['id', 'name', 'email', 'point', 'gender', 'birth_date', 'address', 'badge', 'created_at', 'updated_at', 'deleted_at']
    df_dim_users = users[dim_users_columns]
    df_dim_users.to_csv('/home/daffaalfahryan/data_warehouse/users.csv', index=False)

    # dim_reports
    reports = pd.read_csv('/home/daffaalfahryan/staging_area/reports.csv')
    dim_reports_columns = ['id', 'report_type', 'title', 'description', 'waste_type', 'latitude', 'longitude', 'address', 'city', 'province', 'status', 'reason', 'created_at', 'updated_at', 'deleted_at']
    df_dim_reports = reports[dim_reports_columns]
    df_dim_reports.to_csv('/home/daffaalfahryan/data_warehouse/reports.csv', index=False)

    # dim_report_waste_materials
    report_waste_materials = pd.read_csv('/home/daffaalfahryan/staging_area/report_waste_materials.csv')
    dim_report_waste_materials_columns = ['id', 'waste_material', 'created_at', 'updated_at', 'deleted_at']
    df_dim_report_waste_materials = report_waste_materials[dim_report_waste_materials_columns]
    df_dim_report_waste_materials.to_csv('/home/daffaalfahryan/data_warehouse/report_waste_materials.csv', index=False)

    # dim_user_task_challenges
    user_task_challenges = pd.read_csv('/home/daffaalfahryan/staging_area/user_task_challenges.csv')
    dim_user_task_challenges_columns = ['id', 'status_progress', 'status_accept', 'point', 'reason', 'accepted_at', 'created_at', 'updated_at', 'deleted_at']
    df_dim_user_task_challenges = user_task_challenges[dim_user_task_challenges_columns]
    df_dim_user_task_challenges.to_csv('/home/daffaalfahryan/data_warehouse/user_task_challenges.csv', index=False)

    # dim_task_challenges
    task_challenges = pd.read_csv('/home/daffaalfahryan/staging_area/task_challenges.csv')
    dim_task_challenges_columns = ['id', 'title', 'description', 'start_date', 'end_date', 'point', 'status', 'created_at', 'updated_at', 'deleted_at']
    df_dim_task_challenges = task_challenges[dim_task_challenges_columns]
    df_dim_task_challenges.to_csv('/home/daffaalfahryan/data_warehouse/task_challenges.csv', index=False)

    # dim_videos
    videos = pd.read_csv('/home/daffaalfahryan/staging_area/videos.csv')
    dim_videos_columns = ['id', 'title', 'description', 'link', 'viewer', 'created_at', 'updated_at', 'deleted_at']
    df_dim_videos = videos[dim_videos_columns]
    df_dim_videos.to_csv('/home/daffaalfahryan/data_warehouse/videos.csv', index=False)

    # dim_comments
    comments = pd.read_csv('/home/daffaalfahryan/staging_area/comments.csv')
    dim_comments_columns = ['id', 'comment', 'created_at', 'updated_at', 'deleted_at']
    df_dim_comments = comments[dim_comments_columns]
    df_dim_comments.to_csv('/home/daffaalfahryan/data_warehouse/comments.csv', index=False)

    # dim_articles
    articles = pd.read_csv('/home/daffaalfahryan/staging_area/articles.csv')
    dim_articles_columns = ['id', 'title', 'description', 'created_at', 'updated_at', 'deleted_at']
    df_dim_articles = articles[dim_articles_columns]
    df_dim_articles.to_csv('/home/daffaalfahryan/data_warehouse/articles.csv', index=False)

    # dim_article_comments
    article_comments = pd.read_csv('/home/daffaalfahryan/staging_area/article_comments.csv')
    dim_article_comments_columns = ['id', 'comment', 'created_at', 'updated_at', 'deleted_at']
    df_dim_article_comments = article_comments[dim_article_comments_columns]
    df_dim_article_comments.to_csv('/home/daffaalfahryan/data_warehouse/article_comments.csv', index=False)


def load_to_gcs(**kwargs):
    credentials_path = Variable.get("GOOGLE_APPLICATION_CREDENTIALS", default_var=None)
    if not credentials_path:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS Airflow variable is not set")
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    # Inisialisasi client GCS
    client = storage.Client()
    
    # Nama bucket dan folder di GCS
    bucket_name = "recything-data-archive-etl"
    folder_name = "2024-06-24"
    
    # Daftar file yang ingin diunggah
    files_to_upload = [
        'about_us_images.csv', 'about_us.csv', 'achievements.csv', 'admins.csv',
        'article_categories.csv', 'article_comments.csv', 'article_sections.csv',
        'articles.csv', 'comments.csv', 'content_categories.csv', 'custom_data.csv',
        'faqs.csv', 'report_images.csv', 'report_waste_materials.csv', 'reports.csv',
        'task_challenges.csv', 'task_steps.csv', 'user_task_challenges.csv',
        'user_task_images.csv', 'user_task_steps.csv', 'users.csv',
        'video_categories.csv', 'videos.csv', 'waste_categories.csv', 'waste_materials.csv'
    ]
    
    # Loop untuk mengunggah setiap file ke GCS
    for file_name in files_to_upload:
        local_file_path = f"/home/daffaalfahryan/staging_area/{file_name}"
        
        # Upload file ke GCS
        blob = client.bucket(bucket_name).blob(f"{folder_name}/{file_name}")
        blob.upload_from_filename(local_file_path)
        
        print(f"File {file_name} berhasil diunggah ke GCS.")

def load_to_bigquery(**kwargs):
    credentials_path = Variable.get("GOOGLE_APPLICATION_CREDENTIALS", default_var=None)
    if not credentials_path:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS Airflow variable is not set")
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    # BigQuery client
    client = bigquery.Client()
    
    # Nama dataset
    dataset_name = 'recything'
    
    # Folder data
    data_folder = '/home/daffaalfahryan/data_warehouse'

    # List data warehouse
    csv_files = [
        'article_comments.csv', 'articles.csv', 'comments.csv', 'fact_articles_comment.csv',
        'fact_challange.csv', 'fact_reporting.csv', 'fact_videos_comment.csv', 'report_waste_materials.csv',
        'reports.csv', 'task_challenges.csv', 'user_task_challenges.csv', 'users.csv',
        'videos.csv'
    ]
    
    # Fungsi untuk mengunggah file CSV ke BigQuery
    def load_csv_to_bigquery(file_name):
        file_path = os.path.join(data_folder, file_name)
        table_name = file_name.replace('.csv', '')
        
        # Muat file CSV ke dalam DataFrame pandas
        df = pd.read_csv(file_path)
        
        # Tentukan ID tabel
        table_id = f"{client.project}.{dataset_name}.{table_name}"
        
        # Definisikan konfigurasi job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Replace table if it already exists
        )
        
        # Muat DataFrame ke tabel BigQuery
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        
        print(f"Loaded {file_name} into {table_id}")
    
    # Memastikan dataset ada
    dataset_id = f"{client.project}.{dataset_name}"
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_name} already exists")
    except NotFound:
        # Create a new dataset
        dataset = bigquery.Dataset(dataset_id)
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset_name}")
    
    # Upload file ke BigQuery
    for csv_file in csv_files:
        load_csv_to_bigquery(csv_file)
    print("Data successfully uploaded")
# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_all_tables_to_csv,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_gcs_task = PythonOperator(
    task_id='load_to_gcs',
    python_callable=load_to_gcs,
    dag=dag,
)

load_bigquery_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> [load_gcs_task, load_bigquery_task]
