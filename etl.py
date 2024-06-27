import pandas as pd
import MySQLdb
import os
import io
from google.cloud import bigquery
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from google.oauth2 import service_account
from datetime import datetime, timedelta

# Define database connection parameters
DB_USER = 'capstone'
DB_PASSWORD = 'AVNS_jhbqUpYpzutyZP2lk7U'
DB_HOST = 'db-alterra-do-user-16372004-0.c.db.ondigitalocean.com'
DB_PORT = 25060
DB_NAME = 'db_capstone_production'
SSL_CA = './ca-certificate.crt'

@task
def extract_data():
    try:
        conn = MySQLdb.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            passwd=DB_PASSWORD,
            db=DB_NAME,
            ssl={'ca': SSL_CA}
        )
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        data_frames = {}
        for table in tables:
            table_name = table[0]
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
            data_frames[table_name] = df
        return data_frames
    except MySQLdb.Error as e:
        print("Error: ", e)
        return None

@task
def transform_data(data_frames):
    # Transform data here
    df_dim_users = pd.DataFrame(columns=['user_id', 'username', 'email', 'name', 'address', 'phone_number', 'gender', 'age', 'points'])
    df_dim_doctors = pd.DataFrame(columns=['doctor_id', 'username', 'email', 'name', 'address', 'phone_number', 'gender', 'is_available', 'balance', 'experience', 'practice_location', 'practice_city',  'fee', 'specialist'])
    df_dim_articles = pd.DataFrame(columns=['article_id', 'doctor_id', 'title', 'content', 'date'])
    df_dim_inspirational_stories = pd.DataFrame(columns=['story_id', 'doctor_id', 'title', 'content', 'date'])
    df_dim_music = pd.DataFrame(columns=['music_id', 'doctor_id','title', 'singer'])
    df_dim_forum = pd.DataFrame(columns=['forum_id', 'doctor_id', 'name', 'description','created_at', 'updated_at'])
    df_dim_post = pd.DataFrame(columns=['post_id', 'user_id','forum_id', 'content','created_at', 'updated_at'])
    df_dim_user_moods = pd.DataFrame(columns=['user_mood_id', 'user_id', 'moods_name', 'date'])
    df_dim_complaints = pd.DataFrame(columns=['complaint_id', 'name', 'age', 'gender', 'message', 'medical_history'])
    df_dim_consultations = pd.DataFrame(columns=['consultation_id', 'complaint_id', 'payment_status', 'is_accepted', 'is_active', 'consultation_status', 'start_date', 'finish_date'])
    df_dim_transactions = pd.DataFrame(columns=['transaction_id', 'consultation_id', 'price', 'payment_type', 'bank', 'va_numbers', 'transaction_time', 'transaction_status'])
    df_dim_ratings = pd.DataFrame(columns=['rating_id', 'rating', 'message'])

    df_users = data_frames['users']
    df_doctors = data_frames['doctors']
    df_articles = data_frames['articles']
    df_stories = data_frames['stories']
    df_musics = data_frames['musics']
    df_forums = data_frames['forums']
    df_posts = data_frames['posts']
    df_moods = data_frames['moods']
    df_complaints = data_frames['complaints']
    df_consultations = data_frames['consultations']
    df_transactions = data_frames['transactions']
    df_ratings = data_frames['ratings']

    for index, row in df_users.iterrows():
        df_dim_users.loc[index] = [
            row['id'],
            row['username'],
            row['email'],
            row['name'],
            row['address'],
            row['phone_number'],
            row['gender'],
            row['age'],
            row['points']
        ]

    for index, row in df_doctors.iterrows():
        df_dim_doctors.loc[index] = [
            row['id'],
            row['username'],
            row['email'],
            row['name'],
            row['address'],
            row['phone_number'],
            row['gender'],
            row['is_available'],
            row['balance'],
            row['experience'],
            row['practice_location'],
            row['practice_city'],
            # row['practice_province'],
            row['fee'],
            row['specialist'],
        ]

    for index, row in df_articles.iterrows():
        df_dim_articles.loc[index] = [
            row['id'],
            row['doctor_id'],
            row['title'],
            row['content'],
            row['date'],
        ]

    for index, row in df_stories.iterrows():
        df_dim_inspirational_stories.loc[index] = [
            row['id'],
            row['doctor_id'],
            row['title'],
            row['content'],
            row['date'],
        ]

    for index, row in df_musics.iterrows():
        df_dim_music.loc[index] = [
            row['id'],
            row['doctor_id'],
            row['title'],
            row['singer'],
        ]

    for index, row in df_forums.iterrows():
        df_dim_forum.loc[index] = [
            row['id'],
            row['doctor_id'],
            row['name'],
            row['description'],
            row['created_at'],
            row['updated_at'],
        ]

    for index, row in df_posts.iterrows():
        df_dim_post.loc[index] = [
            row['id'],
            row['user_id'],
            row['forum_id'],
            row['content'],
            row['created_at'],
            row['updated_at'],
        ]

    # Transform df_moods
    df_moods = df_moods.merge(data_frames['mood_types'], left_on='mood_type_id', right_on='id', how='left')
    df_moods.drop(['id_y','created_at_y', 'updated_at_y', 'deleted_at_y'], axis=1, inplace=True)
    df_moods.rename(columns={'name_mood': 'moods_name'}, inplace=True)

    for index, row in df_moods.iterrows():
        df_dim_user_moods.loc[index] = [
            row['id_x'],
            row['user_id'],
            row['name'],
            row['date'],
        ]

    for index, row in df_complaints.iterrows():
        df_dim_complaints.loc[index] = [
            row['id'],
            row['name'],
            row['age'],
            row['gender'],
            row['message'],
            row['medical_history'],
        ]

    for index, row in df_consultations.iterrows():
        df_dim_consultations.loc[index] = [
            row['id'],
            row['complaint_id'],
            row['payment_status'],
            row['is_accepted'],
            row['is_active'],
            row['status'],
            row['created_at'],
            row['updated_at'],
        ]

    for index, row in df_transactions.iterrows():
        df_dim_transactions.loc[index] = [
            row['id'],
            row['consultation_id'],
            row['price'],
            row['payment_type'],
            row['bank'],
            row['payment_link'],
            row['updated_at'],
            row['status'],
        ]

    for index, row in df_ratings.iterrows():
        df_dim_ratings.loc[index] = [
            row['id'],
            row['rate'],
            row['message'],
        ]

    df_dim_user_moods['date'] = pd.to_datetime(df_dim_user_moods['date'])
    df_dim_consultations['complaint_id'] = pd.to_numeric(df_dim_consultations['complaint_id'], errors='coerce').astype('Int64')

    df_dim_users['name'] = df_dim_users['name'].replace(to_replace=r'[^a-zA-Z\s]', value='', regex=True)
    df_dim_users['address'] = df_dim_users['address'].replace(to_replace=r'[^a-zA-Z0-9\s.,]', value='', regex=True)
    df_dim_doctors['name'] = df_dim_doctors['name'].replace(to_replace=r'[^a-zA-Z\s]', value='', regex=True)
    df_dim_doctors['address'] = df_dim_doctors['address'].replace(to_replace=r'[^a-zA-Z0-9\s.,]', value='', regex=True)
    df_dim_doctors['practice_location'] = df_dim_doctors['practice_location'].replace(to_replace=r'[^a-zA-Z0-9\s]', value='', regex=True)
    df_dim_doctors['practice_city'] = df_dim_doctors['practice_city'].replace(to_replace=r'[^a-zA-Z\s]', value='', regex=True)
    # df_dim_doctors['practice_province'] = df_dim_doctors['practice_province'].replace(to_replace=r'[^a-zA-Z\s]', value='', regex=True)
    df_dim_doctors['specialist'] = df_dim_doctors['specialist'].replace(to_replace=r'[^a-zA-Z\s]', value='', regex=True)
    df_dim_music['title'] = df_dim_music['title'].replace(to_replace=r'[^a-zA-Z0-9\s]', value='', regex=True)

    # Fact tables for articles, music, and inspirational stories
    df_article_likes = data_frames['article_likes']
    df_article_views = data_frames['article_views']
    df_music_likes = data_frames['music_likes']
    df_music_views = data_frames['music_views']
    df_story_likes = data_frames['story_likes']
    df_story_views = data_frames['story_views']
    df_post_likes = data_frames['post_likes']
    # df_post_views = data_frames['post_views']

    # # Transform fact table for articles
    # total_likes_articles = df_article_likes.groupby('article_id').size().reset_index(name='total_like')
    # user_ids_articles = df_article_likes.groupby('article_id')['user_id'].first().reset_index()
    # df_temp_articles = pd.merge(total_likes_articles, user_ids_articles, on='article_id')
    # df_fact_article = pd.merge(df_temp_articles, df_articles[['id', 'view_count']], left_on='article_id', right_on='id').drop(columns=['id'])

    # # Transform fact table for music
    # total_likes_music = df_music_likes.groupby('music_id').size().reset_index(name='total_like')
    # user_ids_music = df_music_likes.groupby('music_id')['user_id'].first().reset_index()
    # df_temp_music = pd.merge(total_likes_music, user_ids_music, on='music_id')
    # df_fact_music = pd.merge(df_temp_music, df_musics[['id', 'view_count']], left_on='music_id', right_on='id').drop(columns=['id'])

    # # Transform fact table for inspirational stories
    # total_likes_stories = df_story_likes.groupby('story_id').size().reset_index(name='total_like')
    # user_ids_stories = df_story_likes.groupby('story_id')['user_id'].first().reset_index()
    # df_temp_stories = pd.merge(total_likes_stories, user_ids_stories, on='story_id')
    # df_fact_inspirational_stories = pd.merge(df_temp_stories, df_stories[['id', 'view_count']], left_on='story_id', right_on='id').drop(columns=['id'])
    
    # # Transform fact table for posts
    # total_likes_posts = df_post_likes.groupby('post_id').size().reset_index(name='total_like')
    # user_ids_posts = df_post_likes.groupby('post_id')['user_id'].first().reset_index()
    # df_temp_posts = pd.merge(total_likes_posts, user_ids_posts, on='post_id')
    # df_fact_post = pd.merge(df_temp_posts, df_posts[['id']], left_on='post_id', right_on='id').drop(columns=['id'])
    
    # df_fact_article['view_count'] = pd.to_numeric(df_fact_article['view_count'], errors='coerce').astype('Int64')
    # df_fact_music['view_count'] = pd.to_numeric(df_fact_music['view_count'], errors='coerce').astype('Int64')
    # df_fact_inspirational_stories['view_count'] = pd.to_numeric(df_fact_inspirational_stories['view_count'], errors='coerce').astype('Int64')
    

    
    # Dapatkan tanggal saat ini dan tanggal kemarin
    current_date = datetime.now().date()
    yesterday_date = current_date - timedelta(days=1)

    # Filter df_article_likes dan df_article_views untuk hanya menyertakan data dari tanggal kemarin
    df_article_likes_yesterday = df_article_likes[df_article_likes['updated_at'].dt.date == yesterday_date]
    df_article_views_yesterday = df_article_views[df_article_views['updated_at'].dt.date == yesterday_date]

    # Hitung total likes dan views per article_id untuk tanggal kemarin
    total_likes_articles = df_article_likes_yesterday.groupby('article_id').size().reset_index(name='total_like')
    total_views_articles = df_article_views_yesterday.groupby('article_id').size().reset_index(name='view_count')

    # Buat daftar lengkap semua article_id dari df_articles
    all_article_ids = pd.DataFrame(df_articles['id'].unique(), columns=['article_id'])

    # Gabungkan daftar lengkap dengan hasil penghitungan total likes dan views
    df_fact_article_per_tanggal = pd.merge(all_article_ids, total_likes_articles, on='article_id', how='left').merge(total_views_articles, on='article_id', how='left')

    # Isi nilai NaN dengan 0 untuk total_like dan view_count
    df_fact_article_per_tanggal['total_like'] = df_fact_article_per_tanggal['total_like'].fillna(0)
    df_fact_article_per_tanggal['view_count'] = df_fact_article_per_tanggal['view_count'].fillna(0)

    # Tambahkan kolom upload_at dengan tanggal sekarang
    df_fact_article_per_tanggal['upload_at'] = current_date

    # Tambahkan kolom updated_at dengan tanggal kemarin
    df_fact_article_per_tanggal['updated_at'] = yesterday_date

    # Jika ada data lama di df_fact_article_fix, hapus data yang sudah ada dengan upload_at yang sama
    if 'df_fact_article_fix' in locals():
        df_fact_article_fix = df_fact_article_fix[df_fact_article_fix['upload_at'] != current_date]
        df_fact_article_fix = pd.concat([df_fact_article_fix, df_fact_article_per_tanggal])
    else:
        df_fact_article_fix = df_fact_article_per_tanggal

    # Hapus duplikasi berdasarkan article_id dan upload_at untuk memastikan setiap article_id hanya muncul sekali
    df_fact_article_fix = df_fact_article_fix.drop_duplicates(subset=['article_id', 'upload_at'])

    # Convert date columns to string format
    df_fact_article_fix['upload_at'] = df_fact_article_fix['upload_at'].astype(str)
    df_fact_article_fix['updated_at'] = df_fact_article_fix['updated_at'].astype(str)
    df_fact_article_fix['view_count'] = pd.to_numeric(df_fact_article_fix['view_count'], errors='coerce').astype('Int64')
    df_fact_article_fix['total_like'] = pd.to_numeric(df_fact_article_fix['total_like'], errors='coerce').astype('Int64')



    # Filter df_music_likes dan df_music_views untuk hanya menyertakan data dari tanggal kemarin
    df_music_likes_yesterday = df_music_likes[df_music_likes['updated_at'].dt.date == yesterday_date]
    df_music_views_yesterday = df_music_views[df_music_views['updated_at'].dt.date == yesterday_date]

    # Hitung total likes dan views per music_id untuk tanggal kemarin
    total_likes_musics = df_music_likes_yesterday.groupby('music_id').size().reset_index(name='total_like')
    total_views_musics = df_music_views_yesterday.groupby('music_id').size().reset_index(name='view_count')

    # Buat daftar lengkap semua music_id dari df_musics
    all_music_ids = pd.DataFrame(df_musics['id'].unique(), columns=['music_id'])

    # Gabungkan daftar lengkap dengan hasil penghitungan total likes dan views
    df_fact_music_per_tanggal = pd.merge(all_music_ids, total_likes_musics, on='music_id', how='left').merge(total_views_musics, on='music_id', how='left')

    # Isi nilai NaN dengan 0 untuk total_like dan view_count
    df_fact_music_per_tanggal['total_like'] = df_fact_music_per_tanggal['total_like'].fillna(0)
    df_fact_music_per_tanggal['view_count'] = df_fact_music_per_tanggal['view_count'].fillna(0)

    # Tambahkan kolom upload_at dengan tanggal sekarang
    df_fact_music_per_tanggal['upload_at'] = current_date

    # Tambahkan kolom updated_at dengan tanggal kemarin
    df_fact_music_per_tanggal['updated_at'] = yesterday_date

    # Jika ada data lama di df_fact_music_fix, hapus data yang sudah ada dengan upload_at yang sama
    if 'df_fact_music_fix' in locals():
        df_fact_music_fix = df_fact_music_fix[df_fact_music_fix['upload_at'] != current_date]
        df_fact_music_fix = pd.concat([df_fact_music_fix, df_fact_music_per_tanggal])
    else:
        df_fact_music_fix = df_fact_music_per_tanggal

    # Hapus duplikasi berdasarkan music_id dan upload_at untuk memastikan setiap music_id hanya muncul sekali
    df_fact_music_fix = df_fact_music_fix.drop_duplicates(subset=['music_id', 'upload_at'])

    # Convert date columns to string format
    df_fact_music_fix['upload_at'] = df_fact_music_fix['upload_at'].astype(str)
    df_fact_music_fix['updated_at'] = df_fact_music_fix['updated_at'].astype(str)
    df_fact_music_fix['view_count'] = pd.to_numeric(df_fact_music_fix['view_count'], errors='coerce').astype('Int64')
    df_fact_music_fix['total_like'] = pd.to_numeric(df_fact_music_fix['total_like'], errors='coerce').astype('Int64')

    

    # Filter df_story_likes dan df_story_views untuk hanya menyertakan data dari tanggal kemarin
    df_story_likes_yesterday = df_story_likes[df_story_likes['updated_at'].dt.date == yesterday_date]
    df_story_views_yesterday = df_story_views[df_story_views['updated_at'].dt.date == yesterday_date]

    # Hitung total likes dan views per story_id untuk tanggal kemarin
    total_likes_stories = df_story_likes_yesterday.groupby('story_id').size().reset_index(name='total_like')
    total_views_stories = df_story_views_yesterday.groupby('story_id').size().reset_index(name='view_count')

    # Buat daftar lengkap semua story_id dari df_stories
    all_story_ids = pd.DataFrame(df_stories['id'].unique(), columns=['story_id'])

    # Gabungkan daftar lengkap dengan hasil penghitungan total likes dan views
    df_fact_story_per_tanggal = pd.merge(all_story_ids, total_likes_stories, on='story_id', how='left').merge(total_views_stories, on='story_id', how='left')

    # Isi nilai NaN dengan 0 untuk total_like dan view_count
    df_fact_story_per_tanggal['total_like'] = df_fact_story_per_tanggal['total_like'].fillna(0)
    df_fact_story_per_tanggal['view_count'] = df_fact_story_per_tanggal['view_count'].fillna(0)

    # Tambahkan kolom upload_at dengan tanggal sekarang
    df_fact_story_per_tanggal['upload_at'] = current_date

    # Tambahkan kolom updated_at dengan tanggal kemarin
    df_fact_story_per_tanggal['updated_at'] = yesterday_date

    # Jika ada data lama di df_fact_story_fix, hapus data yang sudah ada dengan upload_at yang sama
    if 'df_fact_story_fix' in locals():
        df_fact_story_fix = df_fact_story_fix[df_fact_story_fix['upload_at'] != current_date]
        df_fact_story_fix = pd.concat([df_fact_story_fix, df_fact_story_per_tanggal])
    else:
        df_fact_story_fix = df_fact_story_per_tanggal

    # Hapus duplikasi berdasarkan story_id dan upload_at untuk memastikan setiap story_id hanya muncul sekali
    df_fact_story_fix = df_fact_story_fix.drop_duplicates(subset=['story_id', 'upload_at'])

    # Convert date columns to string format
    df_fact_story_fix['upload_at'] = df_fact_story_fix['upload_at'].astype(str)
    df_fact_story_fix['updated_at'] = df_fact_story_fix['updated_at'].astype(str)
    df_fact_story_fix['view_count'] = pd.to_numeric(df_fact_story_fix['view_count'], errors='coerce').astype('Int64')
    df_fact_story_fix['total_like'] = pd.to_numeric(df_fact_story_fix['total_like'], errors='coerce').astype('Int64')


    # Filter df_post_likes dan df_post_views untuk hanya menyertakan data dari tanggal kemarin
    df_post_likes_yesterday = df_post_likes[df_post_likes['updated_at'].dt.date == yesterday_date]
    # df_post_views_yesterday = df_post_views[df_post_views['updated_at'].dt.date == yesterday_date]

    # Hitung total likes dan views per post_id untuk tanggal kemarin
    total_likes_posts = df_post_likes_yesterday.groupby('post_id').size().reset_index(name='total_like')
    # total_views_posts = df_post_views_yesterday.groupby('post_id').size().reset_index(name='view_count')

    # Buat daftar lengkap semua post_id dari df_posts
    all_post_ids = pd.DataFrame(df_posts['id'].unique(), columns=['post_id'])

    # Gabungkan daftar lengkap dengan hasil penghitungan total likes dan views
    df_fact_post_per_tanggal = pd.merge(all_post_ids, total_likes_posts, on='post_id', how='left')

    # Isi nilai NaN dengan 0 untuk total_like dan view_count
    df_fact_post_per_tanggal['total_like'] = df_fact_post_per_tanggal['total_like'].fillna(0)
    # df_fact_post_per_tanggal['view_count'] = df_fact_post_per_tanggal['view_count'].fillna(0)

    # Tambahkan kolom upload_at dengan tanggal sekarang
    df_fact_post_per_tanggal['upload_at'] = current_date

    # Tambahkan kolom updated_at dengan tanggal kemarin
    df_fact_post_per_tanggal['updated_at'] = yesterday_date

    # Jika ada data lama di df_fact_post_fix, hapus data yang sudah ada dengan upload_at yang sama
    if 'df_fact_post_fix' in locals():
        df_fact_post_fix = df_fact_post_fix[df_fact_post_fix['upload_at'] != current_date]
        df_fact_post_fix = pd.concat([df_fact_post_fix, df_fact_post_per_tanggal])
    else:
        df_fact_post_fix = df_fact_post_per_tanggal

    # Hapus duplikasi berdasarkan post_id dan upload_at untuk memastikan setiap post_id hanya muncul sekali
    df_fact_post_fix = df_fact_post_fix.drop_duplicates(subset=['post_id', 'upload_at'])

    # Convert date columns to string format
    df_fact_post_fix['upload_at'] = df_fact_post_fix['upload_at'].astype(str)
    df_fact_post_fix['updated_at'] = df_fact_post_fix['updated_at'].astype(str)
    # df_fact_post_fix['view_count'] = pd.to_numeric(df_fact_post_fix['view_count'], errors='coerce').astype('Int64')
    df_fact_post_fix['total_like'] = pd.to_numeric(df_fact_post_fix['total_like'], errors='coerce').astype('Int64')


    # Mengubah kolom 'created_at' menjadi tipe datetime
    df_consultations['updated_at'] = pd.to_datetime(df_consultations['updated_at'])

    # Memfilter data berdasarkan status yang diinginkan
    valid_statuses = ['done', 'active', 'incoming']
    df_consultations = df_consultations[df_consultations['status'].isin(valid_statuses)]


    # Fact consultation doctor
    # Mendapatkan tanggal kemarin
    yesterday_date = datetime.now().date() - timedelta(days=1)

    # Memfilter data untuk tanggal kemarin
    df_yesterday_consultations = df_consultations[df_consultations['updated_at'].dt.date == yesterday_date]

    # Mengambil baris terakhir untuk setiap pasangan (doctor_id, date)
    df_yesterday_consultations['date'] = df_yesterday_consultations['updated_at'].dt.date
    df_latest_consultation = df_yesterday_consultations.sort_values(['date', 'updated_at']).drop_duplicates(subset=['doctor_id', 'date'], keep='last')

    # Menghitung total konsultasi per doctor_id untuk setiap tanggal
    total_consultation_per_doctor = df_yesterday_consultations.groupby(['doctor_id', 'date']).size().reset_index(name='total_consultation')

    # Menggabungkan hasil total konsultasi dengan data yang sudah unik
    df_fact_consultation_doctor = pd.merge(df_latest_consultation, total_consultation_per_doctor, on=['doctor_id', 'date'])

    # Menyusun DataFrame akhir dan mengganti nama kolom 'updated_at' menjadi 'updated_at'
    df_fact_consultation_doctor = df_fact_consultation_doctor[['id', 'doctor_id', 'user_id', 'total_consultation', 'updated_at']]

    # Menambahkan kolom 'uploaded_at' dengan tanggal sekarang beserta waktunya
    df_fact_consultation_doctor['uploaded_at'] = datetime.now()


    # Fact consultation user
    # Mendapatkan tanggal kemarin
    yesterday_date = datetime.now().date() - timedelta(days=1)

    # Memfilter data untuk tanggal kemarin
    df_yesterday_consultations = df_consultations[df_consultations['updated_at'].dt.date == yesterday_date]

    # Mengambil baris terakhir untuk setiap pasangan (user_id, date)
    df_yesterday_consultations['date'] = df_yesterday_consultations['updated_at'].dt.date
    df_latest_consultation = df_yesterday_consultations.sort_values(['date', 'updated_at']).drop_duplicates(subset=['user_id', 'date'], keep='last')

    # Menghitung total konsultasi per user_id untuk setiap tanggal
    total_consultation_per_user = df_yesterday_consultations.groupby(['user_id', 'date']).size().reset_index(name='total_consultation')

    # Menggabungkan hasil total konsultasi dengan data yang sudah unik
    df_fact_consultation_user = pd.merge(df_latest_consultation, total_consultation_per_user, on=['user_id', 'date'])

    # Menyusun DataFrame akhir dan mengganti nama kolom 'updated_at' menjadi 'updated_at'
    df_fact_consultation_user = df_fact_consultation_user[['id', 'doctor_id', 'user_id', 'total_consultation', 'updated_at']]

    # Menambahkan kolom 'uploaded_at' dengan tanggal sekarang beserta waktunya
    df_fact_consultation_user['uploaded_at'] = datetime.now()

    # Gabungkan kedua tabel berdasarkan consultation_id
    df_merged = df_transactions.merge(df_consultations, left_on='consultation_id', right_on='id')

    # Filter hanya status 'settlement'
    df_settlement = df_merged[df_merged['payment_status'] == 'settlement']

    # Hitung total_amount per user_id
    total_amount = df_settlement.groupby('user_id')['price'].sum().reset_index()
    total_amount.columns = ['user_id', 'total_amount']

    # Ambil transaksi terakhir untuk setiap user_id
    latest_transaction = df_settlement.sort_values('updated_at_x').drop_duplicates('user_id', keep='last')

    # Gabungkan total_amount dengan data transaksi terbaru
    result = latest_transaction.merge(total_amount, on='user_id')

    # Pilih kolom yang diinginkan
    df_fact_transaction_fix = result[['consultation_id', 'doctor_id', 'user_id', 'total_amount', 'updated_at_x']]
    df_fact_transaction_fix = df_fact_transaction_fix.rename(columns={'updated_at_x': 'updated_at'})

    
    # Ubah kolom 'updated_at' menjadi tipe datetime
    df_ratings['updated_at'] = pd.to_datetime(df_ratings['updated_at'])

    # Hitung rata-rata rating per doctor_id
    avg_ratings = df_ratings.groupby('doctor_id')['rate'].mean().reset_index()
    avg_ratings.columns = ['doctor_id', 'avg_rating']

    # Ambil rating terakhir untuk setiap doctor_id
    latest_rating = df_ratings.sort_values('updated_at').drop_duplicates('doctor_id', keep='last')

    # Gabungkan hasilnya
    result = pd.merge(avg_ratings, latest_rating[['id', 'user_id', 'doctor_id', 'updated_at']], on='doctor_id', how='left')

    # Tambahkan kolom uploaded_at dengan nilai datetime now
    result['uploaded_at'] = pd.Timestamp.now()

    # Buat tabel fact_rating_fix dengan memilih kolom yang diinginkan
    df_fact_rating_fix = result[['id', 'user_id', 'doctor_id', 'avg_rating', 'updated_at', 'uploaded_at']]


    # Include fact tables in the transformed data
    return {
        'dim_users': df_dim_users,
        'dim_doctors': df_dim_doctors,
        'dim_articles': df_dim_articles,
        'dim_inspirational_stories': df_dim_inspirational_stories,
        'dim_music': df_dim_music,
        'dim_forum': df_dim_forum,
        'dim_post': df_dim_post,
        'dim_user_moods': df_dim_user_moods,
        'dim_complaints': df_dim_complaints,
        'dim_consultations': df_dim_consultations,
        'dim_transactions': df_dim_transactions,
        'dim_ratings': df_dim_ratings,
        'fact_article_fix': df_fact_article_fix,
        'fact_music_fix': df_fact_music_fix,
        'fact_story_fix': df_fact_story_fix,
        'fact_post_fix': df_fact_post_fix,
        'fact_consultation_doctor_fix': df_fact_consultation_doctor,
        'fact_consultation_user_fix': df_fact_consultation_user,
        'fact_transaction_fix': df_fact_transaction_fix,
        'fact_rating_fix': df_fact_rating_fix,
    }

@task
def load_data(transformed_data):
    # Set credentials
    # credentials = service_account.Credentials.from_service_account_file('./serviceAccount.json')
    # project_id = 'alterra-capstone-426112'
    # dataset_id = 'data_engineer'

    # client = bigquery.Client(project=project_id, credentials=credentials)

    # for table_name, df in transformed_data.items():
    #     table_id = f'{project_id}.{dataset_id}.{table_name}'

    #     if table_name.startswith('dim_'):
    #         df.to_gbq(destination_table=table_id, project_id=project_id, if_exists='replace', credentials=credentials)
    #     else:
    #         # Keep duplicates if the table starts with 'fact_'
    #         df.to_gbq(destination_table=table_id, project_id=project_id, if_exists='replace', credentials=credentials)

    #     # Load data to BigQuery using Pandas to_gbq

    #     # Optionally, ensure the table exists
    #     client.create_table(table_id, exists_ok=True)

    credentials = service_account.Credentials.from_service_account_file('./serviceAccount.json')
    project_id = 'alterra-capstone-426112'
    dataset_id = 'data_engineer'

    client = bigquery.Client(project=project_id, credentials=credentials)

    for table_name, df in transformed_data.items():
        table_id = f'{project_id}.{dataset_id}.{table_name}'

        # Memeriksa apakah tabel sudah ada
        table_ref = client.dataset(dataset_id).table(table_name)
        try:
            client.get_table(table_ref)
        except Exception as e:
            print(f"Tabel {table_id} tidak ada. Membuat tabel baru.")
            schema = []  # Definisikan skema jika diperlukan
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)  # Buat tabel jika belum ada

        # Memeriksa awalan tabel dan menentukan write disposition
        if table_name.startswith('dim_'):
            # Tabel dim_: replace data
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        elif table_name.startswith('fact_'):
            # Tabel fact_: tambahkan data baru tanpa mengupdate yang sudah ada
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        else:
            # Default: replace data
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

        # Memuat data ke tabel
        client.load_table_from_dataframe(df, table_id, job_config=job_config)

@flow(task_runner=SequentialTaskRunner())
def etl_flow():
    data_frames = extract_data()
    transformed_data = transform_data(data_frames)
    load_data(transformed_data)

if __name__ == "__main__":
    etl_flow()
