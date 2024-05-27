import pendulum
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from helpers.MyNotifier import MyNotifier

def download_zip():
    import urllib.request
    import os
    import zipfile

    directory = "./opt/airflow/localdata/"

    os.makedirs(directory, exist_ok = True)
    # Download the dataset
    url = "http://files.grouplens.org/datasets/movielens/ml-100k.zip"
    filename = "./opt/airflow/localdata/ml-100k.zip"
    urllib.request.urlretrieve(url, filename)

    # Unzipping
    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall()

def calculate_mean():
    import pandas as pd
    from sqlalchemy import create_engine

    # load data
    user_header = ['user_id', 'age', 'sex', 'occupation', 'zip_code']
    users = pd.read_csv('ml-100k/u.user', sep='|', names=user_header, encoding='latin-1')

    # Calculate the mean age
    mean_age = users.groupby('occupation')['age'].mean()

    # Push to db
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/datastore')
    conn2 = engine.connect() 
    mean_age.to_sql('occupationwisemeanage', con=conn2, index=True, if_exists='replace')
    
def top_rated():
    import pandas as pd
    from sqlalchemy import create_engine

    # Load data
    ratings_cols = ['user_id', 'movie_id', 'rating', 'timestamp']
    ratings = pd.read_csv('ml-100k/u.data', sep='\t', names=ratings_cols, encoding='latin-1')
    movies_cols = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url']
    movies = pd.read_csv('ml-100k/u.item', sep='|', names=movies_cols, usecols=range(5), encoding='latin-1')

    # Join on movie_id
    data = pd.merge(movies, ratings, on="movie_id")

    # Processing
    mean_ratings = data.groupby('title')['rating'].agg(['mean', 'count'])
    popular_movies = mean_ratings[mean_ratings['count'] >= 35]
    top_movies = popular_movies.sort_values('mean', ascending=False).head(20)

    # Take out names from dataframe
    movie_names = pd.DataFrame(list(top_movies.index), columns=['movie_name'])

    # Push to db
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/datastore')
    conn = engine.connect() 
    movie_names.to_sql('topratedmovies', con=conn, index=False, if_exists='replace')


def top_genres():
    import pandas as pd
    from sqlalchemy import create_engine
    import numpy as np
    from psycopg2.extensions import register_adapter, AsIs
    register_adapter(np.int64, AsIs)

    # Load data
    users_header = ['user_id','age', 'gender', 'occupation', 'zip_code']
    users = pd.read_csv('ml-100k/u.user', sep='|', names=users_header)
    ratings_header = ['user_id', 'movie_id', 'rating', 'timestamp']
    ratings = pd.read_csv('ml-100k/u.data', sep='\t', names=ratings_header)
    movies_header = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url', 'unknown', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
    movies = pd.read_csv('ml-100k/u.item', sep='|', names=movies_header, encoding='latin-1')

    # Join datasets
    data = pd.merge(pd.merge(ratings, users), movies)

    # create ad apply labels
    bins = [0, 20, 25, 35, 45, 100]
    labels = ['<20', '20-24', '25-34', '35-44', '45+']
    data['age_group'] = pd.cut(data['age'], bins=bins, labels=labels, right=False)

    # Group by occupation, age group and array[genres], then calculate the mean rating
    grouped = data.groupby(['occupation', 'age_group', 'unknown', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western' ])['rating'].agg(['mean'])

    # For each occupation and age group, find the genre with the highest mean rating
    top_genres = grouped.groupby(['occupation', 'age_group']).idxmax()

    # REmoving NAN
    top_genres = top_genres.dropna()

    genres = ['unknown', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']

    def map_genres(row):
        tmp = row[2:]
        return [genre for genre, value in zip(genres, tmp) if value == 1]

    # Get Genre Names for final storing
    top_genres['topgenre'] = top_genres[top_genres.columns[0]].apply(map_genres)

    top_genres = top_genres.drop(top_genres.columns[0], axis=1)

    # Push to db
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/datastore')
    conn = engine.connect() 
    top_genres.to_sql('topgenres', con=conn, index=True, if_exists='replace')



with DAG(
    dag_id="pipeline2",
    start_date=pendulum.datetime(2024, 5, 25, tz="UTC"),
    schedule_interval= '0 20 * * Mon-Fri',
    concurrency = 1,
    on_success_callback=MyNotifier(message="Success!"),
    on_failure_callback=MyNotifier(message="Failure!")
) as dag:
    
    task1 = PythonOperator(
        task_id = "download_zip",
        python_callable=download_zip,
        on_success_callback=MyNotifier(message="Success!"),
        on_failure_callback=MyNotifier(message="Failure!")
    )

    task2= PythonOperator(
        task_id = "calculate_mean",
        python_callable=calculate_mean,
        on_success_callback=MyNotifier(message="Success!"),
        on_failure_callback=MyNotifier(message="Failure!")
    )

    task3 = PythonOperator(
        task_id = "top_rated_movies",
        python_callable=top_rated,
        on_success_callback=MyNotifier(message="Success!"),
        on_failure_callback=MyNotifier(message="Failure!")
    )

    task4 = PythonOperator(
        task_id = "top_genres",
        python_callable=top_genres,
        on_success_callback=MyNotifier(message="Success!"),
        on_failure_callback=MyNotifier(message="Failure!")  
    )

    # Adding external dependency task on web_scrapper dag
    check_task = ExternalTaskSensor(
        task_id='check_task',
        external_dag_id='web_scrapper',
        allowed_states=['success'],
        soft_fail=True,
        execution_delta=datetime.timedelta(hours=1),
        dag=dag,
        timeout = datetime.timedelta(seconds=60),
        deferrable=False
    )
    check_task >> task1 >> [task2,task3, task4]
