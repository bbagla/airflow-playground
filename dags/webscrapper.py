import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import psycopg2

def scrap_finshots():
    import requests
    import json
    from bs4 import BeautifulSoup
    import hashlib

    # Define the tickers and the data source
    tickers = ["Tata Motors", "HDFC"]
    data_source = "https://backend.finshots.in/backend"
    # data_source = "http://yourstory.com"        

    def get_sentiment_scores(text):
        # For now just creating a hash
        hashed_text = hashlib.sha256(text.encode()).hexdigest()
        int_hash = int(hashed_text, 16)
        float_hash = int_hash / float(1 << 256)
        return float_hash

    
    # Function to fetch articles
    def fetch_articles(ticker):
        # Make a request to the website search link
        r = requests.get(f"{data_source}/search?q={ticker}")
        r.raise_for_status()
        json_val = json.loads(r.text)
        json_val['matches'].sort(key=lambda x:x['published_date'], reverse=True)

        conn = psycopg2.connect(
            dbname="datastore",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
        )
        cur = conn.cursor()

        # iterate over top 5 latest matched articles
        for obj in json_val['matches'][0:5]:
            rq = requests.get(obj["post_url"])
            # Parsing usine bs4
            soup = BeautifulSoup(rq.text, 'html.parser')
            title = soup.find('h1').text
            content = soup.find('div', class_='post-content').text
            score = get_sentiment_scores(content)
            title = title.replace("'", "")
            content = content.replace("'", "")
            # tuple = (title,content, score)

            # delete record if already exists
            cur.execute(f"DELETE FROM articlescore where title = '{title}'")
            # add new record
            cur.execute(f"INSERT INTO articlescore (title, content, score) VALUES ('{title}','{content}',{score})")
        
        cur.close() 
        # commit the changes 
        conn.commit() 
        conn.close()





    # Fetch articles for each ticker
    for ticker in tickers:
        print(f"Fetching articles for {ticker}...")
        fetch_articles(ticker)
        print("\n")

def create_db():
    conn = psycopg2.connect(
            dbname="datastore",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
        )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS articlescore(
            title character varying,
            content character varying,
            score float,
            primary key (title)
        )
        """
    )
    cur.close() 
    # commit the changes 
    conn.commit() 
    conn.close()


with DAG(
    dag_id="web_scrapper",
    start_date=pendulum.datetime(2024, 5, 25, tz="UTC"),
    schedule_interval= '0 19 * * Mon-Fri',
    concurrency = 1
) as dag:
    
    task1 = PythonOperator(
        task_id = "creatdb",
        python_callable=create_db
    )

    task2= PythonOperator(
        task_id = "finshots",
        python_callable=scrap_finshots
    )
    
    task1 >> task2
    