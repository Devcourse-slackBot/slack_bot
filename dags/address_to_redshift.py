from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import re
from sqlalchemy import create_engine
import json
import os

# Slack 봇 토큰 및 Redshift 연결 정보 설정
SLACK_BOT_TOKEN = "xoxb-7214094977028-7277978237584-WwzUd1iixh0Z9ggAyAndByN0"
REDSHIFT_CONN_STRING = 'redshift+psycopg2://wnstkd525:Wnstkd525!1@learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev'
CHANNEL_ID = "C075T29797Z"
SLACK_COMMAND = "적재"
SCHEMA = "wnstkd525"
TABLE = "user_data"
EXECUTE_TIME = '0 1 * * *'

# Slack 메시지를 파싱하는 함수
def parse_message(text):
    pattern = r"\(([^,]+),\s*([^,]+),\s*([^,]+)\)"
    match = re.search(pattern, text)
    if match:
        return match.groups()
    return None

# Redshift에 메시지를 적재하는 함수
def post_message_to_redshift(city1, city2, time):
    engine = create_engine(REDSHIFT_CONN_STRING)
    with engine.connect() as connection:
        try:
            # 테이블이 존재하면 삭제하고 새로 생성
            drop_table_query = f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE}"
            create_table_query = f"""
            CREATE TABLE {SCHEMA}.{TABLE} (
                city1 VARCHAR(255),
                city2 VARCHAR(255),
                time VARCHAR(255)
            )
            """
            connection.execute(drop_table_query)
            connection.execute(create_table_query)
            
            # 데이터 삽입
            insert_query = f"INSERT INTO {SCHEMA}.{TABLE} (city1, city2, time) VALUES ('{city1}', '{city2}', '{time}')"
            connection.execute(insert_query)
            print(f"Data inserted into Redshift: city1={city1}, city2={city2}, time={time}")
        except Exception as e:
            print(f"Error inserting data into Redshift: {e}")

# Slack 메시지를 처리하는 함수
def handle_slack_message(event):
    text = event.get("text")
    if text.startswith(SLACK_COMMAND):
        parsed = parse_message(text[len(SLACK_COMMAND):].strip())
        if parsed:
            city1, city2, time = parsed
            with open('/tmp/latest_message.json', 'w') as f:
                json.dump({"city1": city1, "city2": city2, "time": time}, f)
            print(f"Message parsed and saved: city1={city1}, city2={city2}, time={time}")
        else:
            print("Message parsing failed")
    else:
        print("Message does not start with the command")

# Airflow DAG에서 호출되는 함수 - 메시지 수집
def fetch_messages(**kwargs):
    client = WebClient(token=SLACK_BOT_TOKEN)
    try:
        response = client.conversations_history(channel=CHANNEL_ID)
        print(f"Fetched messages: {response['messages']}")
        for message in response['messages']:
            handle_slack_message(message)
    except SlackApiError as e:
        print(f"Error fetching conversations: {e.response['error']}")

# Airflow DAG에서 호출되는 함수 - Redshift 적재
def load_to_redshift(**kwargs):
    try:
        file_path = '/tmp/latest_message.json'
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)
                print(f"Loaded data from file: {data}")
                post_message_to_redshift(data['city1'], data['city2'], data['time'])
        else:
            print("No message to load, file does not exist")
    except Exception as e:
        print(f"Error loading data from file: {e}")

# Airflow DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Airflow DAG 정의
dag = DAG(
    'slack_command_to_redshift',
    default_args=default_args,
    description='Slack 명령어로 입력된 메시지를 Redshift에 저장',
    schedule_interval=EXECUTE_TIME,
    catchup=False,
)

# Airflow PythonOperator 정의
t1 = PythonOperator(
    task_id='fetch_messages',
    provide_context=True,
    python_callable=fetch_messages,
    dag=dag,
)

t2 = PythonOperator(
    task_id='load_to_redshift',
    provide_context=True,
    python_callable=load_to_redshift,
    dag=dag,
)

t1 >> t2
