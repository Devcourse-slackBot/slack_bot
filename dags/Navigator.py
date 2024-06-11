from airflow import DAG
from airflow.macros import *
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

import os
from glob import glob
import logging
import subprocess
import json
import requests


DAG_ID = "Navigator_v1"
KAKAO_API_KEY = Variable.get("KAKAO_API_KEY")


def gps_api(address):
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    params = {
        "query" : address
    }
    headers = {
        "Authorization": f"KakaoAK {KAKAO_API_KEY}"
    }
    res = requests.get(url, headers=headers, params=params)
    results = json.loads(res.text)
    x = results["documents"][0]["x"]
    y = results["documents"][0]["y"]
    return x + "," + y


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def read_meta_from_redshift():
    cur = get_Redshift_connection()
    schema = "cjswldn99"
    table = "morning_slack_user_info"

    query = f"SELECT * FROM {schema}.{table};"

    try:
        cur.execute(query)
        records = cur.fetchall()
        return records[0]
    except Exception as e:
        print(f"Error executing query: {e}")
        raise
    finally:
        cur.close()


@task
def read_meta():
    meta_query_result = read_meta_from_redshift()
    logging.info(meta_query_result)
    origin_address = meta_query_result[0]
    destination_address = meta_query_result[1]
    return {
        "origin" : gps_api(origin_address),
        "destination" : gps_api(destination_address)
    }


@task
def navi_api(meta):
    url = "https://apis-navi.kakaomobility.com/v1/directions"
    params = {
        "origin" : meta["origin"],
        "destination" : meta["destination"],
        "waypoints": "",
        "priority": "RECOMMEND",
        "car_fuel": "GASOLINE",
        "car_hipass": "false",
        "alternatives": "false",
        "road_details": "false"
    }
    
    headers = {
        "Authorization": f"KakaoAK {KAKAO_API_KEY}"
    }

    res = requests.get(url, headers=headers, params=params)
    results = json.loads(res.text)
    logging.info(res.text)
    return results


@task
def transform(api_results):
    duration = api_results["routes"][0]["summary"]["duration"]
    logging.info(f"duration: {duration}")
    return {
        "duration" : duration
    }


@task
def upload():
    pass


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 8 * * *",
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    start_date=datetime(2024, 6, 10),
    default_args= {
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    meta = read_meta()
    api_results = navi_api(meta)
    transformed = transform(api_results)

