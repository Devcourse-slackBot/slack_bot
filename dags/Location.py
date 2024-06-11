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

from utils.forecast_grid import grid


DAG_ID = "Location_v1"
KAKAO_API_KEY = Variable.get("KAKAO_API_KEY")


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


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
    x = results["documents"][0]["x"]    # 경도 (Longitude)
    y = results["documents"][0]["y"]    # 위도 (Latitude)
    return {
        "longitude" : x,
        "latitude" : y
    }


@task
def read_cities():
    cur = get_Redshift_connection()
    schema = "cjswldn99"
    table = "morning_slack_user_info"

    query = f"SELECT * FROM {schema}.{table};"

    try:
        cur.execute(query)
        records = cur.fetchall()
        logging.info(records)
        return {
            "origin_address" : records[0][0],
            "destination_address" : records[0][1]
        }
    except Exception as e:
        print(f"Error executing query: {e}")
        raise
    finally:
        cur.close()


@task
def city_to_gps(cities):
    origin_gps = gps_api(cities["origin_address"])
    destination_gps = gps_api(cities["destination_address"])
    logging.info(f"origin: {origin_gps}")
    logging.info(f"destination: {destination_gps}")
    return {
        "origin_gps" : origin_gps,
        "destination_gps" : destination_gps
    }


@task
def gps_to_forecast_grid(gps):
    origin_lat, origin_long = float(gps["origin_gps"]["latitude"]), float(gps["origin_gps"]["longitude"])
    destination_lat, destination_long = float(gps["destination_gps"]["latitude"]), float(gps["destination_gps"]["longitude"])
    origin_grid = grid(latitude=origin_lat, longitude=origin_long)
    destination_grid = grid(latitude=destination_lat, longitude=destination_long)
    logging.info(f"origin: {origin_grid}")
    logging.info(f"destination: {destination_grid}")
    return {
        "origin_grid": origin_grid,
        "destination_grid": destination_grid
    }


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
    cities = read_cities()
    gps_info = city_to_gps(cities)
    gird_info = gps_to_forecast_grid(gps_info)

