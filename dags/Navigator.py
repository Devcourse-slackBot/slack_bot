from airflow import DAG
from airflow.macros import *
from airflow.models import Variable
from airflow.decorators import task

import os
from glob import glob
import logging
import subprocess
import json
import requests


DAG_ID = "Navigator_v1"
KAKAO_API_KEY = Variable.get("KAKAO_API_KEY")
ORIGIN_ADDRESS = "서울 서초구 서초대로38길 12"
DESTINATION_ADDRESS = "경기 성남시 분당구 정자일로 95 네이버 1784"

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

@task
def load_meta():
    return {
        "origin" : gps_api(ORIGIN_ADDRESS),
        "destination" : gps_api(DESTINATION_ADDRESS)
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
    meta = load_meta()
    api_results = navi_api(meta)
    transformed = transform(api_results)

