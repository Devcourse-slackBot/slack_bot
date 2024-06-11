from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import logging
import requests

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract(url, nx, ny):
    # 기상청 단기 예보 조회
    user_agent = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"
    }
    base_date, base_time = get_base_dateTime()
    logging.info(f"{base_date} {base_time}")
    fcst_api_key = Variable.get("data_go_fcst_secret_api_key")
    params = {
        "pageNo": '1',
        "numOfRows": '870',  # 최대 일일 기상정보 record 갯수 = 290개
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
        "serviceKey": fcst_api_key,
    }
    response = requests.get(url=url, headers=user_agent, params=params)
    logging.info(response.text)
    return response.json()['response']['body']['items']['item']


def get_base_dateTime():
    # airflow에서는 UTC 기준이므로 한국시간에 맞게 9시간 더하기
    kr_now = datetime.now() + timedelta(hours=9)
    base_date = kr_now.strftime("%Y%m%d")
    base_time = kr_now.strftime("%H00") # 정각으로 시간 조정
    return base_date, base_time


@task
def transform(data):
    fcst_dict = {}
    """
    반환 값들, SKY, PTY, PCP는 형변환 진행
    TMP: 기온 int
    SKY: 하늘상태 int to varchar: '1' to '맑음', '2' to '구름조금', '3' to '구름많음', '4' to '흐림'
    PTY: 강수형태 int to varchar: '0' to '강수없음', '1' to '비', '2' to '눈/비', '3' to '눈', '4' to '소나기'
    REH: 상대습도 int
    PCP: 강수량 varchar or float
    TMN: 최저기온(Nullable) int, 06시(fcstTime) 예보정보만 있음, 최초 예보부터 예보 발표 시간 기준(baseTime) 당일 02시까지만 존재
    TMX: 최고기온(Nullable) int, 15시(fcstTime) 예보 정보에만 있음, 최초 예보부터 예보 발표 시간 기준(baseTime) 당일 11시까지만 존재
    """
    target_keys = ['TMP', 'SKY', 'PTY', 'REH', 'PCP', 'TMN', 'TMX']
    for row in data:
        # airflow 에서는 키 값으로 datetime field가 들어올 수 없으므로 원하는 형식으로 datetime 구성 후, 문자열로 다시 변환
        fcst_dateTime = datetime.strptime(f"{row['fcstDate']}{row['fcstTime']}", "%Y%m%d%H%M").strftime("%Y-%m-%d %H:%M")
        category = row['category']
        if category in target_keys:
            fcst_value = transform_fcst_value(category, row['fcstValue'])
            fcst_dict[fcst_dateTime] = dict(fcst_dict.get(fcst_dateTime, {}), **{category: fcst_value})
    for k, v in fcst_dict.items():
        logging.info(f"{k}: {v}")

    # print(len(fcst_dict))
    return fcst_dict


def transform_fcst_value(category, fcst_value):
    if category == 'SKY':
        fcst_value = transform_sky(sky_value=fcst_value)
    elif category == 'PTY':
        fcst_value = transform_pty(pty_value=fcst_value)
    elif category == 'PCP':
        fcst_value = transform_pcp(pcp_value=fcst_value)
    else:
        pass
    return fcst_value


def transform_sky(sky_value):
    """
    api를 통해 받은 하늘 상태 코드를 읽을 수 있는 문자열로 변환
    :param sky_value: 하늘 상태 코드, '1', '2', '3', '4' 문자열
    :return: 맑음, 구름조금, 구름많음, 흐림 중 하나의 문자열
    """
    sky_map = {'1': '맑음', '2': '구름조금', '3': '구름많음', '4': '흐림'}
    return sky_map.get(sky_value, None)


def transform_pty(pty_value):
    """
    api를 통해 받은 강수 형태 코드를 읽을 수 있는 문자열로 변환
    :param pty_value: 강수 형태 코드, '0', '1', '2', '3', '4' 문자열
    :return: 강수없음, 비, 눈/비, 눈, 소나기 중 하나의 문자열
    """
    pty_map = {'0': '강수없음', '1': '비', '2': '눈/비', '3': '눈', '4': '소나기'}
    return pty_map.get(pty_value, None)


def transform_pcp(pcp_value):
    """
    api를 통해 받은 강수 형태 정보를 실수 문자열로 변환
    사실상 '강수없음' 을 '0'으로 변환하는 함수
    :param pcp_value: '강수없음' 문자열, 혹은 '1.0', '30.2' 형태의 실수 문자열
    :return: '0' 혹은 실수 형태의 문자열
    """
    if pcp_value == '강수없음':
        return '0'
    return pcp_value


@task
def load(schema, table, fcst_dict):
    """
    Redshift에 시간별 날씨 정보를 Incremental Update를 수행
    예측 날짜 시간인 fcst_timestamp를 기준으로 가장 최신의 레코드들을 적재하여 멱등성을 보장,
    하지만, 그 날의 최고기온인 tmx와 최저기온인 tmn은 최신 레코드에서 Null로 들어오는 경우가 있어, 이 경우에는 기존의 값을 유지하도록 함.
    적재 방식은
    1. 원본 테이블을 복사한 임시테이블에 새 레코드들을 적재하고,
    2. 원본 테이블의 내용을 모두 지운후,
    2. 임시 테이블로부터 최신 레코드들을 선택(여기서 tmx, tmn은 최신 값이 null인 경우 기존의 값을 가져옴)
    3. 선택한 레코드들을 다시 원본 테이블로 적재
    4. 최종적으로 최신 레코드들만 남게되어 멱등성을 보장
    :param schema: 테이블을 적재할 스키마 이름
    :param table: 만들 테이블 이름
    :param fcst_dict: 적재할 딕셔너리 형태의 시간별 날씨 정보 레코드들
    :return: None
    """
    cur = get_Redshift_connection(False)
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
        fcst_timestamp timestamp primary key,
        tmp integer,
        sky varchar(50),
        pty varchar(50),
        reh integer,
        pcp float,
        tmx float,
        tmn float,
        updated_at timestamp default GETDATE()
    );"""
    logging.info(create_table_sql)

    # 임시 테이블 생성
    create_t_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""
    logging.info(create_t_sql)
    try:
        cur.execute(create_table_sql)
        cur.execute(create_t_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 임시 테이블 데이터 입력
    try:
        for pk, record in fcst_dict.items():
            insert_sql = """INSERT INTO t VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
            logging.info(
                insert_sql % (
                    pk, record['TMP'], record['SKY'],
                    record['PTY'], record['REH'], record['PCP'],
                    record.get('TMX'), record.get('TMN'))
            )
            cur.execute(
                insert_sql,
                (pk, record['TMP'], record['SKY'],
                 record['PTY'], record['REH'], record['PCP'],
                 record.get('TMX', None), record.get('TMN', None))
            )
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    logging.info("Load to temp table done")

    # 기존 테이블 대체
    alter_sql = f"""DELETE FROM {schema}.{table};
    INSERT INTO {schema}.{table}
    SELECT fcst_timestamp, tmp, sky, pty, reh, pcp,
      CASE
        WHEN(tmx IS NOT NULL) then tmx
        ELSE max_tmx
      END as tmx,
      CASE
        WHEN(tmn IS NOT NULL) then tmn
        ELSE max_tmn
      END as tmn
    FROM 
      (SELECT *,  
        MAX(tmx) OVER(PARTITION BY fcst_timestamp) as max_tmx,
        MAX(tmn) OVER(PARTITION BY fcst_timestamp) as max_tmn,
        ROW_NUMBER() OVER(PARTITION BY fcst_timestamp ORDER BY updated_at DESC) as seq
      FROM t)
    WHERE seq = 1;
    """
    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done successfully")


with DAG(
        dag_id='Hourly_forecast',
        start_date=datetime(2024, 6, 11),  # 날짜가 미래인 경우 실행이 안됨
        schedule='30 2,5,8,11,14,17,20,23 * * *',  # 매일 02시 30분부터 세시간 간격
        max_active_runs=1,
        catchup=False,
        default_args={
            'retries': 1,
            'retry_delay': timedelta(minutes=1),
        }
) as dag:
    url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
    schema = 'volcano0204'
    table = 'hourly_forecast'
    SEOUL_GANGNAM_NX = '61' # 서울시 강남구 좌표
    SEOUL_GANGNAM_NY = '126'

    fcst_dict = transform(extract(url, SEOUL_GANGNAM_NX, SEOUL_GANGNAM_NY))
    load(schema, table, fcst_dict)