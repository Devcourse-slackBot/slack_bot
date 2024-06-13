import json
import logging
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.models.blocks import SectionBlock, ActionsBlock, ButtonElement, Option, DividerBlock
import psycopg2

#! 설정 파일 읽기 - 개인 환경에 맞게 설정필요.
with open("/opt/airflow/slack_info.json") as f:
    config = json.load(f)

# 특정 채널 ID 설정
TARGET_CHANNEL_ID = 'C075U6B360P'

# DB 접속정보 [slack_info.json]
dbname = config['DB_NAME']
user = config['USER']
password = config['PASSWORD']
host = config['HOST']
port = config["PORT"]
schema = config["SCHEMA"]

app = App(token=config["SLACK_BOT_TOKEN"])

# 사용자의 상태를 저장할 딕셔너리
user_states = {}

# 로거 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# DB 연결 테스트
def connect_to_redshift(dbname,user,password,host,port,schema):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        return conn
      
    except Exception as e:
        print(f"Error : {e}")
        raise
        

def reset_user_state(user_id):
    if user_id in user_states:
        user_states.pop(user_id)

def display_initial_options(say):
    blocks = [
        SectionBlock(
            block_id="section1",
            text="무엇을 도와드릴까요?"
        ),
        ActionsBlock(
            block_id="actions1",
            elements=[
                ButtonElement(
                    text="날씨를 알려줘",
                    action_id="button_weather",
                    value="weather"
                ),
                ButtonElement(
                    text="뉴스를 보여줘",
                    action_id="button_news",
                    value="news"
                )
            ]
        )
    ]
    say(blocks=blocks)

def get_weather_from_db(location):
    # 이 함수는 데이터베이스에서 날씨 정보를 읽어오는 함수입니다.

    # DB 접속 및 슬랙봇 메세지 전달 테스트
    conn = connect_to_redshift(dbname,user,password,host,port,schema)
    try:
        with conn.cursor() as cur :
            cur.execute(f"SELECT * FROM {schema}.nps_summary LIMIT 1 ")
            result = cur.fetchone()
            print(result)
            if result :
                return result[0]
            else :
                return None
    except Exception as e:
        logger.error(f'Error fetching weather data : {e}')
        return None
    finally:
        conn.close()
    
def get_news(category):
    # 카테고리별 최신뉴스 3개를 가져와서 보여준다.
    category_map = {
        "비즈니스": "business",
        "테크": "technology",
        "연예": "entertainment",
        "스포츠": "sports",
        "건강" : "health",
        "과학" : "science"
    }
    category_english = category_map.get(category)
    if not category_english:
        logger.error(f'Invalid category: {category}')
        return None
    
    conn = connect_to_redshift(dbname, user, password, host, port, schema)
    query = f"""SELECT title, description, url FROM news_{category_english} ORDER BY published_at DESC LIMIT 3"""
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            news_data = [{"title": row[0], "url": row[2]} for row in result]
    except Exception as e:
        logger.error(f'Error fetching news data: {e}')
        return None
    finally:
        conn.close()

    return news_data

@app.event("app_mention")
def handle_app_mention_events(body, say, logger):
    event = body['event']
    user_id = event['user']
    channel_id = event['channel']
    text = event['text']

    logger.info(f"Received app_mention event: {body}")  # 로그 출력 추가

    if channel_id != TARGET_CHANNEL_ID:
        return

    if "처음" in text.strip().lower():
        reset_user_state(user_id)
        say("처음부터 다시 시작합니다. 무엇을 도와드릴까요?")
        display_initial_options(say)
        return

    if user_id not in user_states:
        display_initial_options(say)
        user_states[user_id] = 'waiting_for_selection'
    elif user_states[user_id] == 'waiting_for_location':
        location = text.strip()
        weather_info = get_weather_from_db(location)
        if weather_info:
            say(f"{location}의 날씨는 {weather_info}입니다.")
        else:
            say(f"{location}의 날씨 정보를 가져올 수 없습니다.")
        
        reset_user_state(user_id)

@app.event("message")
def handle_message_events(body, say, logger):
    logger.info(f"Received message event: {body}")
    
    event = body['event']
    user_id = event['user']
    text = event['text']
    if "처음" in text.strip().lower():
        reset_user_state(user_id)
        say("처음부터 다시 시작합니다. 무엇을 도와드릴까요?")
        display_initial_options(say)
        return

    if user_id in user_states and user_states[user_id] == 'waiting_for_location':
        location = text.strip()
        weather_info = get_weather_from_db(location)
        if weather_info:
            say(f"{location}의 날씨는 {weather_info}입니다.")
        else:
            say(f"{location}의 날씨 정보를 가져올 수 없습니다.")
        reset_user_state(user_id)

@app.action("button_weather")
def handle_button_weather(ack, body, say):
    ack()
    user_id = body['user']['id']
    say("어느 지역의 날씨를 알려드릴까요?")
    user_states[user_id] = 'waiting_for_location'

@app.action("button_news")
def handle_button_news(ack, body, say):
    ack()
    user_id = body['user']['id']
    
    blocks = [
        SectionBlock(
            block_id="section2",
            text="어떤 종류의 뉴스를 보여드릴까요?"
        ),
        ActionsBlock(
            block_id="actions2",
            elements=[
                ButtonElement(
                    text="비즈니스",
                    action_id="news_business",
                    value="비즈니스"
                ),
                ButtonElement(
                    text="건강",
                    action_id="news_health",
                    value="건강"
                ),
                ButtonElement(
                    text="연예",
                    action_id="news_entertainment",
                    value="연예"
                ),
                ButtonElement(
                    text="스포츠",
                    action_id="news_sports",
                    value="스포츠"
                ),
                ButtonElement(
                    text="테크",
                    action_id="news_technology",
                    value="테크"
                ),
                ButtonElement(
                    text="과학",
                    action_id="news_science",
                    value="과학"
                )
            ]
        )
    ]
    say(blocks=blocks)
    user_states[user_id] = 'waiting_for_news_category'

@app.action("news_business")
@app.action("news_technology")
@app.action("news_entertainment")
@app.action("news_sports")
@app.action("news_science")
@app.action("news_health")
def handle_news_category_selection(ack, body, say):
    ack()
    user_id = body['user']['id']
    action_id = body['actions'][0]['action_id']
    category_map = {
        "news_business": "비즈니스",
        "news_technology": "테크",
        "news_entertainment": "연예",
        "news_sports": "스포츠",
        "news_health": "건강",
        "news_science": "과학"
    }
    category = category_map.get(action_id, "")
    if category:
        news_data = get_news(category)
        if news_data:
            blocks = [
                SectionBlock(block_id="section3", text=f"오늘의 {category} 뉴스입니다:"),
                DividerBlock()
            ]
            for news in news_data:
                blocks.append(SectionBlock(text=f"<{news['url']}|{news['title']}>"))
            say(blocks=blocks)
        else:
            say(f"{category} 뉴스 데이터를 가져오는 데 문제가 발생했습니다.")
        reset_user_state(user_id)
    else:
        say("잘못된 카테고리 선택입니다. 다시 시도해주세요.")

if __name__ == "__main__":
    handler = SocketModeHandler(app, config["SLACK_APP_TOKEN"])
    handler.start()
