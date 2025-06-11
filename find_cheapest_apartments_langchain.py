import os
import pandas as pd
import logging
from datetime import datetime
import json
import re
import psycopg2
from decimal import Decimal
from langchain_community.utilities import SQLDatabase
from load_env import load_environment_variables
from dotenv import load_dotenv
import asyncio
import aiohttp
import html

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/find_apartments_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_environment_variables()
load_dotenv()

# Проверяем загрузку переменных окружения
logger.info("Проверка переменных окружения:")
logger.info(f"OPENROUTER_API_KEY: {'Найден' if os.getenv('OPENROUTER_API_KEY') else 'Не найден'}")
logger.info(f"TELEGRAM_BOT_TOKEN: {'Найден' if os.getenv('TELEGRAM_BOT_TOKEN') else 'Не найден'}")
logger.info(f"TELEGRAM_CHAT_ID: {'Найден' if os.getenv('TELEGRAM_CHAT_ID') else 'Не найден'}")

# Функция для сериализации Decimal в JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# Создаем строку подключения
CONNECTION_STRING = f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"

# Директория для сохранения результатов
RESULTS_DIR = "analysis_results"
os.makedirs(RESULTS_DIR, exist_ok=True)
RESULT_FILENAME = f"cheapest_apartments_with_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHANNEL_ID = os.getenv('TELEGRAM_CHANNEL_ID')

def extract_neighborhood(location_json):
    """Извлекает название района из JSON строки местоположения"""
    try:
        # Сначала пытаемся исправить JSON с одинарными кавычками
        if location_json and "'" in location_json:
            location_json = location_json.replace("'", '"')
        
        # Парсим JSON
        data = json.loads(location_json)
        
        # Ищем элемент с type = neighbourhood
        for item in data:
            if item.get('type') == 'neighbourhood':
                return item.get('name')
        
        # Ищем элемент с level = 2 (обычно это район)
        for item in data:
            if item.get('level') == 2:
                return item.get('name')
        
        # Если не нашли ни по типу, ни по уровню, берем город (level = 1)
        for item in data:
            if item.get('level') == 1:
                return item.get('name')
        
        # Если ничего не нашли, возвращаем первый элемент name
        if data and len(data) > 0:
            return data[0].get('name')
        
        return None
    except (json.JSONDecodeError, TypeError) as e:
        # Если JSON невалидный, пытаемся извлечь название с помощью регулярных выражений
        try:
            # Ищем тип neighbourhood
            neighbourhood_match = re.search(r'"type": "neighbourhood".*?"name": "([^"]+)"', location_json)
            if neighbourhood_match:
                return neighbourhood_match.group(1)
            
            # Ищем уровень 2
            level2_match = re.search(r'"level": 2.*?"name": "([^"]+)"', location_json)
            if level2_match:
                return level2_match.group(1)
            
            # Ищем уровень 1
            level1_match = re.search(r'"level": 1.*?"name": "([^"]+)"', location_json)
            if level1_match:
                return level1_match.group(1)
            
            # Ищем любое название
            name_match = re.search(r'"name": "([^"]+)"', location_json)
            if name_match:
                return name_match.group(1)
        except:
            pass
        
        logger.error(f"Ошибка при извлечении района: {e}")
        return None

def get_db_connection():
    """Создает и возвращает соединение с базой данных"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.set_client_encoding('UTF8')
        logger.info("Соединение с базой данных успешно установлено")
        return conn
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных: {e}")
        return None

def get_cheapest_apartments():
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return pd.DataFrame()
        
        cursor = conn.cursor()
        
        # Проверяем существование представления bayut_api_view
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.views 
                WHERE table_name = 'bayut_api_view'
            )
        """)
        
        view_exists = cursor.fetchone()[0]
        table_name = 'bayut_api_view' if view_exists else 'bayut_properties'
        
        # Запрос для получения детальной информации о квартирах
        query = f"""
        SELECT 
            id, 
                title, 
                price, 
                rooms, 
                baths, 
                area, 
                rent_frequency,
                location, 
                property_type,
                property_url,
                furnishing_status,
                completion_status,
                amenities,
                agency_name
            FROM {table_name}
            WHERE price > 0 AND price <= %s
            ORDER BY price
            LIMIT %s
        """
        
        cursor.execute(query, (5000000, 15))
        apartments = cursor.fetchall()
        
        # Преобразуем результаты в список словарей
        columns = ['id', 'title', 'price', 'rooms', 'baths', 'area', 
                  'rent_frequency', 'location', 'property_type', 'property_url',
                  'furnishing_status', 'completion_status', 'amenities', 'agency_name']
        
        result = pd.DataFrame(apartments, columns=columns)
        
        cursor.close()
        logger.info(f"Получено {len(result)} квартир из базы данных")
        return result
    
    except Exception as e:
        logger.error(f"Ошибка при получении дешевых квартир: {e}")
        return pd.DataFrame()
    
    finally:
        if conn:
            conn.close()

def clean_html_and_sanitize(text):
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', text)
    return text

def split_text_into_chunks(text, max_length=3000):
    chunks = []
    current_chunk = ""
    paragraphs = text.split('\n')
    for paragraph in paragraphs:
        if len(current_chunk) + len(paragraph) + 1 > max_length:
            chunks.append(current_chunk.strip())
            current_chunk = paragraph + "\n"
        else:
            current_chunk += paragraph + "\n"
    if current_chunk.strip():
        chunks.append(current_chunk.strip())
    return chunks

def format_apartments_report(df):
    if df.empty:
        return "Не найдено квартир, соответствующих заданным критериям."
    output = "Три самых дешевых квартиры (площадь до 40 кв.м.) в каждой локации:\n\n"
    for location, group in df.groupby('location'):
        output += f"Локация: {location}\n"
        output += "-" * 30 + "\n"
        for i, (_, row) in enumerate(group.sort_values('price').iterrows(), 1):
            output += f"{i}. {row['title']}\n"
            output += f"   ID: {row['id']}\n"
            output += f"   Цена: {float(row['price']):,.2f} AED\n"
            output += f"   Площадь: {float(row['area']):,.2f} кв.м.\n"
            output += f"   Спальни: {row['rooms']}\n"
            output += f"   Ссылка: {row['property_url']}\n"
            output += "\n"
        output += "\n"
    return output

async def send_to_telegram(text):
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    text = clean_html_and_sanitize(text)
    chunks = split_text_into_chunks(text, max_length=3000)
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        logger.error("TELEGRAM_BOT_TOKEN или TELEGRAM_CHANNEL_ID не найдены в .env!")
        return
    connector = aiohttp.TCPConnector()  # стандартный SSL
    async with aiohttp.ClientSession(connector=connector) as session:
        for i, chunk in enumerate(chunks):
            if i == 0:
                chunk = f"📊 Анализ квартир до 40 кв.м. - {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n" + chunk
            if i == len(chunks) - 1:
                chunk = chunk + "\n\n#недвижимость #анализ #инвестиции"
            try:
                async with session.post(api_url, json={"chat_id": TELEGRAM_CHANNEL_ID, "text": chunk}) as response:
                    if response.status == 200:
                        logger.info(f"Часть {i+1}/{len(chunks)} успешно отправлена в Telegram ({len(chunk)} символов)")
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка при отправке части {i+1}/{len(chunks)}: {error_text}")
                        error_file = f"error_chunk_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i}.txt"
                        with open(error_file, 'w', encoding='utf-8') as f:
                            f.write(chunk)
                        logger.info(f"Проблемный чанк сохранен в файл: {error_file}")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Ошибка при отправке части {i+1}/{len(chunks)}: {e}")
        logger.info(f"Успешно отправлено {len(chunks)} частей сообщения")

def main():
    logger.info("Проверка переменных окружения:")
    logger.info(f"TELEGRAM_BOT_TOKEN: {'Найден' if TELEGRAM_BOT_TOKEN else 'Не найден'}")
    logger.info(f"TELEGRAM_CHANNEL_ID: {'Найден' if TELEGRAM_CHANNEL_ID else 'Не найден'}")
    df = get_cheapest_apartments()
    report = format_apartments_report(df)
    print(report)
    asyncio.run(send_to_telegram(report))

if __name__ == "__main__":
    main() 