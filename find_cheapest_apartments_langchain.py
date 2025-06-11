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
from langchain_community.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from dotenv import load_dotenv
from langchain.chains import LLMChain

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f'{log_dir}/find_apartments_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
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
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# Создаем строку подключения
CONNECTION_STRING = f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"

# Директория для сохранения результатов
RESULTS_DIR = "analysis_results"
os.makedirs(RESULTS_DIR, exist_ok=True)
RESULT_FILENAME = f"cheapest_apartments_with_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

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

def get_cheapest_apartments(limit=15, max_price=5000000):
    """Получает список самых дешевых квартир из базы данных"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
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
        
        cursor.execute(query, (max_price, limit))
        apartments = cursor.fetchall()
        
        # Преобразуем результаты в список словарей
        columns = ['id', 'title', 'price', 'rooms', 'baths', 'area', 
                  'rent_frequency', 'location', 'property_type', 'property_url',
                  'furnishing_status', 'completion_status', 'amenities', 'agency_name']
        
        result = []
        for apartment in apartments:
            result.append(dict(zip(columns, apartment)))
        
        cursor.close()
        logger.info(f"Получено {len(result)} квартир из базы данных")
        return result
    
    except Exception as e:
        logger.error(f"Ошибка при получении дешевых квартир: {e}")
        return []
    
    finally:
        if conn:
            conn.close()

def analyze_apartments_with_langchain(apartments):
    """Анализирует данные о квартирах с помощью LangChain"""
    if not apartments:
        logger.warning("Нет данных о квартирах для анализа")
        return "Не удалось найти подходящие квартиры для анализа."
    
    try:
        # Подготавливаем данные для анализа
        apartments_data = []
        for i, apt in enumerate(apartments):
            apt_info = {
                "номер": i + 1,
                "id": apt.get('id'),
                "название": apt.get('title'),
                "цена": float(apt.get('price')) if apt.get('price') else 0,  # Конвертируем Decimal в float
                "комнат": apt.get('rooms'),
                "ванных": apt.get('baths'),
                "площадь": float(apt.get('area')) if apt.get('area') else 0,  # Конвертируем Decimal в float
                "местоположение": apt.get('location'),
                "тип": apt.get('property_type'),
                "ссылка": apt.get('property_url'),
                "статус_мебели": apt.get('furnishing_status'),
                "статус_завершения": apt.get('completion_status'),
                "удобства": apt.get('amenities'),
                "агентство": apt.get('agency_name')
            }
            apartments_data.append(apt_info)
        
        # Создаем текст промпта без использования PromptTemplate
        prompt_text = f"""
        Ты - аналитик недвижимости, помогающий найти лучшие предложения по квартирам в Дубае.
        
        Вот данные о {len(apartments)} самых дешевых квартирах в Дубае:
        
        {json.dumps(apartments_data, ensure_ascii=False, indent=2, cls=DecimalEncoder)}
        
        Проанализируй эти данные и предоставь подробную информацию по следующим пунктам:
        
        1. Краткий обзор рынка на основе этих данных (ценовой диапазон, типы недвижимости, районы)
        2. Топ-5 лучших предложений с точки зрения соотношения цена/качество
        3. Рекомендации по выбору квартиры в зависимости от потребностей (для инвестиций, для проживания)
        4. Анализ районов, где расположены самые дешевые квартиры
        5. Общие рекомендации для покупателей недвижимости в Дубае на основе этих данных
        
        Будь конкретным в своем анализе, используй реальные цифры из данных.
        Формат твоего ответа должен быть хорошо структурированным, с заголовками, подзаголовками и маркированными списками.
        """
        
        # Инициализируем модель LangChain
        chat_model = ChatOpenAI(temperature=0.7)
        
        # Запускаем анализ
        logger.info("Запуск анализа данных с помощью LangChain")
        
        # Используем простой запрос к модели
        from langchain_core.messages import HumanMessage
        message = HumanMessage(content=prompt_text)
        result = chat_model.invoke([message])
        
        # Получаем текст ответа из сообщения AI
        analysis_text = result.content
        
        # Форматируем результат для лучшей читаемости
        formatted_result = f"""
# Анализ дешевых квартир в Дубае

{analysis_text}

## Список проанализированных квартир

"""
        
        # Добавляем список квартир с ссылками
        for i, apt in enumerate(apartments):
            price = float(apt.get('price')) if apt.get('price') else 0
            area = float(apt.get('area')) if apt.get('area') else 0
            formatted_result += f"{i+1}. {apt.get('title')} - {price:,.0f} AED\n"
            formatted_result += f"   Локация: {apt.get('location')}, {apt.get('rooms')} комнат, {area} кв.м.\n"
            formatted_result += f"   Ссылка: {apt.get('property_url')}\n\n"
        
        logger.info("Анализ данных с помощью LangChain успешно завершен")
        return formatted_result
        
    except Exception as e:
        logger.error(f"Ошибка при анализе данных с помощью LangChain: {e}")
        return f"Произошла ошибка при анализе данных: {str(e)}"

def save_analysis_to_file(analysis_text):
    """Сохраняет результат анализа в текстовый файл"""
    if not analysis_text:
        logger.warning("Нет данных для сохранения в файл")
        return None
    
    try:
        # Создаем полный путь к файлу
        file_path = os.path.join(RESULTS_DIR, RESULT_FILENAME)
        
        # Записываем результат в файл
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(analysis_text)
        
        logger.info(f"Результат анализа сохранен в файл: {file_path}")
        return file_path
    
    except Exception as e:
        logger.error(f"Ошибка при сохранении результата в файл: {e}")
        return None

def find_cheapest_apartments():
    """Находит самые дешевые квартиры до 40 кв.м. в каждой локации и возвращает текстовый анализ"""
    try:
        # Создаем директорию для сохранения результатов анализа
        reports_dir = "reports"
        os.makedirs(reports_dir, exist_ok=True)
        
        # Подключаемся к базе данных
        print("Подключение к базе данных...")
        conn = psycopg2.connect(**DB_PARAMS)
        print("Подключение к базе данных успешно")
        
        # Выполняем запрос для получения всех квартир до 40 кв.м.
        print("Выполнение запроса для получения всех маленьких квартир...")
        query = """
        SELECT id, title, price, rooms, baths, area, location, property_url
        FROM bayut_properties
        WHERE area <= 40
        ORDER BY location, price
        """
        df = pd.read_sql_query(query, conn)
        
        # Закрываем соединение с базой
        conn.close()
        
        # Проверяем, есть ли данные
        if df.empty:
            print("Нет данных о маленьких квартирах")
            return None
        
        print(f"Получено {len(df)} квартир площадью до 40 кв.м.")
        
        # Группируем по локации и берем 3 самых дешевых квартиры в каждой локации
        result = []
        result.append("Три самых дешевых квартиры (площадь до 40 кв.м.) в каждой локации:\n")
        
        # Получаем уникальные локации и сортируем их
        locations = sorted(df['location'].unique())
        
        for location in locations:
            if not location or pd.isna(location):
                continue
                
            # Получаем 3 самые дешевые квартиры в данной локации
            cheapest = df[df['location'] == location].sort_values('price').head(3)
            
            if len(cheapest) == 0:
                continue
                
            result.append(f"Локация: {location}")
            result.append("------------------------------")
            
            for i, (_, row) in enumerate(cheapest.iterrows(), 1):
                price = float(row['price']) if not pd.isna(row['price']) else 0
                formatted_price = f"{price:,.2f}"
                
                area = float(row['area']) if not pd.isna(row['area']) else 0
                formatted_area = f"{area:.2f}"
                
                rooms = int(row['rooms']) if not pd.isna(row['rooms']) else 0
                
                result.append(f"{i}. {row['title']}")
                result.append(f"   ID: {row['id']}")
                result.append(f"   Цена: {formatted_price} AED")
                result.append(f"   Площадь: {formatted_area} кв.м.")
                result.append(f"   Спальни: {rooms}")
                result.append(f"   Ссылка: {row['property_url']}")
                result.append("")
            
            result.append("")
        
        # Собираем результат в строку
        analysis = "\n".join(result)
        
        # Сохраняем результат в файл с датой и временем
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(reports_dir, f"cheapest_apartments_with_urls_{current_datetime}.txt")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(analysis)
        
        print(f"Результаты сохранены в файл: {output_file}")
        
        return analysis
        
    except Exception as e:
        print(f"Ошибка при поиске самых дешевых квартир: {e}")
        return None

def format_apartments_result(df):
    """Форматирует результаты для Telegram: только текст, без HTML и ="""
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
            output += f"   Спальни: {row['bedrooms']}\n"
            output += f"   Ссылка: {row['url']}\n"
            output += "\n"
        output += "\n"
    return output

class PropertyAnalyzer:
    """Класс для анализа недвижимости с использованием LangChain и SQL через OpenRouter DeepSeek"""
    def __init__(self):
        self.llm = ChatOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=os.getenv("OPENROUTER_API_KEY"),
            model="deepseek/deepseek-chat-v3-0324:free",
            temperature=0.3,
            model_kwargs={
                "extra_headers": {
                    "HTTP-Referer": "https://wealthcompas.com",
                    "X-Title": "WealthCompas Properties Analyzer"
                }
            }
        )
        self.prompt_template = PromptTemplate(
            input_variables=["apartments_data"],
            template="""
            Проанализируй следующие данные о квартирах и составь краткий отчет:
            
            {apartments_data}
            
            Пожалуйста, предоставь:
            1. Общую статистику по ценам и районам
            2. Топ-3 самых выгодных предложений
            3. Рекомендации по выбору района
            4. Анализ соотношения цена/площадь
            
            Отчет должен быть на русском языке и содержать конкретные цифры и факты.
            """
        )
        self.chain = LLMChain(llm=self.llm, prompt=self.prompt_template)
    
    def get_apartments_data(self):
        conn = None
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            cursor = conn.cursor()
            query = """
            WITH LocationData AS (
                SELECT 
                    id, 
                    title, 
                    price, 
                    rooms,
                    area,
                    location,
                    property_url,
                    CASE 
                        WHEN location LIKE '%neighbourhood%' 
                        THEN substring(location FROM '"type": "neighbourhood".*?"name": "([^"]+)"')
                        WHEN location NOT LIKE '%neighbourhood%' AND location LIKE '%"level": 2%' 
                        THEN substring(location FROM '"level": 2.*?"name": "([^"]+)"')
                        ELSE substring(location FROM '"level": 1.*?"name": "([^"]+)"')
                    END AS neighborhood
                FROM bayut_properties
                WHERE area <= 40
            ),
            RankedProperties AS (
                SELECT 
                    id, title, price, rooms, area, neighborhood, property_url,
                    ROW_NUMBER() OVER (PARTITION BY neighborhood ORDER BY price ASC) as rank
                FROM LocationData
                WHERE neighborhood IS NOT NULL
            )
            SELECT id, title, price, rooms, area, neighborhood, property_url
            FROM RankedProperties
            WHERE rank <= 3
            ORDER BY neighborhood, rank;
            """
            cursor.execute(query)
            apartments = cursor.fetchall()
            columns = ['id', 'title', 'price', 'rooms', 'area', 'neighborhood', 'property_url']
            result = [dict(zip(columns, apartment)) for apartment in apartments]
            return result
        except Exception as e:
            logger.error(f"Ошибка при получении данных о квартирах: {e}")
            return []
        finally:
            if conn:
                conn.close()
    def format_apartments_data(self, apartments):
        formatted_data = []
        current_neighborhood = None
        for apt in apartments:
            if apt['neighborhood'] != current_neighborhood:
                if current_neighborhood is not None:
                    formatted_data.append("\n")
                current_neighborhood = apt['neighborhood']
                formatted_data.append(f"Район: {current_neighborhood}")
                formatted_data.append("=" * 50)
            formatted_data.append(f"""
1. {apt['title']}
   ID: {apt['id']}
   Цена: {apt['price']:,.2f} AED
   Площадь: {apt['area']:.2f} кв.м.
   Спальни: {apt['rooms']}
   Ссылка: {apt['property_url']}
""")
        return "\n".join(formatted_data)
    def analyze_apartments(self):
        try:
            apartments = self.get_apartments_data()
            if not apartments:
                return "Не удалось получить данные о квартирах"
            formatted_data = self.format_apartments_data(apartments)
            analysis = self.chain.run(apartments_data=formatted_data)
            return analysis
        except Exception as e:
            logger.error(f"Ошибка при анализе квартир: {e}")
            return f"Произошла ошибка при анализе: {str(e)}"

def main():
    analyzer = PropertyAnalyzer()
    analysis = analyzer.analyze_apartments()
    print(analysis)
    return analysis

if __name__ == "__main__":
    main() 