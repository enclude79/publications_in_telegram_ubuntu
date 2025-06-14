import os
import pandas as pd
import logging
from datetime import datetime
import psycopg2
from load_env import load_environment_variables
from dotenv import load_dotenv

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
logger.info(f"TELEGRAM_BOT_TOKEN: {'Найден' if os.getenv('TELEGRAM_BOT_TOKEN') else 'Не найден'}")
logger.info(f"TELEGRAM_CHAT_ID: {'Найден' if os.getenv('TELEGRAM_CHAT_ID') else 'Не найден'}")

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

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

if __name__ == "__main__":
    # Запускаем анализ самых дешевых квартир
    analysis = find_cheapest_apartments()
    if analysis:
        print(analysis)
    else:
        print("Не удалось выполнить анализ") 