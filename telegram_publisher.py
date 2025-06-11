"""
Скрипт для публикации данных о недвижимости в Telegram канал.
"""

import os
import logging
import asyncio
import ssl
import re
import html
from datetime import datetime
from dotenv import load_dotenv
import aiohttp
from find_cheapest_apartments_langchain import find_cheapest_apartments

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Создаем SSL-контекст
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Параметры подключения к базе данных из .env
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'Admin'),
    'password': os.getenv('DB_PASSWORD', 'Enclude79'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def clean_html_and_sanitize(text):
    """
    Очищает текст от HTML-тегов и специальных символов, 
    которые могут вызывать проблемы в Telegram.
    """
    # Декодируем HTML-сущности (например, &quot; -> ")
    text = html.unescape(text)
    
    # Удаляем все HTML-теги (например, <b>текст</b> -> текст)
    text = re.sub(r'<[^>]+>', '', text)
    
    # Заменяем специальные символы, которые могут вызывать проблемы
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    
    # Удаляем невидимые управляющие символы
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', text)
    
    return text

def split_text_into_chunks(text, max_length=3000):
    """
    Разбивает текст на чанки, стараясь делать разрывы на переносах строк
    и не разрывать слова.
    """
    chunks = []
    current_chunk = ""
    
    paragraphs = text.split('\n')
    
    for paragraph in paragraphs:
        # Если параграф слишком большой, разбиваем его на предложения
        if len(paragraph) > max_length:
            sentences = re.split(r'(?<=[.!?])\s+', paragraph)
            for sentence in sentences:
                # Если предложение слишком большое, разбиваем его на части
                if len(sentence) > max_length:
                    words = sentence.split(' ')
                    for word in words:
                        if len(current_chunk) + len(word) + 1 > max_length:
                            chunks.append(current_chunk.strip())
                            current_chunk = word + " "
                        else:
                            current_chunk += word + " "
                # Иначе добавляем предложение целиком
                elif len(current_chunk) + len(sentence) + 1 > max_length:
                    chunks.append(current_chunk.strip())
                    current_chunk = sentence + " "
                else:
                    current_chunk += sentence + " "
        # Если параграф помещается целиком
        elif len(current_chunk) + len(paragraph) + 1 > max_length:
            chunks.append(current_chunk.strip())
            current_chunk = paragraph + "\n"
        else:
            current_chunk += paragraph + "\n"
    
    # Добавляем последний чанк, если он не пустой
    if current_chunk.strip():
        chunks.append(current_chunk.strip())
    
    return chunks

class TelegramPublisher:
    """Класс для публикации результатов анализа в Telegram"""
    
    def __init__(self):
        """Инициализация класса"""
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHANNEL_ID')
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        # Отладочный вывод
        print(f"TELEGRAM_BOT_TOKEN: {self.bot_token}")
        print(f"TELEGRAM_CHANNEL_ID: {self.chat_id}")
    
    async def send_message(self, text):
        """Отправляет сообщение в Telegram, разбивая на части"""
        # Очищаем текст от HTML-тегов и специальных символов
        text = clean_html_and_sanitize(text)
        
        # Используем улучшенный алгоритм разбиения текста
        chunks = split_text_into_chunks(text, max_length=3000)
        
        try:
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
                # Отправляем каждый чанк
                for i, chunk in enumerate(chunks):
                    # Для первого чанка добавляем заголовок
                    if i == 0:
                        chunk = f"📊 Анализ квартир до 40 кв.м. - {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n" + chunk
                    
                    # Для последнего чанка добавляем хэштеги
                    if i == len(chunks) - 1:
                        chunk = chunk + "\n\n#недвижимость #анализ #инвестиции"
                    
                    # Проверка длины каждого чанка перед отправкой
                    if len(chunk) > 4000:
                        logger.warning(f"Чанк {i+1} слишком длинный ({len(chunk)} символов), обрезаем до 4000 символов")
                        chunk = chunk[:3997] + "..."
                    
                    try:
                        async with session.post(
                            self.api_url,
                            json={
                                "chat_id": self.chat_id,
                                "text": chunk
                            }
                        ) as response:
                            if response.status == 200:
                                logger.info(f"Часть {i+1}/{len(chunks)} успешно отправлена в Telegram ({len(chunk)} символов)")
                            else:
                                error_text = await response.text()
                                logger.error(f"Ошибка при отправке части {i+1}/{len(chunks)}: {error_text}")
                                
                                # Сохраняем проблемный чанк в файл для диагностики
                                error_file = f"error_chunk_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i}.txt"
                                with open(error_file, 'w', encoding='utf-8') as f:
                                    f.write(chunk)
                                logger.info(f"Проблемный чанк сохранен в файл: {error_file}")
                                
                                # Пытаемся отправить сокращенную версию
                                if len(chunk) > 1000:
                                    shortened = chunk[:950] + "... (сообщение сокращено)"
                                    logger.info("Пытаемся отправить сокращенную версию чанка")
                                    async with session.post(
                                        self.api_url,
                                        json={
                                            "chat_id": self.chat_id,
                                            "text": shortened
                                        }
                                    ) as retry_response:
                                        if retry_response.status == 200:
                                            logger.info("Сокращенная версия чанка успешно отправлена")
                                        else:
                                            logger.error(f"Не удалось отправить даже сокращенную версию: {await retry_response.text()}")
                                            # Продолжаем с следующим чанком, не останавливаемся
                        
                        # Небольшая пауза между отправками
                        await asyncio.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"Ошибка при отправке части {i+1}/{len(chunks)}: {e}")
                        # Продолжаем с следующим чанком, не останавливаемся
                
                logger.info(f"Успешно отправлено {len(chunks)} частей сообщения")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения в Telegram: {e}")
            return False

    async def publish_analysis(self):
        """Публикует результаты анализа в Telegram"""
        try:
            # Получаем анализ
            logger.info("Получение анализа квартир...")
            analysis = find_cheapest_apartments()
            
            if not analysis:
                logger.error("Не удалось получить анализ")
                return False
            
            # Логируем первые 100 символов для диагностики
            logger.info(f"Первые 100 символов анализа: {analysis[:100]}")
            logger.info(f"Общая длина анализа: {len(analysis)} символов")
            
            # Отправляем сообщение
            logger.info("Отправка анализа в Telegram...")
            success = await self.send_message(analysis)
            
            if success:
                logger.info("Анализ успешно опубликован в Telegram")
            else:
                logger.error("Ошибка при публикации анализа в Telegram")
                
            return success
            
        except Exception as e:
            logger.error(f"Ошибка при публикации анализа: {e}")
            return False

async def main():
    """Основная функция"""
    logger.info("Запуск скрипта публикации анализа в Telegram")
    publisher = TelegramPublisher()
    success = await publisher.publish_analysis()
    if success:
        print("Анализ успешно опубликован в Telegram")
    else:
        print("Ошибка при публикации анализа в Telegram")

if __name__ == "__main__":
    asyncio.run(main()) 