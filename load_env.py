import os
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_environment_variables():
    """Загружает переменные окружения из файла .env"""
    env_file = '.env'
    
    if os.path.exists(env_file):
        logger.info(f"Загрузка переменных окружения из файла {env_file}")
        with open(env_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            key, value = line.split('=', 1)
            os.environ[key.strip()] = value.strip()
        
        return True
        
    logger.warning("Файл .env не найден")
    return False

def set_telegram_env_vars(bot_token=None, channel_id=None):
    """Устанавливает переменные окружения для Telegram"""
    if bot_token:
        os.environ['TELEGRAM_BOT_TOKEN'] = bot_token
        logger.info("Установлен TELEGRAM_BOT_TOKEN")
    
    if channel_id:
        os.environ['TELEGRAM_CHANNEL_ID'] = channel_id
        logger.info("Установлен TELEGRAM_CHANNEL_ID")
        
if __name__ == "__main__":
    load_environment_variables() 