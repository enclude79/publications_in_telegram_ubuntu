import os
import sys
import json
import logging
import subprocess
import time
from datetime import datetime
import schedule

# Настройка логирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, "scheduler.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

SCHEDULE_CONFIG = "schedule_config.json"

# Загрузка расписания

def load_schedule_config():
    if not os.path.exists(SCHEDULE_CONFIG):
        logger.error(f"Файл {SCHEDULE_CONFIG} не найден!")
        return []
    with open(SCHEDULE_CONFIG, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("publications", [])

def run_script(script_name, sql_config=None):
    logger.info(f"Запуск скрипта: {script_name}")
    try:
        env_vars = os.environ.copy()
        if sql_config:
            logger.info(f"Использование специфичных SQL параметров для {script_name}: {sql_config}")
            env_vars["DB_HOST"] = sql_config.get("DB_HOST", env_vars.get("DB_HOST", ""))
            env_vars["DB_PORT"] = str(sql_config.get("DB_PORT", env_vars.get("DB_PORT", "")))
            env_vars["DB_NAME"] = sql_config.get("DB_NAME", env_vars.get("DB_NAME", ""))
            env_vars["DB_USER"] = sql_config.get("DB_USER", env_vars.get("DB_USER", ""))
            env_vars["DB_PASSWORD"] = sql_config.get("DB_PASSWORD", env_vars.get("DB_PASSWORD", ""))

        result = subprocess.run([sys.executable, script_name],
                                capture_output=True, text=True, timeout=3600, env=env_vars, encoding='utf-8')

        logger.info(f"Скрипт {script_name} завершён с кодом {result.returncode}")
        if result.stdout:
            logger.info(f"STDOUT {script_name}:\n{result.stdout}")
        if result.stderr:
            if result.returncode != 0:
                logger.error(f"STDERR {script_name}:\n{result.stderr}")
            else:
                logger.warning(f"STDERR {script_name}:\n{result.stderr}")
    except subprocess.TimeoutExpired:
        logger.error(f"Скрипт {script_name} превысил таймаут выполнения (3600 секунд).")
    except Exception as e:
        logger.error(f"Ошибка при запуске {script_name}: {e}")

def schedule_jobs():
    publications = load_schedule_config()
    for pub in publications:
        script = pub["script_name"]
        days = pub.get("days", [])
        time_str = pub["time"]
        sql_config = pub.get("sql_config")

        job_func = (lambda s, sc: lambda: run_script(s, sc))(script, sql_config)
        
        scheduled_info = []

        for day_entry in days:
            day_normalized = str(day_entry).lower()

            if day_normalized == "понедельник":
                schedule.every().monday.at(time_str).do(job_func)
                scheduled_info.append(f"Пн в {time_str}")
            elif day_normalized == "вторник":
                schedule.every().tuesday.at(time_str).do(job_func)
                scheduled_info.append(f"Вт в {time_str}")
            elif day_normalized == "среда":
                schedule.every().wednesday.at(time_str).do(job_func)
                scheduled_info.append(f"Ср в {time_str}")
            elif day_normalized == "четверг":
                schedule.every().thursday.at(time_str).do(job_func)
                scheduled_info.append(f"Чт в {time_str}")
            elif day_normalized == "пятница":
                schedule.every().friday.at(time_str).do(job_func)
                scheduled_info.append(f"Пт в {time_str}")
            elif day_normalized == "суббота":
                schedule.every().saturday.at(time_str).do(job_func)
                scheduled_info.append(f"Сб в {time_str}")
            elif day_normalized == "воскресенье":
                schedule.every().sunday.at(time_str).do(job_func)
                scheduled_info.append(f"Вс в {time_str}")
            elif day_normalized == "ежедневно":
                 schedule.every().day.at(time_str).do(job_func)
                 scheduled_info.append(f"Ежедневно в {time_str}")
            else:
                logger.warning(f"Неизвестный день недели или формат: '{day_entry}' для скрипта {script}")
        
        if scheduled_info:
            log_message = f"Добавлено расписание для '{script}': {', '.join(scheduled_info)}."
            if sql_config:
                log_message += f" Используется SQL конфиг: {json.dumps(sql_config, ensure_ascii=False)}"
            else:
                log_message += " Используется SQL конфигурация по умолчанию (из .env или системных переменных)."
            logger.info(log_message)
        elif not days :
             logger.warning(f"Для скрипта '{script}' не указаны дни для запуска.")

def main():
    logger.info("Запуск планировщика публикаций...")
    schedule_jobs()
    logger.info("Планировщик запущен. Ожидание задач...")
    try:
        while True:
            schedule.run_pending()
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Планировщик остановлен пользователем.")
    except Exception as e:
        logger.critical(f"Критическая ошибка в основном цикле планировщика: {e}", exc_info=True)
    finally:
        logger.info("Завершение работы планировщика.")

if __name__ == "__main__":
    main() 