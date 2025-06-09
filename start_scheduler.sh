#!/bin/bash
# Скрипт для запуска планировщика публикаций в Telegram на Ubuntu

# Определяем директорию скрипта
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Имя файла PID
PID_FILE="scheduler.pid"

# Проверяем, запущен ли уже планировщик
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Планировщик уже запущен с PID: $PID"
        exit 1
    else
        echo "Найден устаревший PID файл. Удаляем..."
        rm "$PID_FILE"
    fi
fi

# Создаем директорию для отчетов, если она не существует
mkdir -p reports

# Проверяем наличие файла конфигурации
if [ ! -f "schedule_config.json" ]; then
    echo "Ошибка: Файл конфигурации schedule_config.json не найден!"
    exit 1
fi

# Проверяем наличие .env файла
if [ ! -f ".env" ]; then
    echo "Предупреждение: .env файл не найден. Убедитесь, что переменные окружения установлены."
fi

# Запускаем планировщик в фоновом режиме
echo "Запуск планировщика публикаций..."
nohup python3 publication_scheduler.py > scheduler.out 2>&1 &

# Сохраняем PID процесса
echo $! > "$PID_FILE"
echo "Планировщик запущен с PID: $(cat "$PID_FILE")"
echo "Логи записываются в scheduler.log"
echo "Вывод записывается в scheduler.out" 