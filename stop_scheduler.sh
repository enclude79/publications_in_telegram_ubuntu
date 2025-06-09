#!/bin/bash
# Скрипт для остановки планировщика публикаций в Telegram на Ubuntu

# Определяем директорию скрипта
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Имя файла PID
PID_FILE="scheduler.pid"

# Проверяем, существует ли PID файл
if [ ! -f "$PID_FILE" ]; then
    echo "Планировщик не запущен (не найден файл $PID_FILE)"
    exit 0
fi

# Получаем PID процесса
PID=$(cat "$PID_FILE")

# Проверяем, существует ли процесс с таким PID
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "Процесс с PID $PID не найден. Возможно, планировщик уже остановлен."
    rm "$PID_FILE"
    exit 0
fi

# Отправляем сигнал SIGTERM для плавного завершения
echo "Останавливаем планировщик публикаций (PID: $PID)..."
kill -15 "$PID"

# Ждем некоторое время для завершения процесса
TIMEOUT=10
for i in $(seq 1 $TIMEOUT); do
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "Планировщик успешно остановлен."
        rm "$PID_FILE"
        exit 0
    fi
    sleep 1
done

# Если процесс все еще работает, отправляем SIGKILL
echo "Планировщик не завершился корректно в течение $TIMEOUT секунд."
echo "Принудительное завершение процесса..."
kill -9 "$PID"

# Финальная проверка
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "Планировщик принудительно остановлен."
    rm "$PID_FILE"
    exit 0
else
    echo "Не удалось остановить планировщик (PID: $PID). Проверьте процесс вручную."
    exit 1
fi 