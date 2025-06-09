#!/bin/bash
# Скрипт для деплоя проекта на сервере

# Проверяем, что скрипт запущен от правильного пользователя
if [ "$(whoami)" != "enclude" ]; then
    echo "Скрипт должен быть запущен от имени пользователя enclude"
    exit 1
fi

# Переходим в директорию проекта
cd ~/publications_in_telegram_ubuntu

# Останавливаем планировщик, если он запущен
if [ -f scheduler.pid ]; then
    echo "Останавливаем планировщик..."
    ./stop_scheduler.sh
fi

# Обновляем код из репозитория
echo "Обновляем код из репозитория..."
git pull

# Создаем резервную копию текущего .env файла
if [ -f .env ]; then
    echo "Создаем резервную копию текущего .env файла..."
    cp .env .env.backup.$(date +%Y%m%d%H%M%S)
fi

# Проверяем существование эталонного .env файла
if [ -f /opt/wealthcompas/.env ]; then
    echo "Найден эталонный .env файл в /opt/wealthcompas/.env"
    echo "Проверяем наличие необходимых параметров для Telegram..."
    
    # Проверяем, содержит ли эталонный файл настройки для Telegram
    if grep -q "TELEGRAM_BOT_TOKEN" /opt/wealthcompas/.env && grep -q "TELEGRAM_CHANNEL_ID" /opt/wealthcompas/.env; then
        echo "Настройки Telegram найдены, используем эталонный файл..."
        cp /opt/wealthcompas/.env .env
    else
        echo "Настройки Telegram не найдены в эталонном файле"
        echo "Создаем комбинированный .env файл..."
        
        # Создаем новый .env файл с правильными настройками
        cat > .env << 'EOF'
# Параметры базы данных из эталонного файла
DB_HOST=89.169.166.179
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=EnPswFJWY1wa

# Параметры Telegram
TELEGRAM_BOT_TOKEN=7920256794:AAHDAUXkqjPEJzvoqUEV1hUn3OQks0i12jI
TELEGRAM_CHANNEL_ID=@Wealth_Compass_UAE

# Параметры приложения
APP_PORT=8501
EOF
    fi
else
    echo "Эталонный .env файл не найден в /opt/wealthcompas/.env"
    echo "Используем существующий .env файл или создаем новый..."
    
    # Проверяем, существует ли .env файл
    if [ ! -f .env ]; then
        echo "Создаем новый .env файл..."
        
        cat > .env << 'EOF'
# Параметры базы данных
DB_HOST=89.169.166.179
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=EnPswFJWY1wa

# Параметры Telegram
TELEGRAM_BOT_TOKEN=7920256794:AAHDAUXkqjPEJzvoqUEV1hUn3OQks0i12jI
TELEGRAM_CHANNEL_ID=@Wealth_Compass_UAE

# Параметры приложения
APP_PORT=8501
EOF
    fi
fi

# Устанавливаем зависимости
echo "Устанавливаем зависимости..."
pip install -r requirements.txt

# Делаем скрипты исполняемыми
echo "Делаем скрипты исполняемыми..."
chmod +x start_scheduler.sh stop_scheduler.sh

# Запускаем планировщик
echo "Запускаем планировщик..."
./start_scheduler.sh

echo "Деплой завершен успешно!" 