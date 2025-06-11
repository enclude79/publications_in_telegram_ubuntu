import json
import os
import subprocess
from datetime import datetime

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'schedule_config.json')

with open(CONFIG_PATH, encoding='utf-8') as f:
    config = json.load(f)

now = datetime.now()
current_day = now.strftime('%A').lower()  # английский день недели
current_time = now.strftime('%H:%M')

# Маппинг русских дней недели на английские
ru_to_en = {
    'понедельник': 'monday',
    'вторник': 'tuesday',
    'среда': 'wednesday',
    'четверг': 'thursday',
    'пятница': 'friday',
    'суббота': 'saturday',
    'воскресенье': 'sunday',
}

for pub in config['publications']:
    for ru_day in pub['days']:
        en_day = ru_to_en.get(ru_day.lower())
        if en_day == current_day and pub['time'] == current_time:
            script_path = os.path.join(os.path.dirname(__file__), pub['script_name'])
            if os.path.exists(script_path):
                subprocess.Popen(['python3', script_path]) 