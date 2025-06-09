import os
import glob
import re
import sys
from datetime import datetime

def add_marketing_block_to_report(report_path):
    """Добавляет маркетинговый блок после вводного информационного блока"""
    try:
        print(f"Обработка файла: {report_path}")
        
        # Читаем содержимое файла
        try:
            with open(report_path, 'r', encoding='utf-8-sig') as f:
                content = f.read()
        except UnicodeDecodeError:
            # Пробуем другие кодировки
            try:
                with open(report_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            except UnicodeDecodeError:
                try:
                    with open(report_path, 'r', encoding='cp1251') as f:
                        content = f.read()
                except UnicodeDecodeError:
                    print(f"Не удалось прочитать файл {report_path} ни в одной кодировке")
                    return None
        
        # Маркетинговый блок
        market_block = "\n\n💸 Хотите купить недвижимость в Дубае выгодно и без посредников?\n\n"
        market_block += "📍Информация от государственного ресурса ОАЭ  [На интерактивной карте](http://89.169.166.179:8502/) — ТОП-3, 5 самых недорогих квартир в каждом районе.\n"
        market_block += "Фильтруйте по площади, сравнивайте цены и находите лучшие предложения в пару кликов!\n\n"
        market_block += "📊 Умный фильтр по квадратуре\n"
        market_block += "📉 Самые низкие цены по районам\n"
        market_block += "💼 Идеально для инвесторов и переезда\n\n"
        
        # Ищем конец вводного информационного блока
        # Например, после строки "Три самых дешевых квартиры (площадь до 40 кв.м.) в каждой локации:"
        intro_pattern = r".*самых дешевых.*квартир.*в каждой локации:.*?\n"
        intro_match = re.search(intro_pattern, content, re.DOTALL)
        
        if intro_match:
            # Вставляем блок после вводного информационного блока
            insert_position = intro_match.end()
            new_content = content[:insert_position] + market_block + content[insert_position:]
            print("Найден вводный блок, маркетинговый блок добавлен после него")
        else:
            # Если вводный блок не найден, вставляем после первой строки
            first_newline = content.find('\n')
            if first_newline != -1:
                new_content = content[:first_newline+1] + market_block + content[first_newline+1:]
                print("Вводный блок не найден, маркетинговый блок добавлен после первой строки")
            else:
                new_content = content + "\n" + market_block
                print("В файле нет переносов строк, маркетинговый блок добавлен в конец")
        
        # Создаем новое имя файла с маркетинговым блоком
        file_name = os.path.basename(report_path)
        file_dir = os.path.dirname(report_path)
        base_name, ext = os.path.splitext(file_name)
        
        # Добавляем _marketing в имя файла
        new_file_name = f"{base_name}_with_marketing{ext}"
        new_file_path = os.path.join(file_dir, new_file_name)
        
        # Записываем изменения в новый файл
        with open(new_file_path, 'w', encoding='utf-8-sig') as f:
            f.write(new_content)
            
        print(f"Маркетинговый блок успешно добавлен в файл: {new_file_path}")
        return new_file_path
    
    except Exception as e:
        print(f"Ошибка при добавлении маркетингового блока: {e}")
        return None

def get_latest_report(pattern="reports/cheapest_apartments_with_urls_*.txt"):
    """Находит самый свежий файл отчета по шаблону"""
    try:
        files = glob.glob(pattern)
        if not files:
            print(f"Не найдено файлов по шаблону: {pattern}")
            return None
            
        # Сортируем файлы по времени создания (самый новый последний)
        latest_file = max(files, key=os.path.getctime)
        print(f"Найден самый свежий файл: {latest_file}")
        return latest_file
    
    except Exception as e:
        print(f"Ошибка при поиске свежего файла: {e}")
        return None

def main():
    # Если указан путь к файлу, используем его, иначе ищем самый свежий
    if len(sys.argv) > 1:
        report_path = sys.argv[1]
    else:
        report_path = get_latest_report()
    
    if not report_path:
        print("Не удалось найти файл отчета")
        return
    
    # Добавляем маркетинговый блок
    processed_file = add_marketing_block_to_report(report_path)
    
    if processed_file:
        print(f"Обработка завершена успешно. Новый файл: {processed_file}")
    else:
        print("Не удалось обработать файл")

if __name__ == "__main__":
    main() 