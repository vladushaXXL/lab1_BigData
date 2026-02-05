import requests
import csv
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET

# Константы
CURRENCY = "KRW"  # Корейских вон
START_DATE = datetime(2001, 1, 1)
END_DATE = datetime(2026, 1, 26)
API_URL = "http://www.cbr.ru/scripts/XML_daily.asp"
DATE_FORMAT_API = "%d/%m/%Y"
DATE_FORMAT_OUTPUT = "%Y-%m-%d"
MAX_WORKERS = 3
REQUEST_DELAY = 0.5

def generate_date_range(start_date, end_date):
    """Генерирует список дат в формате для API"""
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime(DATE_FORMAT_API))
        current_date += timedelta(days=1)
    return dates


def parse_currency_data(xml_content, date_str):
    """Парсит XML и извлекает данные по валюте"""
    try:
        root = ET.fromstring(xml_content)
        for valute in root.findall('Valute'):
            char_code = valute.find('CharCode')
            if char_code is not None and char_code.text == CURRENCY:
                # Для вон значение хранится за 1000 единиц
                nominal = int(valute.find('Nominal').text)
                value_text = valute.find('Value').text.replace(',', '.')
                value = float(value_text)
                date_obj = datetime.strptime(date_str, DATE_FORMAT_API)
                
                # Вычисляем стоимость одной вон
                rate = value / nominal
                
                return {
                    'date': date_obj.strftime(DATE_FORMAT_OUTPUT),
                    'rate': round(rate, 6)  # Увеличиваем точность для вон
                }
    except (ET.ParseError, ValueError, AttributeError, TypeError) as e:
        print(f"Ошибка парсинга для даты {date_str}: {e}")
    return None


def fetch_exchange_rate(date_str):
    """Загружает курс валюты для указанной даты"""
    try:
        time.sleep(REQUEST_DELAY)  # Задержка между запросами
        response = requests.get(
            API_URL,
            params={'date_req': date_str},
            timeout=30,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/xml',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
            }
        )
        
        if response.status_code == 200:
            return parse_currency_data(response.content, date_str)
        else:
            print(f"Ошибка HTTP {response.status_code} для даты {date_str}")
    except requests.exceptions.RequestException as e:
        print(f"Ошибка запроса для даты {date_str}: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка для даты {date_str}: {e}")
    
    return None


def download_data_concurrently(dates):
    """Загружает данные параллельно"""
    data = []
    total_dates = len(dates)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_exchange_rate, date): date for date in dates}
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            result = future.result()
            if result:
                data.append(result)
            
            # Прогресс каждые 100 запросов
            if completed % 100 == 0:
                print(f"Обработано: {completed}/{total_dates} дат")
    
    return data

def save_to_csv(data, filename='krw_exchange_rates.csv'):
    """Сохраняет данные в CSV файл"""
    if not data:
        print("Нет данных для сохранения")
        return
    
    data.sort(key=lambda x: x['date'])
    
    with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'rate'])
        for item in data:
            writer.writerow([item['date'], item['rate']])
    
    print(f"Данные сохранены в файл: {filename}")
    print(f"Период: с {data[0]['date']} по {data[-1]['date']}")


def print_statistics(data, execution_time):
    """Выводит статистику выполнения"""
    if data:
        print(f"\n=== СТАТИСТИКА ===")
        print(f"Загружено записей: {len(data)}")
        print(f"Общее время выполнения: {execution_time:.2f} секунд")
        print(f"Среднее время на запрос: {execution_time/len(data):.2f} секунд")
        
        # Дополнительная статистика по курсам
        rates = [item['rate'] for item in data]
        print(f"Минимальный курс: {min(rates):.6f}")
        print(f"Максимальный курс: {max(rates):.6f}")
        print(f"Средний курс: {sum(rates)/len(rates):.6f}")
    else:
        print("Нет данных для отображения статистики")


def main():
    """Основная функция"""
    print("Начинаю загрузку курсов корейских вон (KRW)...")
    
    # Генерация дат
    dates = generate_date_range(START_DATE, END_DATE)
    print(f"Всего дат для обработки: {len(dates)}")
    print(f"Период: с {START_DATE.strftime(DATE_FORMAT_OUTPUT)} по {END_DATE.strftime(DATE_FORMAT_OUTPUT)}")
    
    # Засекаем время
    start_time = time.time()
    
    # Загрузка данных
    data = download_data_concurrently(dates)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Сохранение данных
    save_to_csv(data)
    
    # Вывод статистики
    print_statistics(data, execution_time)


if __name__ == "__main__":
    main()