# v.1.0_20240902
import logging
import os
import time
from datetime import datetime, timedelta

import requests
import schedule
from dateutil.parser import parse
from dateutil.tz import tzlocal
from dotenv import dotenv_values
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Вывод в консоль
    ]
)

logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

logger.info("Starting application...")

config = dotenv_values(".env")

if 'WB_API_TOKEN_FILE' in os.environ:
    with open(os.environ['WB_API_TOKEN_FILE'], 'r') as f:
        WB_API_TOKEN = f.read().strip()
else:
    logger.info("WB_API_TOKEN_FILE не задан в переменных окружения")

MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')
service_account_file = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')
spreadsheet_id = os.getenv('GOOGLE_SPREADSHEET_ID')
logger.info(f"Connecting to MongoDB at {MONGO_URI}")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def get_ad():
    url = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'
    headers = {'Authorization': WB_API_TOKEN}
    response = requests.get(url, headers=headers)
    data = response.json()
    return data


def extract_adId(data, target_statuses=[7, 9, 11], days_threshold=60):
    adId = set()
    current_date = datetime.now(tzlocal())
    threshold_date = current_date - timedelta(days=days_threshold)

    for advert in data.get('adverts', []):
        if advert.get('status') in target_statuses:
            for ad in advert.get('advert_list', []):
                change_time = parse(ad['changeTime'])
                if change_time > threshold_date:
                    adId.add(ad['advertId'])

    return list(adId)


def filter_adids_with_errors(adIds, db):
    filtered_adIds = []

    for ad_id in adIds:
        # Get the last 5 records for this adId, sorted by date in descending order
        last_records = list(db['fullstat'].find(
            {'advertId': ad_id},
            {'error': 1, 'date': 1}
        ).sort('date', -1).limit(5))

        # Check if we have 5 records and if all of them have error code 400
        has_consistent_errors = (
                len(last_records) == 5 and
                all(record.get('error') == '400' for record in last_records)
        )

        # If the adId doesn't have consistent errors, keep it
        if not has_consistent_errors:
            filtered_adIds.append(ad_id)
    logger.info(f"Filtered {len(adIds) - len(filtered_adIds)} adIds with consistent 400 errors")
    return filtered_adIds


def get_date_range(days_back=10):
    # Получаем вчерашнюю дату (конечная дата диапазона)
    end_date = datetime.now().date() - timedelta(days=1)

    # Вычисляем начальную дату диапазона
    start_date = end_date - timedelta(days=days_back)

    # Создаем и возвращаем список дат от start_date до end_date
    return [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]


def collect_fullstat_data(date, ad):
    # URL API для получения полной статистики
    url = 'https://advert-api.wildberries.ru/adv/v2/fullstats'

    # Заголовки запроса с токеном авторизации
    headers = {'Authorization': WB_API_TOKEN}

    # Получаем коллекцию 'fullstat' из базы данных
    collection = db['fullstat']

    # Преобразуем дату в строковый формат
    date_str = date.strftime("%Y-%m-%d")

    # Получаем предыдущую дату
    prev_date = (date - timedelta(days=1)).strftime("%Y-%m-%d")

    # Проверяем, есть ли уже данные в базе
    existing_data = collection.find_one({"advertId": ad, "date": date_str})
    if existing_data:
        logger.info(f"Data for ad {ad} on {date_str} already exists in MongoDB. Skipping.")
        return

    # Формируем параметры запроса
    params = [{"id": ad, "dates": [date_str, prev_date]}]

    logger.info(f"Sending request for ad {ad} on {date_str}:")

    try:
        # Задержка перед запросом
        time.sleep(70)

        # Отправляем POST-запрос к API
        res = requests.post(url, headers=headers, json=params)
        logger.info(f"Response status code: {res.status_code}")

        if res.status_code == 200:
            json_output = res.json()
            if not json_output:
                logger.info(f"Received empty response for ad {ad}")
                # Обновляем документ в MongoDB, устанавливая флаг ошибки
                collection.update_one(
                    {"advertId": ad, "date": date_str},
                    {"$set": {"error": "empty", "data": None}},
                    upsert=True
                )
            else:
                # Добавляем дату к каждому элементу ответа
                for item in json_output:
                    item['date'] = date_str
                # Вставляем данные в MongoDB
                collection.insert_many(json_output)
                logger.info(f"Data for ad {ad} inserted into MongoDB")
        elif res.status_code == 400:
            logger.info(f"Received 400 error for ad {ad}")
            # Обновляем документ в MongoDB, устанавливая флаг ошибки
            collection.update_one(
                {"advertId": ad, "date": date_str},
                {"$set": {"error": "400", "data": None}},
                upsert=True
            )
        else:
            logging.error(f"Error fetching data for ad {ad}: Status code {res.status_code}")
            logging.error(f"Response content: {res.text[:1000]}...")

    except Exception as e:
        logging.error(f"Unexpected error processing data for ad {ad}: {e}")


def get_missing_fullstat_dates(adId, date_range):
    # Инициализируем пустой список для хранения отсутствующих дат
    missing_dates = []

    # Перебираем все даты в заданном диапазоне
    for date in date_range:
        # Преобразуем дату в строковый формат
        date_str = date.strftime("%Y-%m-%d")

        # Ищем данные в коллекции 'fullstat' для данного объявления и даты
        existing_data = db['fullstat'].find_one({"advertId": adId, "date": date_str})

        # Если данных нет или они помечены как ошибочные, добавляем дату в список отсутствующих
        if not existing_data or (
                existing_data.get('error') not in ['empty', '400'] and existing_data.get('data') is not None):
            missing_dates.append(date)

    # Возвращаем список отсутствующих дат
    return missing_dates


def collect_history_data(date, nmIds):
    # URL API для получения детальной информации по товарам
    url = 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail'

    # Заголовки запроса с токеном авторизации
    headers = {'Authorization': WB_API_TOKEN}

    # Получаем коллекцию 'history' из базы данных
    collection = db['history']

    # Перебираем все ID товаров
    for nmId in nmIds:
        # Проверяем, есть ли уже данные в базе для этого товара и даты
        existing_data = collection.find_one({'nmId': nmId, 'date': date})
        if existing_data:
            continue

        # Задержка перед запросом к API
        time.sleep(25)

        # Формируем параметры запроса
        print(nmId)
        params = {
            "nmIDs": [nmId],
            "period": {
                "begin": f"{date} 00:00:00",
                "end": f"{date} 23:59:59"
            },
            "page": 1
        }

        try:
            # Отправляем POST-запрос к API
            res = requests.post(url, headers=headers, json=params)
            if res.status_code == 200:
                json_output = res.json()
                if json_output and 'data' in json_output and 'cards' in json_output['data']:
                    # Извлекаем информацию о товаре из ответа
                    card = json_output['data']['cards'][0] if json_output['data']['cards'] else {}
                    vendor_code = card.get('vendorCode', str(nmId))

                    # Формируем документ для вставки в MongoDB
                    mongo_document = {
                        'date': date,
                        'nmId': nmId,
                        'vendorCode': vendor_code,
                        'api_response': json_output
                    }

                    # Вставляем документ в коллекцию
                    collection.insert_one(mongo_document)
                else:
                    logging.warning(f"Received invalid response for nmId {nmId}")
            else:
                logging.error(f"Error fetching data for nmId {nmId}: Status code {res.status_code}")
        except Exception as e:
            logging.error(f"Unexpected error processing data for nmId {nmId}: {e}")


def process_data(fullstat_data, history_data):
    processed_data = {}
    fullstat_by_nmid = {}

    if not fullstat_data:
        logging.warning("No fullstat data to process")
    if not history_data:
        logging.warning("No history data to process")

    logger.info(f"Processing {len(fullstat_data)} fullstat records")

    # Обработка данных fullstat (оставлена без изменений)
    for fullstat in fullstat_data:
        try:
            ad_id = fullstat.get('advertId')
            date = fullstat.get('date')

            if fullstat.get('error'):
                key = (str(ad_id), date)
                fullstat_by_nmid[key] = {
                    'advertId': ad_id,
                    'date': date,
                    'views': 0,
                    'clicks': 0,
                    'cpc': 0,
                    'ctr': '0.00%',
                    'sum': 0,
                    'error': fullstat.get('error')
                }
            else:
                for day in fullstat.get('days', []):
                    date = day.get('date', '')[:10]
                    for app in day.get('apps', []):
                        for nm in app.get('nm', []):
                            nm_id = str(nm.get('nmId', ''))
                            if nm_id:
                                key = (nm_id, date)
                                fullstat_by_nmid[key] = {
                                    'advertId': ad_id,
                                    'date': date,
                                    'views': day.get('views', 0),
                                    'clicks': day.get('clicks', 0),
                                    'cpc': day.get('cpc', 0),
                                    'ctr': f"{day.get('ctr', 0):.2f}%",
                                    'sum': day.get('sum', 0)
                                }
        except Exception as e:
            logging.error(f"Error processing fullstat data: {e}")

    #    logger.info(f"Processing {len(history_data)} history records")

    # Обработка данных history (модифицированная часть)
    for history in history_data:
        try:
            nm_id = str(history.get('nmId', ''))
            date = history.get('date', '')[:10]
            key = (nm_id, date)

            if nm_id == '241594190' and date == '2024-08-17':
                print('1111')

            # Базовая запись с данными из history
            api_response = history.get('api_response', {})
            cards = api_response.get('data', {}).get('cards', [])
            stats = cards[0].get('statistics', {}).get('selectedPeriod', {}) if cards else {}

            base_record = {
                'advertId': '',
                'date': date,
                'views': '',
                'clicks': '',
                'cpc': '',
                'ctr': '',
                'sum': '',
                'openCardCount': stats.get('openCardCount', 0),
                'addToCartCount': stats.get('addToCartCount', 0),
                'addToCartConversion': f"{stats.get('conversions', {}).get('addToCartPercent', 0):.2f}%",
                'ordersCount': stats.get('ordersCount', 0),
                'ordersSumRub': stats.get('ordersSumRub', 0),
                'vendorCode': cards[0].get('vendorCode', '') if cards else ''
            }

            # Если есть данные в fullstat_by_nmid, обновляем запись
            if key in fullstat_by_nmid:
                base_record.update(fullstat_by_nmid[key])

            processed_data[key] = base_record

        except Exception as e:
            logging.error(f"Error processing history data: {e}")

    return processed_data


def calculate_additional_metrics(data):
    # Перебираем все элементы в данных
    for key, item in data.items():
        # Извлекаем необходимые значения, используя 0 как значение по умолчанию
        views = item.get('views', 0)
        clicks = item.get('clicks', 0)
        open_card_count = item.get('openCardCount', 0)
        add_to_cart_count = item.get('addToCartCount', 0)
        orders_count = item.get('ordersCount', 0)
        orders_sum_rub = item.get('ordersSumRub', 0)

        # Рассчитываем долю рекламных кликов (DRK)
        item['drk'] = f"{(clicks / views if views and clicks and views != 0 else 0):.2f}%"

        # Рассчитываем органический трафик
        item['organic_traffic'] = max(0, open_card_count - clicks if open_card_count and clicks else 0)

        # Рассчитываем долю органического трафика
        item[
            'Share_organic_traffic'] = f"{(((open_card_count - clicks) / open_card_count) if open_card_count and clicks and open_card_count != 0 else 0):.2f}%"

        # Рассчитываем конверсию из корзины в заказ
        item[
            'cartToOrderConversion'] = f"{((orders_count / add_to_cart_count) * 100 if add_to_cart_count and orders_count and add_to_cart_count != 0 else 0):.2f}%"

        # Рассчитываем конверсию из трафика в заказ
        item[
            'CR_traffic_order'] = f"{((orders_count / open_card_count) * 100 if orders_count and open_card_count and open_card_count != 0 else 0):.2f}%"

        # Рассчитываем долю рекламных расходов (DRR)
        item[
            'drr'] = f"{(item.get('sum', 0) / orders_sum_rub if item.get('sum', 0) and orders_sum_rub and orders_sum_rub != 0 else 0):.2f}%"

    # Возвращаем обновленные данные
    return data


def create_sheet_data(final_data):
    # Инициализируем словарь для хранения данных листов
    sheet_data = {}

    # Перебираем все элементы в обработанных данных
    for (nm_id, date), item in final_data.items():
        # Получаем vendor_code из item, если он там есть

        if nm_id == '241594190' and date == '2024-08-16':
            print(nm_id)

        vendor_code = item.get('vendorCode', nm_id)
        sheet_name = f"{nm_id}_{vendor_code}"

        # Если лист еще не создан, инициализируем его с заголовками
        if sheet_name not in sheet_data:
            sheet_data[sheet_name] = [
                ["Кампания", "Дата", "Показы", "Клики", "Доля рекламных кликов", "СРС", "CTR рекламы средний",
                 "Затраты",
                 "Переходы органика", "Доля органических переходов",
                 "Переходы по воронке всего", "Корзины", "CR корзина", "Заказы", "CR заказ",
                 "CR переход-заказ", "Сумма заказов", "ДРР от всех заказов"]
            ]
        # Формируем строку данных
        row = [
            item.get('advertId', ''),  # A
            date,  # B
            item.get('views', ''),  # C
            item.get('clicks', ''),  # D
            item.get('drk', ''),  # E
            item.get('cpc', ''),  # F
            item.get('ctr', ''),  # G
            item.get('sum', ''),  # H
            item.get('organic_traffic', ''),  # I
            item.get('Share_organic_traffic', ''),  # J
            item.get('openCardCount', ''),  # K
            item.get('addToCartCount', ''),  # L
            item.get('addToCartConversion', ''),  # M
            item.get('ordersCount', ''),  # N
            item.get('cartToOrderConversion', ''),  # O
            item.get('CR_traffic_order', ''),  # P
            item.get('ordersSumRub', ''),  # R
            item.get('drr', '')  # S
        ]
        # Добавляем строку данных в соответствующий лист
        sheet_data[sheet_name].append(row)

        for sheet_name in sheet_data:
            # Пропускаем заголовок (первую строку)
            header = sheet_data[sheet_name][0]
            data_rows = sheet_data[sheet_name][1:]

            # Сортировка по второму столбцу (индекс 1), который содержит дату
            sorted_rows = sorted(data_rows, key=lambda x: x[1])

            # Собираем отсортированные данные обратно
            sheet_data[sheet_name] = [header] + sorted_rows

    # Возвращаем подготовленные данные для листов
    return sheet_data


def write_to_google_sheets(sheet_data):
    # Область доступа
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    try:
        # Создаем учетные данные из файла сервисного аккаунта
        creds = service_account.Credentials.from_service_account_file(
            service_account_file, scopes=SCOPES)
        # Создаем сервис для работы с Google Sheets API
        service = build('sheets', 'v4', credentials=creds)
        # Получаем ID таблицы из переменных окружения
        #        spreadsheet_id = os.getenv('GOOGLE_SPREADSHEET_ID')

        # Получаем метаданные о листах в таблице
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        # Формируем список ID листов для удаления (все, кроме "Home")
        sheet_ids_to_delete = [sheet['properties']['sheetId'] for sheet in sheets
                               if sheet['properties']['title'] != 'Home']

        # Удаляем лишние листы
        if sheet_ids_to_delete:
            requests = [{'deleteSheet': {'sheetId': sheet_id}} for sheet_id in sheet_ids_to_delete]
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': requests}).execute()

        # Обрабатываем данные для каждого листа
        for sheet_name, data in sheet_data.items():
            # Создаем новый лист
            request = {
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            }
            response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                                          body={'requests': [request]}).execute()
            sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']

            # Записываем данные в лист
            range_name = f'{sheet_name}!A1'
            body = {'values': data}
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=range_name,
                valueInputOption='RAW', body=body).execute()

            # Форматируем лист

            num_rows = len(data)
            num_cols = len(data[0]) if data else 0

            format_requests = [
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': num_rows,
                            'startColumnIndex': 0,
                            'endColumnIndex': num_cols
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'horizontalAlignment': 'CENTER',
                                'verticalAlignment': 'MIDDLE'
                            }
                        },
                        'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment)'
                    }
                },
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,  # Только первая строка
                            'startColumnIndex': 0,
                            'endColumnIndex': num_cols
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'wrapStrategy': 'WRAP',
                                'textRotation': {
                                    'vertical': False
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(wrapStrategy,textRotation)'
                    }
                },
                {
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': sheet_id,
                            'gridProperties': {
                                'frozenRowCount': 1  # Закрепляем первую строку
                            }
                        },
                        'fields': 'gridProperties.frozenRowCount'
                    }
                },
                {
                    'autoResizeDimensions': {
                        'dimensions': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': 0,
                            'endIndex': num_cols
                        }
                    }
                }
            ]

            format_request = {'requests': format_requests}
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=format_request).execute()


    except HttpError as err:
        logging.error(f"An error occurred: {err}")
    except Exception as e:
        logging.error(f"Error writing to Google Sheets: {e}", exc_info=True)


def get_nmIds_from_fullstat(date):
    nmIds = set()
    date_str = date.strftime("%Y-%m-%d")
    fullstat_data = db['fullstat'].find({'date': date_str})

    for doc in fullstat_data:
        #        logging.debug(f"Processing document: {doc}")
        if 'days' in doc:
            for day in doc['days']:
                if 'apps' in day:
                    for app in day['apps']:
                        if 'nm' in app:
                            for nm in app['nm']:
                                nmId = nm.get('nmId')
                                if nmId:
                                    nmIds.add(nmId)
    #                                    logging.debug(f"Added nmId: {nmId}")

    # Добавляем отладочную информацию
    #        logging.debug(f"Document structure: {json.dumps(doc, default=str)}")
    #    logger.info(f"Found {len(nmIds)} unique nmIds for date {date}")
    #    logger.info(f"nmIds: {nmIds}")
    return list(nmIds)


def get_unique_nmIds_from_fullstat():
    # Получаем все уникальные nmIds из коллекции fullstat
    nmIds = set()
    fullstat_data = db['fullstat'].find({})

    for doc in fullstat_data:
        if 'days' in doc:
            for day in doc['days']:
                if 'apps' in day:
                    for app in day['apps']:
                        if 'nm' in app:
                            for nm in app['nm']:
                                nmId = nm.get('nmId')
                                if nmId:
                                    nmIds.add(nmId)

    logger.info(f"Найдено {len(nmIds)} уникальных nmIds")
    return list(nmIds)


def startprj():
    logger.info("!!!!!Start!!!!")
    print('START!!!!!!!!!!!!')
    # Получаем количество дней для анализа из переменных окружения
    days_back = int(os.getenv('DAYS_BACK', 10))
    # Получаем диапазон дат для анализа
    date_range = get_date_range(days_back)

    # Получаем список кампаний со статусом 7, 9, 11 за 60 дней
    try:
        data = get_ad()
        adId = extract_adId(data)
        adId = filter_adids_with_errors(adId, db)
        #        logger.info(f"Extracted and filtered advertIds: {json.dumps(adId, indent=2)}")
        #logger.info(
        #    f"Extracted unique advertIds for status 7 or 9, 11, changed within last 30 days: {json.dumps(adId, indent=2)}")
    except Exception as e:
        logging.error(f"Error occurred: {e}")

    # Обрабатываем каждую рекламную кампанию
    for ad in adId:
        logger.info(f"Processing adId: {ad}")
        # Получаем список дат, для которых отсутствуют данные fullstat
        missing_fullstat_dates = get_missing_fullstat_dates(ad, date_range)

        # Собираем данные fullstat для отсутствующих дат
        for date in missing_fullstat_dates:
            collect_fullstat_data(date, ad)

    # Получаем список всех уникальных nmIds из fullstat
    unique_nmIds = get_unique_nmIds_from_fullstat()

    # Обрабатываем каждую дату в диапазоне
    for date in date_range:
        logger.info(f"Сбор данных History для {len(unique_nmIds)} nmIds на дату {date}")
        # Собираем исторические данные для всех уникальных nmIds
        collect_history_data(date.strftime("%Y-%m-%d"), unique_nmIds)

    # Получаем все данные fullstat и history из базы данных
    all_fullstat_data = list(db['fullstat'].find())
    all_history_data = list(db['history'].find())

    # Обрабатываем полученные данные
    processed_data = process_data(all_fullstat_data, all_history_data)
    # Рассчитываем дополнительные метрики
    final_data = calculate_additional_metrics(processed_data)

    # Подготавливаем данные для записи в Google Sheets
    sheet_data = create_sheet_data(final_data)
    # Записываем данные в Google Sheets
    write_to_google_sheets(sheet_data)

    logger.info("Все операции завершены!")


def main():
#    logger.info("Start")
#    startprj()
 schedule.every().day.at("03:25").do(startprj)
 while True:
    schedule.run_pending()
#    logger.info("Start")
# Проверяем, запущен ли скрипт напрямую

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Application crashed: {e}", exc_info=True)
