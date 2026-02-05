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