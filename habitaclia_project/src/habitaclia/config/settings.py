"""Configuración centralizada del scraper"""
from pathlib import Path

# Paths del proyecto
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
LOGS_DIR = PROJECT_ROOT / "logs"

# URLs base
BASE_URL = "https://english.habitaclia.com"

# Configuración por defecto
DEFAULT_CONFIG = {
    'max_pages': 2,
    'property_type': 'rent',
    'delay_between_pages': (3, 7),
    'delay_between_properties': (2, 5),
    'delay_between_cities': (30, 60),
    'timeout': 20,
    'max_retries': 3
}

# Headers HTTP por defecto
DEFAULT_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Cache-Control': 'max-age=0'
}