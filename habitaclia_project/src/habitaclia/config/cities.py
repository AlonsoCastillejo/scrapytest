"""Lista de ciudades españolas disponibles"""

# Copiar tu diccionario spanish_cities desde core.py
SPANISH_CITIES = {
    'barcelona': 'Barcelona',
    'madrid': 'Madrid', 
    'valencia': 'Valencia',
    # ... resto de ciudades desde tu código
}

# Grupos útiles
CITY_GROUPS = {
    'tier1': ['barcelona', 'madrid', 'valencia', 'sevilla'],
    'tier2': ['bilbao', 'malaga', 'zaragoza', 'murcia', 'palma'],
    'all_main': ['barcelona', 'madrid', 'valencia', 'sevilla', 'bilbao', 'malaga', 'zaragoza']
}

def validate_city(city_code: str) -> bool:
    return city_code in SPANISH_CITIES

def get_city_name(city_code: str) -> str:
    return SPANISH_CITIES.get(city_code, city_code)