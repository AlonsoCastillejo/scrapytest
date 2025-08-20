"""Tests básicos para el scraper"""
import pytest
from src.habitaclia.config.cities import validate_city, get_city_name
from src.habitaclia.scraper.extractors import PropertyExtractor

def test_city_validation():
    assert validate_city('barcelona') == True
    assert validate_city('ciudad_inexistente') == False

def test_city_name():
    assert get_city_name('barcelona') == 'Barcelona'
    assert get_city_name('madrid') == 'Madrid'

def test_extractor_initialization():
    extractor = PropertyExtractor('https://test.com')
    assert extractor.base_url == 'https://test.com'

# Test con datos reales de tu CSV
def test_data_structure():
    """Test usando tus datos existentes"""
    # Cargar tu CSV actual
    import pandas as pd
    import glob
    
    csv_files = glob.glob('data/raw/habitaclia_properties_*.csv')
    if csv_files:
        df = pd.read_csv(csv_files[0])
        
        # Verificar estructura básica
        assert 'title' in df.columns
        assert 'price' in df.columns
        assert 'city_name' in df.columns
        assert len(df) > 0
        
        print(f"✅ Test con {len(df)} propiedades reales")