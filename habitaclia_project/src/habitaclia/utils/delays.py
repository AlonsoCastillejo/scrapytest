"""Gestión inteligente de delays para evitar detección"""
import time
import random
import logging
from typing import Tuple

class DelayManager:
    """Maneja delays inteligentes entre requests"""
    
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def wait_between_pages(self):
        """Delay entre páginas de búsqueda"""
        delay = random.uniform(*self.config['delay_between_pages'])
        time.sleep(delay)
    
    def wait_between_properties(self):
        """Delay entre propiedades individuales"""
        delay = random.uniform(*self.config['delay_between_properties'])
        time.sleep(delay)
    
    def wait_between_cities(self):
        """Delay entre ciudades"""
        delay = random.uniform(*self.config['delay_between_cities'])
        next_city = "próxima ciudad"  # Placeholder
        self.logger.info(f"⏳ Esperando {delay:.0f}s antes de {next_city}...")
        time.sleep(delay)