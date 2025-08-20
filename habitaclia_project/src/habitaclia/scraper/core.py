import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from urllib.parse import urljoin
import json
import re
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import Dict, List, Optional, Any

class HabitacliaMultiCityScraper:
    def __init__(self, config: Optional[Dict] = None):
        """
        Inicializa el scraper con configuración opcional
        
        Args:
            config: Diccionario con configuración de scraping
        """
        # Configuración por defecto
        self.config = config or {}
        
        # URLs y configuración base
        self.base_url = "https://english.habitaclia.com"
        self.session = requests.Session()
        
        # Headers realistas
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
        self.session.headers.update(self.headers)
        
        # Configurar logging
        self._setup_logging()
        
        # Datos almacenados
        self.all_properties_data = []
        
        # Lista de ciudades españolas disponibles en Habitaclia
        self.spanish_cities = {
            # Ciudades principales (Tier 1)
            'barcelona': 'Barcelona',
            'madrid': 'Madrid', 
            'valencia': 'Valencia',
            'sevilla': 'Sevilla',
            
            # Ciudades grandes (Tier 2)
            'bilbao': 'Bilbao',
            'malaga': 'Málaga',
            'zaragoza': 'Zaragoza',
            'murcia': 'Murcia',
            'palma': 'Palma de Mallorca',
            'las-palmas': 'Las Palmas',
            'valladolid': 'Valladolid',
            'vigo': 'Vigo',
            
            # Ciudades medianas (Tier 3)
            'gijon': 'Gijón',
            'hospitalet': 'L\'Hospitalet',
            'cordoba': 'Córdoba',
            'alicante': 'Alicante',
            'vitoria': 'Vitoria-Gasteiz',
            'granada': 'Granada',
            'oviedo': 'Oviedo',
            'santa-cruz-tenerife': 'Santa Cruz de Tenerife',
            'pamplona': 'Pamplona',
            'almeria': 'Almería',
            'san-sebastian': 'San Sebastián',
            'burgos': 'Burgos',
            'salamanca': 'Salamanca',
            'huelva': 'Huelva',
            'lleida': 'Lleida',
            'tarragona': 'Tarragona',
            'cadiz': 'Cádiz',
            'jaen': 'Jaén',
            'santander': 'Santander',
            'logroño': 'Logroño'
        }
        
        # User agents para rotación
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
    
    def _setup_logging(self):
        """Configura el sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('habitaclia_multicity.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources"""
        if hasattr(self.session, 'close'):
            self.session.close()
    
    def rotate_user_agent(self):
        """Rota el User-Agent para evitar detección"""
        new_ua = random.choice(self.user_agents)
        self.session.headers.update({'User-Agent': new_ua})
    
    def get_available_cities(self) -> Dict[str, str]:
        """Retorna la lista de ciudades disponibles"""
        return self.spanish_cities
    
    def print_available_cities(self):
        """Muestra las ciudades disponibles en formato legible"""
        print("🏙️  Ciudades disponibles para scraping:")
        print("=" * 50)
        for code, name in self.spanish_cities.items():
            print(f"  {code:<20} → {name}")
        print("=" * 50)
    
    def get_search_urls(self, city: str, property_type: str = "rent", max_pages: int = 2) -> List[str]:
        """
        Genera URLs de búsqueda para una ciudad específica
        
        Args:
            city: Código de la ciudad
            property_type: "rent" o "homes"/"buy"
            max_pages: Número máximo de páginas a scrapear
            
        Returns:
            Lista de URLs de búsqueda
        """
        urls = []
        
        if property_type == "rent":
            urls.append(f"{self.base_url}/rent-{city}.htm")
            for page in range(2, max_pages + 1):
                urls.append(f"{self.base_url}/rent-{city}-{page}.htm")
                
        elif property_type == "homes" or property_type == "buy":
            urls.append(f"{self.base_url}/homes-{city}.htm")
            for page in range(2, max_pages + 1):
                urls.append(f"{self.base_url}/homes-{city}-{page}.htm")
        
        return urls
    
    def extract_property_links(self, search_url: str, city_name: str) -> List[str]:
        """
        Extrae enlaces de propiedades de una URL de búsqueda
        
        Args:
            search_url: URL de la página de búsqueda
            city_name: Nombre de la ciudad para logging
            
        Returns:
            Lista de URLs de propiedades únicas
        """
        try:
            self.rotate_user_agent()
            
            # Delay inteligente basado en configuración
            delay_range = self.config.get('delay_between_pages', (3, 7))
            time.sleep(random.uniform(*delay_range))
            
            response = self.session.get(search_url, timeout=20)
            
            if response.status_code != 200:
                self.logger.warning(f"❌ Error {response.status_code} en {search_url}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Buscar enlaces de propiedades con selectores mejorados
            property_links = []
            selectors = [
                'a[href*="-i"]',  # Selector principal
                'a[href*="/rent-"][href*="-i"]',  # Específico para alquileres
                'a[href*="/homes-"][href*="-i"]',  # Específico para ventas
            ]
            
            for selector in selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href and '.htm' in href and 'flats-houses' not in href:
                        full_url = urljoin(self.base_url, href)
                        property_links.append(full_url)
                
                if property_links:  # Si encontramos enlaces, no necesitamos otros selectores
                    break
            
            unique_links = list(set(property_links))
            self.logger.info(f"🔗 {city_name}: {len(unique_links)} enlaces encontrados")
            
            return unique_links
            
        except Exception as e:
            self.logger.error(f"❌ Error extrayendo enlaces de {search_url}: {e}")
            return []
    
    def extract_property_data(self, property_url: str, city_code: str, city_name: str) -> Optional[Dict[str, Any]]:
        """
        Extrae datos de una propiedad específica
        
        Args:
            property_url: URL de la propiedad
            city_code: Código de la ciudad
            city_name: Nombre de la ciudad
            
        Returns:
            Diccionario con datos de la propiedad o None si falla
        """
        try:
            self.rotate_user_agent()
            
            # Delay inteligente basado en configuración
            delay_range = self.config.get('delay_between_properties', (2, 5))
            time.sleep(random.uniform(*delay_range))
            
            response = self.session.get(property_url, timeout=15)
            
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Datos base
            property_data = {
                'url': property_url,
                'city_code': city_code,
                'city_name': city_name,
                'timestamp': datetime.now().isoformat(),
                'status': 'success'
            }
            
            # Extraer información usando métodos especializados
            property_data.update(self._extract_title(soup))
            property_data.update(self._extract_price(soup))
            property_data.update(self._extract_location(soup))
            property_data.update(self._extract_specifications(soup))
            property_data.update(self._extract_description(soup))
            property_data.update(self._extract_images(soup))
            
            return property_data
            
        except Exception as e:
            self.logger.error(f"❌ Error extrayendo datos de {property_url}: {e}")
            return None
    
    def _extract_title(self, soup) -> Dict[str, str]:
        """Extrae el título de la propiedad"""
        data = {}
        title_selectors = ['h1', '.property-title', '.listing-title', '.title', '[data-test="property-title"]']
        
        for selector in title_selectors:
            element = soup.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if title and len(title) > 10:
                    data['title'] = title
                    break
        return data
    
    def _extract_price(self, soup) -> Dict[str, Any]:
        """Extrae el precio de la propiedad con validación mejorada"""
        data = {}
        
        # Buscar precio en todo el texto de la página
        text = soup.get_text()
        price_patterns = [
            r'(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)\s*€',
            r'€\s*(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)',
            r'(\d{1,3}(?:\.\d{3})*)\s*euros?',
            r'Price:\s*(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)'
        ]
        
        for pattern in price_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                for match in matches:
                    try:
                        # Limpiar y convertir precio
                        clean_price = match.replace('.', '').replace(',', '.')
                        price_num = float(clean_price)
                        
                        # Validación de rango razonable para propiedades
                        if 50 <= price_num <= 50000000:
                            data['price'] = price_num
                            data['price_raw'] = f"{match}€"
                            return data
                    except (ValueError, TypeError):
                        continue
        
        return data
    
    def _extract_location(self, soup) -> Dict[str, str]:
        """Extrae la ubicación de la propiedad"""
        data = {}
        
        location_selectors = [
            '.location', '.address', '.property-location', '.zone',
            '[data-test="property-location"]', '.property-address'
        ]
        
        for selector in location_selectors:
            element = soup.select_one(selector)
            if element:
                location = element.get_text(strip=True)
                if location and len(location) > 3:
                    data['location'] = location
                    break
        
        return data
    
    def _extract_specifications(self, soup) -> Dict[str, int]:
        """Extrae especificaciones técnicas (habitaciones, baños, m²)"""
        data = {}
        text = soup.get_text()
        
        # Patrones mejorados para habitaciones
        room_patterns = [
            r'(\d+)\s*(?:rooms?|habitaciones?|hab\.?|dormitorios?|bedrooms?)',
            r'(\d+)\s*bed',
            r'Rooms:\s*(\d+)',
            r'(\d+)\s*(?:rm|room)'
        ]
        
        for pattern in room_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    rooms = int(match.group(1))
                    if 1 <= rooms <= 20:
                        data['rooms'] = rooms
                        break
                except (ValueError, TypeError):
                    continue
        
        # Patrones mejorados para baños
        bath_patterns = [
            r'(\d+)\s*(?:bath|bathroom|baño|aseo|bathrooms?)',
            r'(\d+)\s*(?:wc|toilet)',
            r'Bathrooms?:\s*(\d+)'
        ]
        
        for pattern in bath_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    baths = int(match.group(1))
                    if 1 <= baths <= 10:
                        data['bathrooms'] = baths
                        break
                except (ValueError, TypeError):
                    continue
        
        # Patrones mejorados para metros cuadrados
        area_patterns = [
            r'(\d+)\s*m[²2]',
            r'(\d+)\s*(?:square\s*meters?|metros?\s*cuadrados?)',
            r'(\d+)\s*sqm',
            r'Area:\s*(\d+)',
            r'Size:\s*(\d+)\s*m'
        ]
        
        for pattern in area_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    area = int(match.group(1))
                    if 20 <= area <= 2000:  # Rango ampliado para propiedades grandes
                        data['area_m2'] = area
                        break
                except (ValueError, TypeError):
                    continue
        
        return data
    
    def _extract_description(self, soup) -> Dict[str, str]:
        """Extrae la descripción de la propiedad"""
        data = {}
        
        desc_selectors = [
            '.description', '.property-description', '.details',
            '[data-test="property-description"]', '.property-details'
        ]
        
        for selector in desc_selectors:
            element = soup.select_one(selector)
            if element:
                desc = element.get_text(strip=True)
                if desc and len(desc) > 50:
                    # Limitar longitud y limpiar texto
                    clean_desc = re.sub(r'\s+', ' ', desc)  # Normalizar espacios
                    data['description'] = clean_desc[:500]
                    break
        
        return data
    
    def _extract_images(self, soup) -> Dict[str, Any]:
        """Extrae información de imágenes de la propiedad"""
        data = {}
        
        img_urls = []
        
        # Selectores mejorados para imágenes
        image_selectors = [
            'img[src*="habimg.com"]',
            '.gallery img',
            '.property-images img',
            '.property-photos img',
            '[data-test="property-image"] img'
        ]
        
        for selector in image_selectors:
            images = soup.select(selector)
            for img in images:
                src = img.get('src') or img.get('data-src') or img.get('data-lazy')
                if src:
                    if not src.startswith('http'):
                        src = urljoin(self.base_url, src)
                    img_urls.append(src)
        
        # Limitar número de imágenes basado en configuración
        max_images = self.config.get('max_images', 5)
        unique_urls = list(set(img_urls))[:max_images]
        
        data['image_urls'] = unique_urls
        data['image_count'] = len(unique_urls)
        
        return data
    
    def scrape_city(self, city_code: str, property_type: str = "rent", max_pages: int = 2) -> List[Dict[str, Any]]:
        """
        Scrapea una ciudad específica
        
        Args:
            city_code: Código de la ciudad
            property_type: Tipo de propiedad ("rent" o "homes")
            max_pages: Páginas máximas a scrapear
            
        Returns:
            Lista de propiedades extraídas
        """
        city_name = self.spanish_cities.get(city_code, city_code)
        
        self.logger.info(f"🏙️  Iniciando scraping de {city_name} ({city_code})")
        
        # Obtener URLs de búsqueda
        search_urls = self.get_search_urls(city_code, property_type, max_pages)
        
        city_properties = []
        all_property_links = []
        
        # Extraer enlaces de todas las páginas de búsqueda
        for search_url in search_urls:
            property_links = self.extract_property_links(search_url, city_name)
            all_property_links.extend(property_links)
            
            # Pequeño delay entre páginas
            time.sleep(random.uniform(2, 4))
        
        unique_links = list(set(all_property_links))
        self.logger.info(f"📊 {city_name}: {len(unique_links)} propiedades únicas encontradas")
        
        # Scrapear cada propiedad
        for i, property_url in enumerate(unique_links):
            property_data = self.extract_property_data(property_url, city_code, city_name)
            
            if property_data:
                city_properties.append(property_data)
                
                # Log progreso cada 5 propiedades
                if i % 5 == 0:
                    self.logger.info(f"📈 {city_name}: {i+1}/{len(unique_links)} procesadas")
        
        self.logger.info(f"✅ {city_name} completada: {len(city_properties)} propiedades extraídas")
        
        return city_properties
    
    def scrape_multiple_cities(self, cities: Optional[List[str]] = None, property_type: str = "rent", 
                             max_pages: int = 2, delay_between_cities: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Scrapea múltiples ciudades
        
        Args:
            cities: Lista de códigos de ciudad o None para las principales
            property_type: "rent" o "homes"
            max_pages: Páginas por ciudad
            delay_between_cities: Tuple con delay mín y máx entre ciudades
            
        Returns:
            Lista con todas las propiedades extraídas
        """
        
        # Usar delay de configuración si está disponible
        if delay_between_cities is None:
            delay_between_cities = self.config.get('delay_between_cities', (30, 60))
        
        # Si no se especifican ciudades, usar las principales
        if cities is None:
            cities = ['barcelona', 'madrid', 'valencia', 'sevilla', 'bilbao', 'malaga', 'zaragoza']
        
        # Validar ciudades
        valid_cities = [city for city in cities if city in self.spanish_cities]
        invalid_cities = [city for city in cities if city not in self.spanish_cities]
        
        if invalid_cities:
            self.logger.warning(f"⚠️  Ciudades no válidas ignoradas: {invalid_cities}")
        
        if not valid_cities:
            self.logger.error("❌ No hay ciudades válidas para scrapear")
            return []
        
        self.logger.info(f"🚀 Iniciando scraping multi-ciudad")
        self.logger.info(f"🏙️  Ciudades: {[self.spanish_cities[city] for city in valid_cities]}")
        self.logger.info(f"🏠 Tipo: {property_type}")
        self.logger.info(f"📄 Páginas por ciudad: {max_pages}")
        print("=" * 60)
        
        total_properties = 0
        
        # Scrapear cada ciudad
        for i, city_code in enumerate(valid_cities):
            city_name = self.spanish_cities[city_code]
            
            try:
                # Scrapear ciudad
                city_properties = self.scrape_city(city_code, property_type, max_pages)
                
                # Añadir a datos totales
                self.all_properties_data.extend(city_properties)
                total_properties += len(city_properties)
                
                self.logger.info(f"🎯 Total acumulado: {total_properties} propiedades")
                
                # Delay entre ciudades (excepto la última)
                if i < len(valid_cities) - 1:
                    delay = random.uniform(*delay_between_cities)
                    next_city = self.spanish_cities[valid_cities[i+1]]
                    self.logger.info(f"⏳ Esperando {delay:.0f}s antes de {next_city}...")
                    time.sleep(delay)
                
            except Exception as e:
                self.logger.error(f"❌ Error scrapeando {city_name}: {e}")
                continue
        
        self.logger.info(f"🎉 ¡Scraping multi-ciudad completado!")
        self.logger.info(f"📊 Total final: {total_properties} propiedades de {len(valid_cities)} ciudades")
        
        return self.all_properties_data
    
    def get_statistics(self) -> Dict[str, Any]:
        """Genera estadísticas completas de los datos extraídos"""
        if not self.all_properties_data:
            return {"error": "No hay datos para analizar"}
        
        df = pd.DataFrame(self.all_properties_data)
        
        stats = {
            'total_properties': len(df),
            'cities_scraped': df['city_name'].nunique() if 'city_name' in df.columns else 0,
            'properties_by_city': df['city_name'].value_counts().to_dict() if 'city_name' in df.columns else {},
        }
        
        # Estadísticas de precios
        if 'price' in df.columns and df['price'].notna().any():
            valid_prices = df['price'].dropna()
            stats['price_stats'] = {
                'avg_price': float(valid_prices.mean()),
                'min_price': float(valid_prices.min()),
                'max_price': float(valid_prices.max()),
                'median_price': float(valid_prices.median()),
                'properties_with_price': len(valid_prices)
            }
        
        # Estadísticas de área
        if 'area_m2' in df.columns and df['area_m2'].notna().any():
            valid_areas = df['area_m2'].dropna()
            stats['area_stats'] = {
                'avg_area': float(valid_areas.mean()),
                'min_area': float(valid_areas.min()),
                'max_area': float(valid_areas.max()),
                'median_area': float(valid_areas.median()),
                'properties_with_area': len(valid_areas)
            }
        
        return stats
    
    def save_to_csv(self, filename: Optional[str] = None):
        """Guarda datos en CSV con información organizada por ciudad"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"habitaclia_multicities_{timestamp}.csv"
        
        if self.all_properties_data:
            df = pd.DataFrame(self.all_properties_data)
            
            # Reordenar columnas con información más importante al principio
            preferred_order = [
                'city_name', 'city_code', 'title', 'price', 'price_raw', 
                'location', 'rooms', 'bathrooms', 'area_m2', 'description',
                'image_count', 'url', 'timestamp', 'status'
            ]
            
            existing_cols = df.columns.tolist()
            ordered_cols = [col for col in preferred_order if col in existing_cols]
            remaining_cols = [col for col in existing_cols if col not in ordered_cols]
            final_cols = ordered_cols + remaining_cols
            
            df = df[final_cols]
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"💾 Datos guardados en: {filename}")
            self.logger.info(f"📊 {len(df)} propiedades de {df['city_name'].nunique()} ciudades")
            
            # Mostrar distribución por ciudad
            if 'city_name' in df.columns:
                city_counts = df['city_name'].value_counts()
                self.logger.info("🏙️  Distribución por ciudad:")
                for city, count in city_counts.items():
                    self.logger.info(f"   {city}: {count} propiedades")
        else:
            self.logger.warning("⚠️  No hay datos para guardar")
    
    def save_to_json(self, filename: Optional[str] = None):
        """Guarda en JSON con metadatos estructurados"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"habitaclia_multicities_{timestamp}.json"
        
        if self.all_properties_data:
            # Crear estructura con metadatos
            json_data = {
                "metadata": {
                    "total_properties": len(self.all_properties_data),
                    "extraction_date": datetime.now().isoformat(),
                    "cities": list(set(prop.get('city_name', '') for prop in self.all_properties_data)),
                    "scraper_version": "2.0.0",
                    "config_used": self.config
                },
                "properties": self.all_properties_data
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"💾 Datos JSON guardados en: {filename}")
        else:
            self.logger.warning("⚠️  No hay datos para guardar en JSON")

# Ejemplo de uso directo
if __name__ == "__main__":
    scraper = HabitacliaMultiCityScraper()
    
    # Mostrar ciudades disponibles
    scraper.print_available_cities()
    
    print("\n🚀 CONFIGURACIÓN DE SCRAPING")
    print("=" * 50)
    
    # Configuración de ejemplo
    main_cities = ['barcelona', 'madrid', 'valencia', 'sevilla']
    property_type = "rent"
    max_pages = 2
    
    print(f"Ciudades principales: {[scraper.spanish_cities[city] for city in main_cities]}")
    print(f"Tipo de propiedad: {property_type}")
    print(f"Páginas por ciudad: {max_pages}")
    print("=" * 50)
    
    # Ejecutar scraping
    scraper.scrape_multiple_cities(
        cities=main_cities,
        property_type=property_type,
        max_pages=max_pages,
        delay_between_cities=(30, 45)
    )
    
    # Guardar resultados
    scraper.save_to_csv()
    scraper.save_to_json()
    
    # Mostrar estadísticas
    stats = scraper.get_statistics()
    print("\n📊 ESTADÍSTICAS FINALES")
    print("=" * 50)
    print(f"Total propiedades: {stats['total_properties']}")
    print(f"Ciudades scrapeadas: {stats['cities_scraped']}")
    if stats['avg_price']:
        print(f"Precio promedio: {stats['avg_price']:.2f}€")
    print("\nPropiedades por ciudad:")
    for city, count in stats['properties_by_city'].items():
        print(f"  {city}: {count}")