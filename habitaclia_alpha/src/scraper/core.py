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

class HabitacliaMultiCityScraper:
    def __init__(self):
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
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('habitaclia_multicity.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Datos almacenados
        self.all_properties_data = []
        
        # Lista de ciudades españolas disponibles en Habitaclia
        self.spanish_cities = {
            # Ciudades principales
            'barcelona': 'Barcelona',
            'madrid': 'Madrid', 
            'valencia': 'Valencia',
            'sevilla': 'Sevilla',
            'bilbao': 'Bilbao',
            'malaga': 'Málaga',
            'zaragoza': 'Zaragoza',
            'murcia': 'Murcia',
            'palma': 'Palma de Mallorca',
            'las-palmas': 'Las Palmas',
            'valladolid': 'Valladolid',
            'vigo': 'Vigo',
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
    
    def rotate_user_agent(self):
        """Rota el User-Agent"""
        new_ua = random.choice(self.user_agents)
        self.session.headers.update({'User-Agent': new_ua})
    
    def get_available_cities(self):
        """Retorna la lista de ciudades disponibles"""
        return self.spanish_cities
    
    def print_available_cities(self):
        """Muestra las ciudades disponibles"""
        print("🏙️  Ciudades disponibles para scraping:")
        print("=" * 50)
        for code, name in self.spanish_cities.items():
            print(f"  {code:<20} → {name}")
        print("=" * 50)
    
    def get_search_urls(self, city, property_type="rent", max_pages=2):
        """Genera URLs para una ciudad específica"""
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
    
    def extract_property_links(self, search_url, city_name):
        """Extrae enlaces de propiedades de una URL de búsqueda"""
        try:
            self.rotate_user_agent()
            time.sleep(random.uniform(3, 7))
            
            response = self.session.get(search_url, timeout=20)
            
            if response.status_code != 200:
                self.logger.warning(f"❌ Error {response.status_code} en {search_url}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Buscar enlaces de propiedades
            property_links = []
            links = soup.select('a[href*="-i"]')
            
            for link in links:
                href = link.get('href')
                if href and '.htm' in href and 'flats-houses' not in href:
                    full_url = urljoin(self.base_url, href)
                    property_links.append(full_url)
            
            unique_links = list(set(property_links))
            self.logger.info(f"🔗 {city_name}: {len(unique_links)} enlaces encontrados")
            
            return unique_links
            
        except Exception as e:
            self.logger.error(f"❌ Error extrayendo enlaces de {search_url}: {e}")
            return []
    
    def extract_property_data(self, property_url, city_code, city_name):
        """Extrae datos de una propiedad específica"""
        try:
            self.rotate_user_agent()
            time.sleep(random.uniform(2, 5))
            
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
            
            # Extraer información
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
    
    def _extract_title(self, soup):
        """Extrae título"""
        data = {}
        title_selectors = ['h1', '.property-title', '.listing-title', '.title']
        
        for selector in title_selectors:
            element = soup.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if title and len(title) > 10:
                    data['title'] = title
                    break
        return data
    
    def _extract_price(self, soup):
        """Extrae precio"""
        data = {}
        
        # Buscar precio en texto
        text = soup.get_text()
        price_patterns = [
            r'(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)\s*€',
            r'€\s*(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)'
        ]
        
        for pattern in price_patterns:
            matches = re.findall(pattern, text)
            if matches:
                for match in matches:
                    try:
                        clean_price = match.replace('.', '').replace(',', '.')
                        price_num = float(clean_price)
                        if 50 <= price_num <= 50000000:  # Rango razonable
                            data['price'] = price_num
                            data['price_raw'] = f"{match}€"
                            return data
                    except:
                        continue
        
        return data
    
    def _extract_location(self, soup):
        """Extrae ubicación"""
        data = {}
        
        location_selectors = ['.location', '.address', '.property-location', '.zone']
        
        for selector in location_selectors:
            element = soup.select_one(selector)
            if element:
                location = element.get_text(strip=True)
                if location and len(location) > 3:
                    data['location'] = location
                    break
        
        return data
    
    def _extract_specifications(self, soup):
        """Extrae especificaciones (habitaciones, baños, m²)"""
        data = {}
        text = soup.get_text()
        
        # Habitaciones
        room_patterns = [r'(\d+)\s*(?:rooms?|habitaciones?|hab\.?|dormitorios?)']
        for pattern in room_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    rooms = int(match.group(1))
                    if 1 <= rooms <= 20:
                        data['rooms'] = rooms
                        break
                except:
                    continue
        
        # Baños
        bath_patterns = [r'(\d+)\s*(?:bath|bathroom|baño|aseo)']
        for pattern in bath_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    baths = int(match.group(1))
                    if 1 <= baths <= 10:
                        data['bathrooms'] = baths
                        break
                except:
                    continue
        
        # Metros cuadrados
        area_patterns = [r'(\d+)\s*m[²2]']
        for pattern in area_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    area = int(match.group(1))
                    if 20 <= area <= 1000:
                        data['area_m2'] = area
                        break
                except:
                    continue
        
        return data
    
    def _extract_description(self, soup):
        """Extrae descripción"""
        data = {}
        
        desc_selectors = ['.description', '.property-description', '.details']
        
        for selector in desc_selectors:
            element = soup.select_one(selector)
            if element:
                desc = element.get_text(strip=True)
                if desc and len(desc) > 50:
                    data['description'] = desc[:500]  # Limitar longitud
                    break
        
        return data
    
    def _extract_images(self, soup):
        """Extrae información de imágenes"""
        data = {}
        
        img_urls = []
        images = soup.select('img[src*="habimg.com"], .gallery img, .property-images img')
        
        for img in images:
            src = img.get('src') or img.get('data-src')
            if src:
                if not src.startswith('http'):
                    src = urljoin(self.base_url, src)
                img_urls.append(src)
        
        limited_img_urls = list(set(img_urls))[:5]  # Limitar a las primeras 5 imágenes
        data['image_urls'] = limited_img_urls
        data['image_count'] = len(data['image_urls'])
        
        return data
    
    def scrape_city(self, city_code, property_type="rent", max_pages=2):
        """Scrapea una ciudad específica"""
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
                
                # Log progreso
                if i % 5 == 0:  # Log cada 5 propiedades
                    self.logger.info(f"📈 {city_name}: {i+1}/{len(unique_links)} procesadas")
        
        self.logger.info(f"✅ {city_name} completada: {len(city_properties)} propiedades extraídas")
        
        return city_properties
    
    def scrape_multiple_cities(self, cities=None, property_type="rent", max_pages=2, delay_between_cities=(30, 60)):
        """
        Scrapea múltiples ciudades
        
        Args:
            cities: Lista de códigos de ciudad o None para todas las principales
            property_type: "rent" o "homes"
            max_pages: Páginas por ciudad
            delay_between_cities: Delay entre ciudades (min, max) en segundos
        """
        
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
            return
        
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
                    self.logger.info(f"⏳ Esperando {delay:.0f}s antes de {self.spanish_cities[valid_cities[i+1]]}...")
                    time.sleep(delay)
                
            except Exception as e:
                self.logger.error(f"❌ Error scrapeando {city_name}: {e}")
                continue
        
        self.logger.info(f"🎉 ¡Scraping multi-ciudad completado!")
        self.logger.info(f"📊 Total final: {total_properties} propiedades de {len(valid_cities)} ciudades")
        
        return self.all_properties_data
    
    def get_statistics(self):
        """Genera estadísticas de los datos extraídos"""
        if not self.all_properties_data:
            return "No hay datos para analizar"
        
        df = pd.DataFrame(self.all_properties_data)
        
        stats = {
            'total_properties': len(df),
            'cities_scraped': df['city_name'].nunique(),
            'properties_by_city': df['city_name'].value_counts().to_dict(),
            'avg_price': df['price'].mean() if 'price' in df.columns else None,
            'price_range': {
                'min': df['price'].min(),
                'max': df['price'].max()
            } if 'price' in df.columns else None,
            'avg_area': df['area_m2'].mean() if 'area_m2' in df.columns else None
        }
        
        return stats
    
    def save_to_csv(self, filename=None):
        """Guarda datos en CSV con información de ciudad"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"habitaclia_multicities_{timestamp}.csv"
        
        if self.all_properties_data:
            df = pd.DataFrame(self.all_properties_data)
            
            # Reordenar columnas con ciudad al principio
            column_order = ['city_name', 'city_code', 'title', 'price', 'location', 
                          'rooms', 'bathrooms', 'area_m2', 'url', 'timestamp']
            
            existing_cols = df.columns.tolist()
            ordered_cols = [col for col in column_order if col in existing_cols]
            remaining_cols = [col for col in existing_cols if col not in ordered_cols]
            final_cols = ordered_cols + remaining_cols
            
            df = df[final_cols]
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"💾 Datos guardados en: {filename}")
            self.logger.info(f"📊 {len(df)} propiedades de {df['city_name'].nunique()} ciudades")
            
            # Mostrar distribución por ciudad
            city_counts = df['city_name'].value_counts()
            self.logger.info("🏙️  Distribución por ciudad:")
            for city, count in city_counts.items():
                self.logger.info(f"   {city}: {count} propiedades")
        else:
            self.logger.warning("⚠️  No hay datos para guardar")
    
    def save_to_json(self, filename=None):
        """Guarda en JSON"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"habitaclia_multicities_{timestamp}.json"
        
        if self.all_properties_data:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.all_properties_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"💾 Datos JSON guardados en: {filename}")

# Ejemplo de uso
if __name__ == "__main__":
    scraper = HabitacliaMultiCityScraper()
    
    # Mostrar ciudades disponibles
    scraper.print_available_cities()
    
    print("\n🚀 CONFIGURACIÓN DE SCRAPING")
    print("=" * 50)
    
    # Opción 1: Ciudades principales (recomendado para empezar)
    main_cities = ['barcelona', 'madrid', 'valencia', 'sevilla']
    print(f"Ciudades principales: {[scraper.spanish_cities[city] for city in main_cities]}")
    
    # Opción 2: Todas las ciudades disponibles (¡MUCHO TIEMPO!)
    # all_cities = list(scraper.spanish_cities.keys())
    
    # Configuración
    property_type = "rent"  # o "homes"
    max_pages = 2          # páginas por ciudad
    
    print(f"Tipo de propiedad: {property_type}")
    print(f"Páginas por ciudad: {max_pages}")
    print("=" * 50)
    
    # Ejecutar scraping
    scraper.scrape_multiple_cities(
        cities=main_cities,
        property_type=property_type,
        max_pages=max_pages,
        delay_between_cities=(30, 45)  # 30-45 segundos entre ciudades
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