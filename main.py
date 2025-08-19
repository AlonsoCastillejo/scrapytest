import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from urllib.parse import urljoin, urlparse
import json
import re
from datetime import datetime
import logging

class HabitacliaScraperImproved:
    def __init__(self):
        self.base_url = "https://english.habitaclia.com"
        self.spanish_url = "https://www.habitaclia.com"
        self.session = requests.Session()
        
        # Headers más realistas para evitar detección
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,es;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Cache-Control': 'max-age=0'
        }
        self.session.headers.update(self.headers)
        
        # Configurar logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Lista para almacenar datos
        self.properties_data = []
        
        # Lista de User-Agents para rotar
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
    
    def rotate_user_agent(self):
        """Rota el User-Agent para evitar detección"""
        new_ua = random.choice(self.user_agents)
        self.session.headers.update({'User-Agent': new_ua})
    
    def get_search_urls(self, city="barcelona", property_type="rent", max_pages=3):
        """
        Genera URLs de búsqueda basadas en la estructura real de Habitaclia
        property_type: 'rent' o 'homes' (para compra)
        """
        urls = []
        
        if property_type == "rent":
            base_url = f"{self.base_url}/rent-{city}.htm"
            urls.append(base_url)
            
            # Páginas adicionales siguen el patrón: rent-barcelona-2.htm, rent-barcelona-3.htm, etc.
            for page in range(2, max_pages + 1):
                page_url = f"{self.base_url}/rent-{city}-{page}.htm"
                urls.append(page_url)
                
        elif property_type == "homes" or property_type == "buy":
            base_url = f"{self.base_url}/homes-{city}.htm"
            urls.append(base_url)
            
            # Páginas adicionales: homes-barcelona-2.htm, homes-barcelona-3.htm, etc.
            for page in range(2, max_pages + 1):
                page_url = f"{self.base_url}/homes-{city}-{page}.htm"
                urls.append(page_url)
        
        self.logger.info(f"URLs generadas para {city} - {property_type}: {urls}")
        return urls
    
    def extract_property_links(self, search_url, max_retries=3):
        """Extrae enlaces de propiedades con manejo de errores mejorado"""
        self.logger.info(f"Extrayendo enlaces de: {search_url}")
        
        for attempt in range(max_retries):
            try:
                # Rotar User-Agent en cada intento
                self.rotate_user_agent()
                
                # Delay más conservador
                time.sleep(random.uniform(8, 15))
                
                response = self.session.get(search_url, timeout=30)
                
                # Verificar código de estado
                if response.status_code == 404:
                    self.logger.warning(f"Página no encontrada (404): {search_url}")
                    return []
                elif response.status_code == 403:
                    self.logger.warning(f"Acceso prohibido (403): {search_url} - Intento {attempt + 1}")
                    time.sleep(random.uniform(30, 60))  # Espera más larga si es 403
                    continue
                elif response.status_code != 200:
                    self.logger.warning(f"Código de estado inesperado {response.status_code}: {search_url}")
                    continue
                
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Selectores mejorados basados en la estructura real
                property_links = []
                
                # Intentar diferentes selectores basados en la estructura de Habitaclia
                selectors_to_try = [
                    # Enlaces que contienen el patrón específico de Habitaclia
                    'a[href*="-barcelona-i"]',
                    'a[href*="/rent-"][href*="-i"]',
                    'a[href*="/homes-"][href*="-i"]',
                    'a[href*="-i"][href*=".htm"]',
                    # Selectores por clase (pueden variar)
                    '.property-item a',
                    '.listing-item a',
                    '.result-item a',
                    'article a',
                    '.property a',
                    # Selector más amplio
                    'a[href*=".htm"]'
                ]
                
                for selector in selectors_to_try:
                    links = soup.select(selector)
                    if links:
                        self.logger.info(f"Selector exitoso: {selector} - {len(links)} enlaces encontrados")
                        for link in links:
                            href = link.get('href')
                            if href and ('-i' in href or 'barcelona' in href) and '.htm' in href:
                                # Construir URL completa
                                if href.startswith('http'):
                                    full_url = href
                                else:
                                    full_url = urljoin(self.base_url, href)
                                property_links.append(full_url)
                        break
                
                # Si no encontramos enlaces específicos, buscar cualquier enlace interno
                if not property_links:
                    self.logger.warning("No se encontraron enlaces con selectores específicos, buscando enlaces generales...")
                    all_links = soup.find_all('a', href=True)
                    for link in all_links:
                        href = link.get('href')
                        if href and 'barcelona' in href and ('.htm' in href or '.html' in href):
                            full_url = urljoin(self.base_url, href)
                            property_links.append(full_url)
                
                unique_links = list(set(property_links))
                self.logger.info(f"Enlaces únicos encontrados: {len(unique_links)}")
                
                # Mostrar algunos enlaces de ejemplo para debugging
                if unique_links:
                    self.logger.info(f"Ejemplos de enlaces encontrados:")
                    for i, link in enumerate(unique_links[:3]):
                        self.logger.info(f"  {i+1}: {link}")
                
                return unique_links
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error de conexión en intento {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(30, 60))
                    continue
                else:
                    self.logger.error(f"Falló después de {max_retries} intentos")
                    return []
            
            except Exception as e:
                self.logger.error(f"Error inesperado: {e}")
                return []
        
        return []
    
    def extract_property_data(self, property_url):
        """Extrae datos de una propiedad específica con selectores mejorados"""
        try:
            self.logger.info(f"Scrapeando: {property_url}")
            
            # Rotar User-Agent
            self.rotate_user_agent()
            
            # Delay conservador
            time.sleep(random.uniform(10, 20))
            
            response = self.session.get(property_url, timeout=30)
            
            if response.status_code != 200:
                self.logger.warning(f"Error {response.status_code} al acceder a {property_url}")
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Diccionario para datos
            property_data = {
                'url': property_url,
                'timestamp': datetime.now().isoformat(),
                'status': 'success'
            }
            
            # Extraer datos con selectores más específicos
            property_data.update(self._extract_title(soup))
            property_data.update(self._extract_price(soup))
            property_data.update(self._extract_location(soup))
            property_data.update(self._extract_specifications(soup))
            property_data.update(self._extract_description(soup))
            property_data.update(self._extract_images(soup))
            property_data.update(self._extract_contact_info(soup))
            
            return property_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo datos de {property_url}: {e}")
            return {
                'url': property_url,
                'timestamp': datetime.now().isoformat(),
                'status': 'error',
                'error': str(e)
            }
    
    def _extract_title(self, soup):
        """Extrae el título de la propiedad"""
        data = {}
        
        title_selectors = [
            'h1',
            '.property-title',
            '.listing-title',
            '.title',
            'h1[class*="title"]',
            '[data-test="title"]'
        ]
        
        for selector in title_selectors:
            element = soup.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if title and len(title) > 10:  # Filtrar títulos muy cortos
                    data['title'] = title
                    break
        
        return data
    
    def _extract_price(self, soup):
        """Extrae información de precio con patrones mejorados"""
        data = {}
        
        # Buscar precio en diferentes selectores
        price_selectors = [
            '.price',
            '.property-price',
            '.listing-price',
            '[class*="price"]',
            '[data-price]',
            '.cost',
            '.amount'
        ]
        
        price_found = False
        for selector in price_selectors:
            elements = soup.select(selector)
            for element in elements:
                price_text = element.get_text(strip=True)
                if '€' in price_text:
                    data['price_raw'] = price_text
                    
                    # Extraer número del precio
                    price_match = re.search(r'([\d,.]+)', price_text.replace('.', '').replace(',', '.'))
                    if price_match:
                        try:
                            price_num = float(price_match.group(1))
                            data['price'] = price_num
                            price_found = True
                            break
                        except:
                            pass
            if price_found:
                break
        
        # Si no se encuentra en selectores, buscar en todo el texto
        if not price_found:
            text = soup.get_text()
            price_patterns = [
                r'(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)\s*€',
                r'€\s*(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)',
                r'(\d{1,3}(?:,\d{3})*)\s*euros?'
            ]
            
            for pattern in price_patterns:
                matches = re.findall(pattern, text)
                if matches:
                    for match in matches:
                        try:
                            # Limpiar y convertir
                            clean_price = match.replace('.', '').replace(',', '.')
                            price_num = float(clean_price)
                            if 50 <= price_num <= 10000000:  # Rango razonable
                                data['price'] = price_num
                                data['price_raw'] = f"{match}€"
                                break
                        except:
                            continue
                if 'price' in data:
                    break
        
        return data
    
    def _extract_location(self, soup):
        """Extrae información de ubicación"""
        data = {}
        
        location_selectors = [
            '.location',
            '.address',
            '.property-location',
            '[class*="location"]',
            '[class*="address"]',
            '.zone',
            '.area'
        ]
        
        for selector in location_selectors:
            element = soup.select_one(selector)
            if element:
                location = element.get_text(strip=True)
                if location and len(location) > 3:
                    data['location'] = location
                    break
        
        # Buscar en breadcrumbs o navegación
        if 'location' not in data:
            breadcrumb = soup.select_one('.breadcrumb, .breadcrumbs, nav')
            if breadcrumb:
                location = breadcrumb.get_text(strip=True)
                if 'Barcelona' in location:
                    data['location'] = location
        
        return data
    
    def _extract_specifications(self, soup):
        """Extrae especificaciones como habitaciones, baños, m²"""
        data = {}
        
        # Buscar en todo el texto especificaciones comunes
        text = soup.get_text()
        
        # Habitaciones
        room_patterns = [
            r'(\d+)\s*(?:rooms?|habitaciones?|hab\.?|dormitorios?)',
            r'(?:rooms?|habitaciones?|hab\.?|dormitorios?)[\s:]*(\d+)',
            r'(\d+)\s*bed'
        ]
        
        for pattern in room_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    rooms = int(match.group(1))
                    if 1 <= rooms <= 20:  # Rango razonable
                        data['rooms'] = rooms
                        break
                except:
                    continue
        
        # Baños
        bath_patterns = [
            r'(\d+)\s*(?:bath|bathroom|baño|aseo)',
            r'(?:bath|bathroom|baño|aseo)s?[\s:]*(\d+)'
        ]
        
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
        area_patterns = [
            r'(\d+)\s*m[²2]',
            r'(\d+)\s*(?:square\s*meters?|metros?\s*cuadrados?)',
            r'(\d+)\s*sqm'
        ]
        
        for pattern in area_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    area = int(match.group(1))
                    if 20 <= area <= 1000:  # Rango razonable
                        data['area_m2'] = area
                        break
                except:
                    continue
        
        return data
    
    def _extract_description(self, soup):
        """Extrae descripción de la propiedad"""
        data = {}
        
        desc_selectors = [
            '.description',
            '.property-description',
            '.listing-description',
            '.details',
            '.property-details',
            '.content',
            '.property-content'
        ]
        
        for selector in desc_selectors:
            element = soup.select_one(selector)
            if element:
                desc = element.get_text(strip=True)
                if desc and len(desc) > 50:  # Solo descripciones sustanciales
                    data['description'] = desc
                    break
        
        return data
    
    def _extract_images(self, soup):
        """Extrae URLs de imágenes"""
        data = {}
        
        # Buscar imágenes específicamente de habitaclia
        img_urls = []
        
        # Selectores para imágenes
        img_selectors = [
            'img[src*="habimg.com"]',  # Dominio específico de Habitaclia
            '.gallery img',
            '.property-images img',
            '.listing-images img',
            '.photos img'
        ]
        
        for selector in img_selectors:
            images = soup.select(selector)
            for img in images:
                src = img.get('src') or img.get('data-src') or img.get('data-lazy')
                if src:
                    if not src.startswith('http'):
                        src = urljoin(self.base_url, src)
                    img_urls.append(src)
                    
        limited_img_urls = list(set(img_urls))[:5]  # Limitar a las primeras 5 imágenes
        data['image_urls'] = limited_img_urls
        data['image_count'] = len(data['image_urls'])
        
        return data
    
    def _extract_contact_info(self, soup):
        """Extrae información de contacto si está disponible"""
        data = {}
        
        # Buscar teléfono
        text = soup.get_text()
        phone_patterns = [
            r'(\+34\s*\d{3}\s*\d{3}\s*\d{3})',
            r'(\d{3}\s*\d{3}\s*\d{3})',
            r'(\d{9})'
        ]
        
        for pattern in phone_patterns:
            match = re.search(pattern, text)
            if match:
                data['phone'] = match.group(1)
                break
        
        return data
    
    def scrape_properties(self, city="barcelona", property_type="rent", max_pages=3, delay_range=(10, 20)):
        """Función principal mejorada para scraping"""
        self.logger.info(f"Iniciando scraping mejorado de {city} - {property_type}")
        
        # Obtener URLs de búsqueda
        search_urls = self.get_search_urls(city, property_type, max_pages)
        
        if not search_urls:
            self.logger.error("No se pudieron generar URLs de búsqueda")
            return
        
        all_property_links = []
        
        # Extraer enlaces de cada página
        for i, search_url in enumerate(search_urls):
            self.logger.info(f"Procesando página {i+1}/{len(search_urls)}")
            
            property_links = self.extract_property_links(search_url)
            
            if property_links:
                all_property_links.extend(property_links)
                self.logger.info(f"Enlaces encontrados en esta página: {len(property_links)}")
            else:
                self.logger.warning(f"No se encontraron enlaces en {search_url}")
            
            # Delay entre páginas de búsqueda
            if i < len(search_urls) - 1:  # No delay después de la última página
                delay = random.uniform(*delay_range)
                self.logger.info(f"Esperando {delay:.1f} segundos antes de la siguiente página...")
                time.sleep(delay)
        
        # Eliminar duplicados
        unique_links = list(set(all_property_links))
        self.logger.info(f"Total de propiedades únicas: {len(unique_links)}")
        
        if not unique_links:
            self.logger.error("No se encontraron enlaces de propiedades para scrapear")
            return
        
        # Scrapear cada propiedad
        for i, property_url in enumerate(unique_links):
            try:
                self.logger.info(f"Progreso: {i+1}/{len(unique_links)} - {property_url}")
                
                property_data = self.extract_property_data(property_url)
                
                if property_data:
                    self.properties_data.append(property_data)
                    
                    # Log de datos extraídos
                    if property_data.get('title'):
                        self.logger.info(f"  ✓ Título: {property_data['title'][:50]}...")
                    if property_data.get('price'):
                        self.logger.info(f"  ✓ Precio: {property_data['price']}€")
                    if property_data.get('location'):
                        self.logger.info(f"  ✓ Ubicación: {property_data['location']}")
                else:
                    self.logger.warning(f"  ✗ No se pudieron extraer datos")
                
                # Delay entre propiedades
                if i < len(unique_links) - 1:
                    delay = random.uniform(*delay_range)
                    time.sleep(delay)
                
            except Exception as e:
                self.logger.error(f"Error procesando {property_url}: {e}")
                continue
        
        self.logger.info(f"Scraping completado. Propiedades extraídas: {len(self.properties_data)}")
        
        # Estadísticas
        successful = len([p for p in self.properties_data if p.get('status') == 'success'])
        self.logger.info(f"Extracciones exitosas: {successful}/{len(self.properties_data)}")
    
    def save_to_csv(self, filename=None):
        """Guarda datos con mejor formateo"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"habitaclia_properties_{timestamp}.csv"
        
        if self.properties_data:
            df = pd.DataFrame(self.properties_data)
            
            # Reordenar columnas importantes al principio
            column_order = ['title', 'price', 'location', 'rooms', 'bathrooms', 'area_m2', 
                          'url', 'timestamp', 'status']
            
            # Mantener columnas existentes que no están en el orden
            existing_cols = df.columns.tolist()
            ordered_cols = [col for col in column_order if col in existing_cols]
            remaining_cols = [col for col in existing_cols if col not in ordered_cols]
            final_cols = ordered_cols + remaining_cols
            
            df = df[final_cols]
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"Datos guardados en: {filename}")
            self.logger.info(f"Columnas: {list(df.columns)}")
            self.logger.info(f"Total filas: {len(df)}")
            
            # Estadísticas básicas
            if 'price' in df.columns:
                valid_prices = df['price'].dropna()
                if len(valid_prices) > 0:
                    self.logger.info(f"Precios - Min: {valid_prices.min()}€, Max: {valid_prices.max()}€, Media: {valid_prices.mean():.2f}€")
        else:
            self.logger.warning("No hay datos para guardar")
    
    def save_to_json(self, filename=None):
        """Guarda en JSON con formato mejorado"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"habitaclia_properties_{timestamp}.json"
        
        if self.properties_data:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.properties_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Datos guardados en JSON: {filename}")
        else:
            self.logger.warning("No hay datos para guardar en JSON")

# Ejemplo de uso mejorado
if __name__ == "__main__":
    # Configurar logging más detallado
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('habitaclia_scraper.log'),
            logging.StreamHandler()
        ]
    )
    
    scraper = HabitacliaScraperImproved()
    
    # Configuración más conservadora
    city = "barcelona"
    property_type = "rent"  # o "homes" para compra
    max_pages = 2  # Empezar con pocas páginas
    
    print(f"🏠 Iniciando scraping de Habitaclia")
    print(f"📍 Ciudad: {city}")
    print(f"🏡 Tipo: {property_type}")
    print(f"📄 Páginas máximas: {max_pages}")
    print(f"⏱️  Delays: 10-20 segundos entre requests")
    print(f"🔄 User-Agent rotation: Habilitada")
    print("=" * 50)
    
    # Ejecutar scraping
    scraper.scrape_properties(
        city=city,
        property_type=property_type,
        max_pages=max_pages,
        delay_range=(10, 20)  # Delays muy conservadores
    )
    
    # Guardar resultados
    scraper.save_to_csv()
    scraper.save_to_json()
    
    print("✅ Scraping completado. Revisa los archivos generados.")