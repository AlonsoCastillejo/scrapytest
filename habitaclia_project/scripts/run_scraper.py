#!/usr/bin/env python3
"""
Script principal para ejecutar el scraper de Habitaclia
Uso: python scripts/run_scraper.py --cities barcelona madrid --type rent --pages 2
"""
import argparse
import sys
from pathlib import Path

# Añadir src al path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

try:
    from habitaclia.scraper.core import HabitacliaMultiCityScraper
    from habitaclia.config.cities import SPANISH_CITIES, CITY_GROUPS
except ImportError:
    # Fallback si la estructura modular no está completa
    print("⚠️  Estructura modular no encontrada, usando import directo...")
    sys.path.insert(0, str(project_root))
    from src.habitaclia.scraper.core import HabitacliaMultiCityScraper
    
    # Definir ciudades directamente si no existe el módulo
    SPANISH_CITIES = {
        'barcelona': 'Barcelona', 'madrid': 'Madrid', 'valencia': 'Valencia',
        'sevilla': 'Sevilla', 'bilbao': 'Bilbao', 'malaga': 'Málaga',
        'zaragoza': 'Zaragoza', 'murcia': 'Murcia', 'palma': 'Palma de Mallorca'
    }
    
    CITY_GROUPS = {
        'tier1': ['barcelona', 'madrid', 'valencia', 'sevilla'],
        'tier2': ['bilbao', 'malaga', 'zaragoza', 'murcia', 'palma'],
        'all_main': ['barcelona', 'madrid', 'valencia', 'sevilla', 'bilbao', 'malaga', 'zaragoza']
    }

def parse_arguments():
    """Configura y parsea argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(
        description='Habitaclia Real Estate Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  # Scrapear Barcelona y Madrid, alquiler, 2 páginas
  python scripts/run_scraper.py --cities barcelona madrid --type rent --pages 2
  
  # Scrapear todas las ciudades principales
  python scripts/run_scraper.py --group tier1 --type homes --pages 1
  
  # Scrapear una ciudad específica con más páginas
  python scripts/run_scraper.py --cities valencia --pages 5 --output valencia_rent.csv
        """
    )
    
    # Grupo principal de argumentos
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--cities', 
        nargs='+', 
        choices=list(SPANISH_CITIES.keys()),
        help='Lista de ciudades a scrapear'
    )
    group.add_argument(
        '--group',
        choices=['tier1', 'tier2', 'all_main'],
        help='Grupo predefinido de ciudades'
    )
    
    # Configuración del scraping
    parser.add_argument(
        '--type', 
        choices=['rent', 'homes'], 
        default='rent',
        help='Tipo de propiedad (default: rent)'
    )
    parser.add_argument(
        '--pages', 
        type=int, 
        default=2,
        help='Páginas máximas por ciudad (default: 2)'
    )
    parser.add_argument(
        '--output', 
        help='Nombre del archivo de salida (opcional)'
    )
    
    # Configuración de delays
    parser.add_argument(
        '--fast',
        action='store_true',
        help='Modo rápido con delays reducidos (más riesgo de detección)'
    )
    parser.add_argument(
        '--slow',
        action='store_true',
        help='Modo lento con delays aumentados (más seguro)'
    )
    
    # Opciones adicionales
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validar calidad de datos después del scraping'
    )
    parser.add_argument(
        '--clean',
        action='store_true',
        help='Limpiar datos automáticamente (remover duplicados e inválidos)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Mostrar información detallada'
    )
    
    return parser.parse_args()

def get_scraping_config(args):
    """Genera configuración de scraping basada en argumentos"""
    config = {}
    
    # Configurar delays según modo
    if args.fast:
        config.update({
            'delay_between_pages': (1, 3),
            'delay_between_properties': (1, 2),
            'delay_between_cities': (10, 20)
        })
        print("⚡ Modo RÁPIDO activado - Mayor riesgo de detección")
    elif args.slow:
        config.update({
            'delay_between_pages': (5, 10),
            'delay_between_properties': (3, 8),
            'delay_between_cities': (60, 120)
        })
        print("🐌 Modo LENTO activado - Más seguro pero tardará más")
    else:
        print("🚀 Modo NORMAL activado - Balance entre velocidad y seguridad")
    
    return config

def show_cities_info(cities):
    """Muestra información sobre las ciudades a scrapear"""
    print("\n🏙️  Ciudades seleccionadas:")
    print("=" * 50)
    total_estimated_time = 0
    
    for city in cities:
        city_name = SPANISH_CITIES.get(city, city)
        print(f"  📍 {city:<15} → {city_name}")
        total_estimated_time += 15  # Estimación: 15 min por ciudad
    
    print("=" * 50)
    print(f"⏱️  Tiempo estimado: {total_estimated_time} minutos")
    print(f"📊 Total ciudades: {len(cities)}")

def main():
    """Función principal"""
    args = parse_arguments()
    
    # Determinar ciudades a scrapear
    if args.cities:
        cities = args.cities
    else:
        cities = CITY_GROUPS[args.group]
    
    # Mostrar información
    print("🏠 HABITACLIA SCRAPER - FASE ALPHA")
    print("=" * 60)
    show_cities_info(cities)
    
    # Configuración de scraping
    config = get_scraping_config(args)
    
    print(f"\n🔧 Configuración:")
    print(f"   Tipo: {args.type}")
    print(f"   Páginas por ciudad: {args.pages}")
    if args.output:
        print(f"   Archivo de salida: {args.output}")
    
    # Confirmación del usuario
    if len(cities) > 3:
        response = input(f"\n⚠️  Vas a scrapear {len(cities)} ciudades. ¿Continuar? (y/N): ")
        if response.lower() != 'y':
            print("❌ Operación cancelada")
            return
    
    print("\n🚀 Iniciando scraping...")
    print("=" * 60)
    
    # Ejecutar scraping
    try:
        # ✅ Crear scraper con configuración (sin 'with')
        scraper = HabitacliaMultiCityScraper(config)
        
        # Scrapear datos
        properties_data = scraper.scrape_multiple_cities(
            cities=cities,
            property_type=args.type,
            max_pages=args.pages
        )
        
        if not properties_data:
            print("❌ No se obtuvieron datos")
            return
        
        print(f"\n✅ Scraping completado: {len(properties_data)} propiedades")
        
        # Validación de datos (comentada hasta implementar)
        if args.validate:
            print("\n🔍 Validando calidad de datos...")
            print("⚠️  Validación no implementada aún - será añadida en próxima versión")
            
            # TODO: Implementar cuando esté disponible
            # try:
            #     from habitaclia.data.validator import PropertyDataValidator
            #     validator = PropertyDataValidator()
            #     validation_report = validator.validate_dataset(properties_data)
            #     print(f"📊 Propiedades válidas: {validation_report['valid_properties']}")
            # except ImportError:
            #     print("⚠️  Módulo de validación no disponible")
        
        # ✅ Guardar datos usando métodos correctos
        print("\n💾 Guardando datos...")
        if args.output:
            # Preparar nombres de archivo
            if args.output.endswith('.csv'):
                csv_filename = args.output
                json_filename = args.output.replace('.csv', '.json')
            else:
                csv_filename = f"{args.output}.csv"
                json_filename = f"{args.output}.json"
            
            # Guardar con nombres específicos
            scraper.save_to_csv(csv_filename)
            scraper.save_to_json(json_filename)
            print(f"📁 Archivos guardados: {csv_filename}, {json_filename}")
        else:
            # Guardar con nombres automáticos basados en timestamp
            scraper.save_to_csv()
            scraper.save_to_json()
        
        # Estadísticas finales
        if args.verbose:
            print("\n📈 Estadísticas finales:")
            stats = scraper.get_statistics()
            
            if 'error' not in stats:
                print(f"   Total propiedades: {stats['total_properties']}")
                print(f"   Ciudades scrapeadas: {stats['cities_scraped']}")
                
                # Estadísticas de precios
                if 'price_stats' in stats:
                    price_stats = stats['price_stats']
                    print(f"   Precio promedio: {price_stats['avg_price']:.2f}€")
                    print(f"   Rango: {price_stats['min_price']:.0f}€ - {price_stats['max_price']:.0f}€")
                    print(f"   Propiedades con precio: {price_stats['properties_with_price']}")
                
                # Estadísticas de área
                if 'area_stats' in stats:
                    area_stats = stats['area_stats']
                    print(f"   Área promedio: {area_stats['avg_area']:.1f}m²")
                    print(f"   Propiedades con área: {area_stats['properties_with_area']}")
                
                print(f"\n🏙️  Propiedades por ciudad:")
                for city, count in stats['properties_by_city'].items():
                    print(f"     {city}: {count}")
            else:
                print(f"   ❌ {stats['error']}")
        
        print("\n🎉 ¡Proceso completado exitosamente!")
        
    except KeyboardInterrupt:
        print("\n⚠️  Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"\n❌ Error durante el scraping: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()