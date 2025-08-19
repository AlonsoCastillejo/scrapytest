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

from habitaclia.scraper.core import HabitacliaMultiCityScraper
from habitaclia.config.cities import SPANISH_CITIES, CITY_GROUPS
from habitaclia.data.validator import PropertyDataValidator

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
        with HabitacliaMultiCityScraper(config) as scraper:
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
            
            # Validación de datos
            if args.validate:
                print("\n🔍 Validando calidad de datos...")
                validator = PropertyDataValidator()
                validation_report = validator.validate_dataset(properties_data)
                
                print(f"📊 Reporte de validación:")
                print(f"   Propiedades válidas: {validation_report['valid_properties']}/{validation_report['total_properties']} ({validation_report['validity_rate']}%)")
                print(f"   URLs duplicadas: {validation_report['duplicate_urls']}")
                
                print("\n💡 Recomendaciones:")
                for rec in validation_report['recommendations']:
                    print(f"   {rec}")
                
                # Limpieza automática si se solicita
                if args.clean and validation_report['validity_rate'] < 90:
                    print("\n🧹 Limpiando datos...")
                    properties_data, cleaning_report = validator.clean_dataset(properties_data)
                    print(f"   Eliminados: {cleaning_report['duplicates_removed']} duplicados, {cleaning_report['invalid_removed']} inválidos")
                    print(f"   Datos finales: {len(properties_data)} propiedades")
            
            # Guardar datos
            print("\n💾 Guardando datos...")
            if args.output:
                csv_filename = args.output if args.output.endswith('.csv') else f"{args.output}.csv"
                json_filename = args.output.replace('.csv', '.json') if args.output.endswith('.csv') else f"{args.output}.json"
                scraper.save_data(csv_filename, json_filename)
            else:
                scraper.save_data()
            
            # Estadísticas finales
            if args.verbose:
                print("\n📈 Estadísticas finales:")
                stats = scraper.get_statistics()
                print(f"   Total propiedades: {stats['total_properties']}")
                print(f"   Ciudades scrapeadas: {stats['cities_scraped']}")
                
                if 'price_stats' in stats:
                    price_stats = stats['price_stats']
                    print(f"   Precio promedio: {price_stats['avg_price']:.2f}€")
                    print(f"   Rango de precios: {price_stats['min_price']:.0f}€ - {price_stats['max_price']:.0f}€")
                
                print("\n🏙️  Propiedades por ciudad:")
                for city, count in stats['properties_by_city'].items():
                    print(f"     {city}: {count}")
            
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
