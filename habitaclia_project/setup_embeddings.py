#!/usr/bin/env python3
"""
Script de configuración rápida para el sistema de embeddings - VERSIÓN CORREGIDA
Ejecutar desde: habitaclia_project/
"""

import os
import sys
from pathlib import Path

# Añadir src al path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

def check_requirements():
    """Verifica que estén instaladas las dependencias necesarias"""
    
    try:
        import chromadb
        print("✅ chromadb instalado")
    except ImportError:
        print("❌ chromadb no encontrado")
        return False
    
    try:
        import openai
        print("✅ openai instalado")
    except ImportError:
        print("❌ openai no encontrado")
        return False
    
    try:
        import dotenv
        print("✅ python-dotenv instalado")
    except ImportError:
        print("❌ python-dotenv no encontrado")
        return False
    
    try:
        import pandas
        print("✅ pandas instalado")
    except ImportError:
        print("❌ pandas no encontrado")
        return False
    
    print("✅ Todas las dependencias están instaladas")
    return True

def setup_environment():
    """Configura el entorno y crea archivos necesarios"""
    
    print("🔧 Configurando entorno...")
    
    # Crear directorios necesarios
    directories = [
        'src/habitaclia/search',
        'src/habitaclia/config', 
        'data/embeddings',
        'data/embeddings/chromadb',
        'notebooks'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"📁 Directorio creado: {directory}")
    
    # Crear archivo .env si no existe
    env_file = Path('.env')
    if not env_file.exists():
        env_content = """# OpenAI API Configuration
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=text-embedding-3-small
OPENAI_MAX_TOKENS=8000

# ChromaDB Configuration
CHROMADB_PERSIST_DIRECTORY=./data/embeddings/chromadb
CHROMADB_COLLECTION_NAME=spanish_properties

# Project Configuration
PROJECT_ROOT=./
DATA_PATH=./data/
LOGS_PATH=./logs/

# Embedding Configuration
EMBEDDING_BATCH_SIZE=100
EMBEDDING_DIMENSIONS=1536
MAX_DESCRIPTION_LENGTH=500
"""
        with open(env_file, 'w') as f:
            f.write(env_content)
        
        print("📄 Archivo .env creado")
        print("⚠️  IMPORTANTE: Añade tu OpenAI API key al archivo .env")
    else:
        print("📄 Archivo .env ya existe")
    
    # Crear __init__.py files
    init_files = [
        'src/habitaclia/__init__.py',
        'src/habitaclia/search/__init__.py',
        'src/habitaclia/config/__init__.py'
    ]
    
    for init_file in init_files:
        Path(init_file).touch()
    
    print("✅ Entorno configurado correctamente")

def find_csv_files():
    """Encuentra archivos CSV de propiedades en el proyecto"""
    
    csv_files = []
    
    # Buscar en varios directorios
    search_dirs = [
        Path('data'),
        Path('data/raw'),
        Path('.'),  # directorio actual
    ]
    
    patterns = [
        'habitaclia_*.csv',
        'properties_*.csv',
        '*_properties_*.csv'
    ]
    
    for search_dir in search_dirs:
        if search_dir.exists():
            for pattern in patterns:
                csv_files.extend(search_dir.glob(pattern))
    
    return list(set(csv_files))  # Remover duplicados

def create_basic_components():
    """Crea componentes básicos si no existen"""
    
    # Verificar si los archivos de los componentes existen
    components = [
        'src/habitaclia/search/embedding_generator.py',
        'src/habitaclia/search/chromadb_manager.py',
        'src/habitaclia/search/search_engine.py'
    ]
    
    missing_components = []
    for component in components:
        if not Path(component).exists():
            missing_components.append(component)
    
    if missing_components:
        print("❌ Faltan componentes del sistema:")
        for comp in missing_components:
            print(f"   - {comp}")
        print("\n💡 Copia los archivos desde los artefactos proporcionados")
        return False
    
    print("✅ Todos los componentes están presentes")
    return True

def interactive_setup():
    """Configuración interactiva del sistema"""
    
    print("🏠 CONFIGURACIÓN INTERACTIVA DEL SISTEMA DE EMBEDDINGS")
    print("=" * 65)
    
    # Verificar componentes
    if not create_basic_components():
        return False
    
    # Verificar API key
    from dotenv import load_dotenv
    load_dotenv()
    
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key or api_key == 'your_openai_api_key_here':
        print("❌ API key de OpenAI no configurada")
        
        # Solicitar API key interactivamente
        print("\n🔑 Configuración de API Key:")
        print("1. Ve a: https://platform.openai.com/api-keys")
        print("2. Crea una nueva API key")
        print("3. Cópiala aquí (o presiona Enter para salir):")
        
        user_api_key = input("API Key: ").strip()
        
        if not user_api_key:
            print("❌ Sin API key. Configúrala en .env y vuelve a ejecutar")
            return False
        
        # Actualizar archivo .env
        env_content = f"""# OpenAI API Configuration
OPENAI_API_KEY={user_api_key}
OPENAI_MODEL=text-embedding-3-small
OPENAI_MAX_TOKENS=8000

# ChromaDB Configuration
CHROMADB_PERSIST_DIRECTORY=./data/embeddings/chromadb
CHROMADB_COLLECTION_NAME=spanish_properties

# Project Configuration
PROJECT_ROOT=./
DATA_PATH=./data/
LOGS_PATH=./logs/

# Embedding Configuration
EMBEDDING_BATCH_SIZE=100
EMBEDDING_DIMENSIONS=1536
MAX_DESCRIPTION_LENGTH=500
"""
        
        with open('.env', 'w') as f:
            f.write(env_content)
        
        print("✅ API key guardada en .env")
        
        # Recargar variables de entorno
        load_dotenv(override=True)
    
    print("✅ API key de OpenAI configurada")
    
    # Buscar archivos CSV
    csv_files = find_csv_files()
    
    if not csv_files:
        print("❌ No se encontraron archivos CSV de propiedades")
        print("💡 Archivos buscados:")
        print("   - habitaclia_*.csv")
        print("   - properties_*.csv") 
        print("   - *_properties_*.csv")
        print("\n📁 Directorios buscados: data/, data/raw/, ./")
        print("🚀 Ejecuta primero el scraper para obtener datos")
        return False
    
    print(f"\n📊 Archivos CSV encontrados:")
    for i, csv_file in enumerate(csv_files):
        print(f"   {i+1}. {csv_file}")
    
    # Seleccionar archivo
    while True:
        try:
            choice = input(f"\nSelecciona un archivo (1-{len(csv_files)}): ")
            if not choice:
                print("❌ Operación cancelada")
                return False
            selected_file = csv_files[int(choice) - 1]
            break
        except (ValueError, IndexError):
            print("❌ Selección inválida")
    
    print(f"📁 Archivo seleccionado: {selected_file}")
    
    # Verificar que el archivo tiene datos
    try:
        import pandas as pd
        df = pd.read_csv(selected_file)
        print(f"📊 Propiedades en archivo: {len(df)}")
        
        if len(df) == 0:
            print("❌ El archivo está vacío")
            return False
            
    except Exception as e:
        print(f"❌ Error leyendo archivo: {e}")
        return False
    
    # Configurar sistema
    try:
        print("\n🚀 Iniciando configuración del sistema...")
        print("⏱️  Esto puede tardar varios minutos...")
        print("💰 Costo estimado: $0.01-0.05 USD")
        
        # Confirmar antes de proceder
        confirm = input("\n¿Continuar con la creación de embeddings? (y/N): ")
        if confirm.lower() != 'y':
            print("❌ Operación cancelada")
            return False
        
        # Importar y configurar
        from habitaclia.search.search_engine import setup_search_system
        
        search_engine = setup_search_system(str(selected_file))
        
        if search_engine:
            print("\n🎉 ¡Sistema configurado exitosamente!")
            
            # Mostrar estadísticas
            stats = search_engine.get_system_stats()
            print(f"\n📊 Estadísticas del sistema:")
            print(f"   🏠 Propiedades indexadas: {stats['database_stats']['total_properties']}")
            print(f"   🏙️  Ciudades disponibles: {stats['database_stats']['total_cities']}")
            print(f"   💰 Costo estimado: ${stats['embedding_stats']['total_cost_usd']:.4f}")
            
            # Ejecutar búsqueda de prueba
            print(f"\n🔍 Ejecutando búsqueda de prueba...")
            
            test_results = search_engine.search(
                query="piso moderno con mucha luz",
                n_results=3
            )
            
            print(f"   ✅ Resultados encontrados: {len(test_results['properties'])}")
            
            if test_results['properties']:
                top_result = test_results['properties'][0]
                print(f"   🏆 Top resultado: {top_result['metadata']['title'][:50]}...")
                print(f"       📍 {top_result['metadata']['city_name']}")
                print(f"       🎯 Similaridad: {top_result['similarity_score']:.3f}")
            
            print(f"\n✅ Sistema listo para búsquedas semánticas!")
            return True
        else:
            print("❌ Error configurando el sistema")
            return False
            
    except Exception as e:
        print(f"❌ Error durante la configuración: {e}")
        print(f"💡 Asegúrate de que todos los archivos estén en su lugar")
        return False

def main():
    """Función principal"""
    
    print("🏠 SETUP AUTOMÁTICO - SISTEMA DE EMBEDDINGS HABITACLIA")
    print("=" * 65)
    
    # Verificar dependencias
    if not check_requirements():
        print("\n💡 Para instalar las dependencias faltantes:")
        print("pip install openai chromadb python-dotenv")
        return
    
    # Configurar entorno
    setup_environment()
    
    # Configuración interactiva
    print("\n" + "=" * 65)
    success = interactive_setup()
    
    if success:
        print(f"\n🎯 PRÓXIMOS PASOS:")
        print(f"1. Ejecuta búsquedas usando el motor de búsqueda")
        print(f"2. Revisa los notebooks en /notebooks/ para análisis")
        print(f"3. Usa la API de búsqueda para integrar en aplicaciones")
        
        print(f"\n📚 EJEMPLO DE USO:")
        print(f"```python")
        print(f"from src.habitaclia.search.search_engine import PropertySearchEngine")
        print(f"")
        print(f"engine = PropertySearchEngine()")
        print(f"results = engine.search('piso luminoso cerca del mar')")
        print(f"```")
    else:
        print(f"\n❌ Configuración incompleta")
        print(f"💡 Revisa los errores anteriores y vuelve a intentar")

if __name__ == "__main__":
    main()