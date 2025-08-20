"""
Sistema de búsqueda semántica completo para propiedades inmobiliarias
Combina OpenAI embeddings con ChromaDB para búsquedas naturales
"""

import os
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union
import logging
import json
from datetime import datetime
import openai
from dotenv import load_dotenv

from .embedding_generator import PropertyEmbeddingGenerator
from .chromadb_manager import PropertyVectorDB

# Cargar variables de entorno
load_dotenv()

class PropertySearchEngine:
    """Motor de búsqueda semántica para propiedades inmobiliarias"""
    
    def __init__(self, collection_name: Optional[str] = None):
        """
        Inicializa el motor de búsqueda
        
        Args:
            collection_name: Nombre de la colección de ChromaDB
        """
        
        # Configurar logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Inicializar componentes
        self.embedding_generator = PropertyEmbeddingGenerator()
        self.vector_db = PropertyVectorDB(collection_name=collection_name)
        
        self.logger.info("🚀 Motor de búsqueda inmobiliaria inicializado")
    
    def index_properties_from_csv(self, csv_file: str, force_reindex: bool = False) -> Dict[str, Any]:
        """
        Indexa propiedades desde un archivo CSV
        
        Args:
            csv_file: Archivo CSV con propiedades
            force_reindex: Si True, reinicia la colección antes de indexar
            
        Returns:
            Estadísticas del indexado
        """
        
        self.logger.info(f"📁 Iniciando indexado de: {csv_file}")
        
        # Verificar si ya existe contenido
        stats = self.vector_db.get_collection_stats()
        if stats.get('total_properties', 0) > 0 and not force_reindex:
            self.logger.info(f"⚠️ La colección ya tiene {stats['total_properties']} propiedades")
            response = input("¿Quieres añadir más propiedades (a) o reiniciar la colección (r)? [a/r]: ")
            if response.lower() == 'r':
                force_reindex = True
        
        if force_reindex:
            self.logger.info("🔄 Reiniciando colección...")
            self.vector_db.reset_collection()
        
        try:
            # Paso 1: Generar embeddings
            self.logger.info("🧠 Generando embeddings...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            embeddings_file = f"properties_with_embeddings_{timestamp}.pkl"
            
            df_with_embeddings = self.embedding_generator.process_properties_dataset(
                csv_file, 
                embeddings_file
            )
            
            # Paso 2: Indexar en ChromaDB
            self.logger.info("💾 Indexando en ChromaDB...")
            self.vector_db.add_properties_batch(df_with_embeddings)
            
            # Estadísticas finales
            final_stats = self.vector_db.get_collection_stats()
            embedding_stats = self.embedding_generator.get_statistics()
            
            result = {
                'indexing_completed': True,
                'total_properties_indexed': final_stats.get('total_properties', 0),
                'cities_available': final_stats.get('cities_available', []),
                'embeddings_created': embedding_stats['embeddings_created'],
                'tokens_used': embedding_stats['total_tokens_used'],
                'cost_usd': embedding_stats['total_cost_usd'],
                'embeddings_file': embeddings_file
            }
            
            self.logger.info("✅ Indexado completado exitosamente")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Error durante el indexado: {e}")
            return {'indexing_completed': False, 'error': str(e)}
    
    def search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        n_results: int = 10,
        include_explanations: bool = True
    ) -> Dict[str, Any]:
        """
        Realiza búsqueda semántica de propiedades
        
        Args:
            query: Consulta en lenguaje natural
            filters: Filtros adicionales (precio, ciudad, habitaciones, etc.)
            n_results: Número máximo de resultados
            include_explanations: Si incluir explicaciones de relevancia
            
        Returns:
            Resultados de búsqueda estructurados
        """
        
        self.logger.info(f"🔍 Búsqueda: '{query}'")
        
        try:
            # Realizar búsqueda en ChromaDB
            results = self.vector_db.search_properties(
                query_text=query,
                n_results=n_results,
                filters=filters
            )
            
            # Enriquecer resultados si se solicita
            if include_explanations and results['properties']:
                results = self._add_explanations(results, query)
            
            # Añadir metadata de búsqueda
            results['search_metadata'] = {
                'timestamp': datetime.now().isoformat(),
                'filters_applied': filters or {},
                'results_count': len(results['properties']),
                'query_processed': query
            }
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Error en búsqueda: {e}")
            return {
                'query': query,
                'error': str(e),
                'properties': [],
                'search_metadata': {'error': True}
            }
    
    def _add_explanations(self, results: Dict[str, Any], query: str) -> Dict[str, Any]:
        """Añade explicaciones de por qué cada propiedad es relevante"""
        
        for prop in results['properties']:
            explanation_parts = []
            
            # Analizar similaridad
            score = prop['similarity_score']
            if score > 0.8:
                explanation_parts.append("Muy alta coincidencia semántica")
            elif score > 0.6:
                explanation_parts.append("Alta relevancia para tu búsqueda")
            elif score > 0.4:
                explanation_parts.append("Coincidencia moderada")
            else:
                explanation_parts.append("Coincidencia básica")
            
            # Información clave
            metadata = prop['metadata']
            if metadata.get('price', 0) > 0:
                explanation_parts.append(f"Precio: {metadata['price']}€")
            if metadata.get('rooms', 0) > 0:
                explanation_parts.append(f"{metadata['rooms']} habitaciones")
            if metadata.get('area_m2', 0) > 0:
                explanation_parts.append(f"{metadata['area_m2']}m²")
            
            prop['explanation'] = " | ".join(explanation_parts)
        
        return results
    
    def get_search_suggestions(self, partial_query: str) -> List[str]:
        """
        Genera sugerencias de búsqueda basadas en consulta parcial
        
        Args:
            partial_query: Consulta parcial del usuario
            
        Returns:
            Lista de sugerencias de búsqueda
        """
        
        # Plantillas de búsqueda comunes
        templates = [
            "piso moderno en {city} con vistas",
            "apartamento económico cerca del centro",
            "casa familiar con jardín para niños",
            "ático con terraza y mucha luz",
            "loft industrial con espacios abiertos",
            "estudio para estudiante bien comunicado",
            "duplex con parking incluido",
            "piso reformado listo para entrar"
        ]
        
        # Obtener ciudades disponibles
        stats = self.vector_db.get_collection_stats()
        cities = stats.get('cities_available', [])
        
        suggestions = []
        
        # Sugerencias basadas en ciudades
        if partial_query.lower() in ['madrid', 'barcelona', 'valencia']:
            city_suggestions = [
                f"piso moderno en {partial_query}",
                f"apartamento céntrico {partial_query}",
                f"casa familiar {partial_query}"
            ]
            suggestions.extend(city_suggestions)
        
        # Sugerencias generales
        if len(partial_query) > 3:
            general_suggestions = [
                f"{partial_query} cerca del mar",
                f"{partial_query} bien comunicado",
                f"{partial_query} con parking",
                f"{partial_query} luminoso"
            ]
            suggestions.extend(general_suggestions)
        
        return suggestions[:5]  # Limitar a 5 sugerencias
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas completas del sistema"""
        
        vector_stats = self.vector_db.get_collection_stats()
        embedding_stats = self.embedding_generator.get_statistics()
        
        return {
            'database_stats': vector_stats,
            'embedding_stats': embedding_stats,
            'system_ready': vector_stats.get('total_properties', 0) > 0,
            'search_capabilities': {
                'semantic_search': True,
                'filtered_search': True,
                'multi_city_search': True,
                'natural_language': True
            }
        }
    
    def search_with_examples(self) -> Dict[str, Any]:
        """Ejecuta búsquedas de ejemplo para demostrar capacidades"""
        
        example_queries = [
            {
                'query': "piso moderno con mucha luz cerca del mar",
                'filters': {'max_price': 2000},
                'description': "Búsqueda semántica con filtro de precio"
            },
            {
                'query': "casa familiar con jardín para niños",
                'filters': {'min_rooms': 3},
                'description': "Búsqueda conceptual con filtro de habitaciones"
            },
            {
                'query': "apartamento céntrico bien comunicado",
                'filters': {'city': 'barcelona'},
                'description': "Búsqueda por ubicación y características"
            }
        ]
        
        results = {'examples': []}
        
        for example in example_queries:
            self.logger.info(f"🔍 Ejemplo: {example['query']}")
            
            search_result = self.search(
                query=example['query'],
                filters=example['filters'],
                n_results=3
            )
            
            results['examples'].append({
                'query': example['query'],
                'filters': example['filters'],
                'description': example['description'],
                'results_count': len(search_result['properties']),
                'top_result': search_result['properties'][0] if search_result['properties'] else None
            })
        
        return results

# Función principal para configuración rápida
def setup_search_system(csv_file: str, collection_name: str = None) -> PropertySearchEngine:
    """
    Configuración rápida del sistema de búsqueda
    
    Args:
        csv_file: Archivo CSV con propiedades
        collection_name: Nombre de la colección
        
    Returns:
        Motor de búsqueda configurado
    """
    
    search_engine = PropertySearchEngine(collection_name=collection_name)
    
    # Indexar propiedades
    indexing_result = search_engine.index_properties_from_csv(csv_file)
    
    if indexing_result.get('indexing_completed'):
        print(f"✅ Sistema configurado exitosamente:")
        print(f"   📊 {indexing_result['total_properties_indexed']} propiedades indexadas")
        print(f"   🏙️ {len(indexing_result['cities_available'])} ciudades disponibles")
        print(f"   💰 Costo: ${indexing_result['cost_usd']:.4f}")
        
        return search_engine
    else:
        print(f"❌ Error configurando sistema: {indexing_result.get('error')}")
        return None

if __name__ == "__main__":
    # Ejemplo de uso completo
    import sys
    
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        
        print("🏠 CONFIGURANDO SISTEMA DE BÚSQUEDA INMOBILIARIA")
        print("=" * 60)
        
        # Configurar sistema
        search_engine = setup_search_system(csv_file)
        
        if search_engine:
            print("\n🔍 EJECUTANDO BÚSQUEDAS DE EJEMPLO")
            print("=" * 60)
            
            # Ejecutar ejemplos
            examples = search_engine.search_with_examples()
            
            for example in examples['examples']:
                print(f"\n📝 {example['description']}")
                print(f"   Query: '{example['query']}'")
                print(f"   Filtros: {example['filters']}")
                print(f"   Resultados: {example['results_count']}")
                
                if example['top_result']:
                    top = example['top_result']
                    print(f"   Top: {top['metadata']['title']} ({top['similarity_score']:.3f})")
            
            print(f"\n✅ Sistema listo para búsquedas interactivas!")
            
    else:
        print("Uso: python search_engine.py <archivo_propiedades.csv>")
        print("\nEjemplo:")
        print("python search_engine.py data/habitaclia_multicities_20250819_164523.csv")