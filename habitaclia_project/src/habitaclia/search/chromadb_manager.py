"""
Manager para ChromaDB - Base de datos vectorial para búsquedas semánticas
"""

import os
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union
import logging
import json
from datetime import datetime
import chromadb
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class PropertyVectorDB:
    """Administrador de base de datos vectorial para propiedades inmobiliarias"""
    
    def __init__(self, persist_directory: Optional[str] = None, collection_name: Optional[str] = None):
        """
        Inicializa la conexión a ChromaDB
        
        Args:
            persist_directory: Directorio para persistir datos
            collection_name: Nombre de la colección
        """
        
        # Configuración
        self.persist_directory = persist_directory or os.getenv('CHROMADB_PERSIST_DIRECTORY', './data/embeddings/chromadb')
        self.collection_name = collection_name or os.getenv('CHROMADB_COLLECTION_NAME', 'spanish_properties')
        
        # Asegurar que el directorio existe
        os.makedirs(self.persist_directory, exist_ok=True)
        
        # Configurar logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Inicializar ChromaDB
        self.client = chromadb.PersistentClient(path=self.persist_directory)
        
        # Crear o obtener colección
        try:
            self.collection = self.client.create_collection(
                name=self.collection_name,
                metadata={"description": "Spanish real estate properties with semantic embeddings"}
            )
            self.logger.info(f"✅ Colección '{self.collection_name}' creada")
        except Exception:
            # La colección ya existe
            self.collection = self.client.get_collection(name=self.collection_name)
            self.logger.info(f"📂 Colección '{self.collection_name}' cargada ({self.collection.count()} propiedades)")
    
    def add_properties_batch(self, df: pd.DataFrame, batch_size: int = 100) -> None:
        """
        Añade propiedades con embeddings a la base de datos vectorial
        
        Args:
            df: DataFrame con propiedades y embeddings
            batch_size: Tamaño del lote para inserción
        """
        
        # Filtrar solo propiedades con embeddings válidos
        valid_df = df[df['has_embedding'] == True].copy()
        
        if len(valid_df) == 0:
            self.logger.warning("⚠️ No hay propiedades con embeddings válidos")
            return
        
        self.logger.info(f"🚀 Añadiendo {len(valid_df)} propiedades a ChromaDB...")
        
        # Procesar en lotes
        total_batches = (len(valid_df) + batch_size - 1) // batch_size
        
        for i in range(0, len(valid_df), batch_size):
            batch_df = valid_df.iloc[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            # Preparar datos para ChromaDB
            ids = [f"prop_{row['city_code']}_{idx}" for idx, row in batch_df.iterrows()]
            embeddings = [embedding for embedding in batch_df['embedding'] if embedding is not None]
            
            # Metadatos
            metadatas = []
            documents = []
            
            for _, row in batch_df.iterrows():
                # Documento (texto de búsqueda)
                documents.append(row.get('embedding_text', ''))
                
                # Metadatos (para filtros)
                metadata = {
                    'city_name': str(row.get('city_name', '')),
                    'city_code': str(row.get('city_code', '')),
                    'title': str(row.get('title', ''))[:100],  # Truncar título largo
                    'location': str(row.get('location', '')),
                    'price': float(row.get('price', 0)) if pd.notna(row.get('price')) else 0.0,
                    'rooms': int(row.get('rooms', 0)) if pd.notna(row.get('rooms')) else 0,
                    'bathrooms': int(row.get('bathrooms', 0)) if pd.notna(row.get('bathrooms')) else 0,
                    'area_m2': float(row.get('area_m2', 0)) if pd.notna(row.get('area_m2')) else 0.0,
                    'url': str(row.get('url', '')),
                    'timestamp': str(row.get('timestamp', ''))
                }
                metadatas.append(metadata)
            
            try:
                # Añadir lote a ChromaDB
                self.collection.add(
                    ids=ids,
                    embeddings=embeddings,
                    documents=documents,
                    metadatas=metadatas
                )
                
                self.logger.info(f"✅ Lote {batch_num}/{total_batches} añadido ({len(batch_df)} propiedades)")
                
            except Exception as e:
                self.logger.error(f"❌ Error en lote {batch_num}: {e}")
                continue
        
        final_count = self.collection.count()
        self.logger.info(f"🎉 Completado: {final_count} propiedades total en ChromaDB")
    
    def search_properties(
        self,
        query_text: str,
        n_results: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        embedding_function=None
    ) -> Dict[str, Any]:
        """
        Busca propiedades usando búsqueda semántica
        
        Args:
            query_text: Texto de búsqueda
            n_results: Número de resultados a retornar
            filters: Filtros adicionales (precio, ciudad, etc.)
            embedding_function: Función para generar embedding de la query
            
        Returns:
            Resultados de búsqueda con propiedades relevantes
        """
        
        # Construir filtros de ChromaDB
        where_clause = {}
        if filters:
            if 'city' in filters:
                where_clause['city_code'] = filters['city']
            if 'min_price' in filters:
                where_clause['price'] = {"$gte": filters['min_price']}
            if 'max_price' in filters:
                if 'price' in where_clause:
                    where_clause['price']["$lte"] = filters['max_price']
                else:
                    where_clause['price'] = {"$lte": filters['max_price']}
            if 'min_rooms' in filters:
                where_clause['rooms'] = {"$gte": filters['min_rooms']}
        
        try:
            # Realizar búsqueda
            if where_clause:
                results = self.collection.query(
                    query_texts=[query_text],
                    n_results=n_results,
                    where=where_clause
                )
            else:
                results = self.collection.query(
                    query_texts=[query_text],
                    n_results=n_results
                )
            
            # Formatear resultados
            formatted_results = {
                'query': query_text,
                'filters_applied': filters or {},
                'total_results': len(results['ids'][0]),
                'properties': []
            }
            
            # Procesar cada resultado
            for i in range(len(results['ids'][0])):
                property_result = {
                    'id': results['ids'][0][i],
                    'similarity_score': float(1 - results['distances'][0][i]),  # Convertir distancia a similaridad
                    'metadata': results['metadatas'][0][i],
                    'document': results['documents'][0][i]
                }
                formatted_results['properties'].append(property_result)
            
            return formatted_results
            
        except Exception as e:
            self.logger.error(f"Error en búsqueda: {e}")
            return {
                'query': query_text,
                'error': str(e),
                'properties': []
            }
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de la colección"""
        try:
            count = self.collection.count()
            
            # Obtener muestra para estadísticas
            sample = self.collection.get(limit=100)
            
            # Análisis de metadatos
            cities = set()
            price_range = {'min': float('inf'), 'max': 0}
            rooms_range = {'min': float('inf'), 'max': 0}
            
            for metadata in sample['metadatas']:
                cities.add(metadata.get('city_name', ''))
                
                price = metadata.get('price', 0)
                if price > 0:
                    price_range['min'] = min(price_range['min'], price)
                    price_range['max'] = max(price_range['max'], price)
                
                rooms = metadata.get('rooms', 0)
                if rooms > 0:
                    rooms_range['min'] = min(rooms_range['min'], rooms)
                    rooms_range['max'] = max(rooms_range['max'], rooms)
            
            return {
                'total_properties': count,
                'cities_available': sorted(list(cities)),
                'total_cities': len(cities),
                'price_range': price_range if price_range['min'] != float('inf') else {'min': 0, 'max': 0},
                'rooms_range': rooms_range if rooms_range['min'] != float('inf') else {'min': 0, 'max': 0},
                'collection_name': self.collection_name
            }
            
        except Exception as e:
            self.logger.error(f"Error obteniendo estadísticas: {e}")
            return {'error': str(e)}
    
    def delete_collection(self) -> bool:
        """Elimina la colección completa"""
        try:
            self.client.delete_collection(name=self.collection_name)
            self.logger.info(f"🗑️ Colección '{self.collection_name}' eliminada")
            return True
        except Exception as e:
            self.logger.error(f"Error eliminando colección: {e}")
            return False
    
    def reset_collection(self) -> None:
        """Reinicia la colección (elimina y recrea)"""
        try:
            self.delete_collection()
            self.collection = self.client.create_collection(
                name=self.collection_name,
                metadata={"description": "Spanish real estate properties with semantic embeddings"}
            )
            self.logger.info(f"🔄 Colección '{self.collection_name}' reiniciada")
        except Exception as e:
            self.logger.error(f"Error reiniciando colección: {e}")

# Función de utilidad
def load_properties_to_chromadb(df_with_embeddings: pd.DataFrame, collection_name: str = None) -> PropertyVectorDB:
    """
    Función de conveniencia para cargar propiedades a ChromaDB
    
    Args:
        df_with_embeddings: DataFrame con embeddings
        collection_name: Nombre de la colección
        
    Returns:
        Instancia de PropertyVectorDB
    """
    
    vector_db = PropertyVectorDB(collection_name=collection_name)
    vector_db.add_properties_batch(df_with_embeddings)
    
    return vector_db

if __name__ == "__main__":
    # Ejemplo de uso
    vector_db = PropertyVectorDB()
    
    # Mostrar estadísticas
    stats = vector_db.get_collection_stats()
    print("📊 Estadísticas de ChromaDB:")
    print(json.dumps(stats, indent=2, ensure_ascii=False))
    
    # Ejemplo de búsqueda
    if stats.get('total_properties', 0) > 0:
        results = vector_db.search_properties(
            query_text="piso moderno con vistas al mar",
            n_results=5
        )
        
        print(f"\n🔍 Resultados de búsqueda:")
        for prop in results['properties'][:3]:
            print(f"- {prop['metadata']['title']} ({prop['similarity_score']:.3f})")