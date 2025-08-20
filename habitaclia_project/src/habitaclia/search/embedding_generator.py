"""
Generador de embeddings para propiedades inmobiliarias usando OpenAI API
"""

import os
import time
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
import logging
import json
from datetime import datetime
import openai
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class PropertyEmbeddingGenerator:
    """Genera embeddings semánticos para propiedades inmobiliarias"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Inicializa el generador de embeddings
        
        Args:
            api_key: OpenAI API key. Si no se proporciona, se lee de .env
        """
        
        # Configurar OpenAI
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key no encontrada. Añádela al archivo .env")
        
        self.client = openai.OpenAI(api_key=self.api_key)
        
        # Configuración
        self.model = os.getenv('OPENAI_MODEL', 'text-embedding-3-small')
        self.batch_size = int(os.getenv('EMBEDDING_BATCH_SIZE', 100))
        self.max_description_length = int(os.getenv('MAX_DESCRIPTION_LENGTH', 500))
        
        # Configurar logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Estadísticas
        self.total_tokens_used = 0
        self.total_cost = 0.0
        self.embeddings_created = 0
        
    def prepare_property_text(self, property_data: Dict[str, Any]) -> str:
        """
        Prepara el texto descriptivo completo de una propiedad para embedding
        
        Args:
            property_data: Datos de la propiedad
            
        Returns:
            Texto combinado y optimizado para embedding
        """
        
        components = []
        
        # Título (peso alto)
        title = property_data.get('title', '').strip()
        if title:
            components.append(f"Propiedad: {title}")
        
        # Ubicación (peso alto)
        city = property_data.get('city_name', '').strip()
        location = property_data.get('location', '').strip()
        if city:
            components.append(f"Ciudad: {city}")
        if location:
            components.append(f"Zona: {location}")
        
        # Características técnicas
        rooms = property_data.get('rooms')
        if rooms and rooms > 0:
            components.append(f"{rooms} habitaciones")
            
        bathrooms = property_data.get('bathrooms')
        if bathrooms and bathrooms > 0:
            components.append(f"{bathrooms} baños")
            
        area = property_data.get('area_m2')
        if area and area > 0:
            components.append(f"{area} metros cuadrados")
        
        # Precio
        price = property_data.get('price')
        if price and price > 0:
            components.append(f"Precio: {price}€")
        
        # Descripción (truncada si es muy larga)
        description = property_data.get('description', '').strip()
        if description:
            if len(description) > self.max_description_length:
                description = description[:self.max_description_length] + "..."
            components.append(f"Descripción: {description}")
        
        # Combinar todos los componentes
        full_text = " | ".join(components)
        
        return full_text
    
    def create_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Crea embeddings para un lote de textos
        
        Args:
            texts: Lista de textos para procesar
            
        Returns:
            Lista de vectores de embedding
        """
        
        try:
            response = self.client.embeddings.create(
                model=self.model,
                input=texts
            )
            
            # Extraer embeddings
            embeddings = [embedding.embedding for embedding in response.data]
            
            # Actualizar estadísticas
            self.total_tokens_used += response.usage.total_tokens
            
            # Calcular costo (aproximado para text-embedding-3-small)
            cost_per_1k_tokens = 0.00002  # $0.00002 per 1K tokens
            batch_cost = (response.usage.total_tokens / 1000) * cost_per_1k_tokens
            self.total_cost += batch_cost
            
            self.embeddings_created += len(embeddings)
            
            self.logger.info(f"Batch procesado: {len(embeddings)} embeddings, {response.usage.total_tokens} tokens")
            
            return embeddings
            
        except Exception as e:
            self.logger.error(f"Error creando embeddings: {e}")
            raise
    
    def process_properties_dataset(self, csv_file_path: str, output_file: Optional[str] = None) -> pd.DataFrame:
        """
        Procesa un dataset completo de propiedades y genera embeddings
        
        Args:
            csv_file_path: Ruta al archivo CSV con propiedades
            output_file: Archivo de salida (opcional)
            
        Returns:
            DataFrame con embeddings añadidos
        """
        
        self.logger.info(f"🚀 Iniciando procesamiento de embeddings: {csv_file_path}")
        
        # Cargar datos
        try:
            df = pd.read_csv(csv_file_path)
            self.logger.info(f"📊 Dataset cargado: {len(df)} propiedades")
        except Exception as e:
            self.logger.error(f"Error cargando dataset: {e}")
            raise
        
        # Preparar textos para embedding
        self.logger.info("📝 Preparando textos para embedding...")
        texts = []
        for _, row in df.iterrows():
            text = self.prepare_property_text(row.to_dict())
            texts.append(text)
        
        # Procesar en lotes
        all_embeddings = []
        total_batches = (len(texts) + self.batch_size - 1) // self.batch_size
        
        self.logger.info(f"🔄 Procesando {total_batches} lotes de {self.batch_size} propiedades...")
        
        for i in range(0, len(texts), self.batch_size):
            batch_texts = texts[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            
            self.logger.info(f"Procesando lote {batch_num}/{total_batches}...")
            
            try:
                batch_embeddings = self.create_embeddings_batch(batch_texts)
                all_embeddings.extend(batch_embeddings)
                
                # Rate limiting: pequeña pausa entre lotes
                if batch_num < total_batches:  # No hacer pausa en el último lote
                    time.sleep(1)
                    
            except Exception as e:
                self.logger.error(f"Error en lote {batch_num}: {e}")
                # Añadir embeddings vacíos para mantener la estructura
                all_embeddings.extend([None] * len(batch_texts))
        
        # Añadir embeddings al DataFrame
        df['embedding_text'] = texts
        df['embedding'] = all_embeddings
        df['has_embedding'] = df['embedding'].notna()
        
        # Estadísticas finales
        valid_embeddings = df['has_embedding'].sum()
        self.logger.info(f"✅ Procesamiento completado:")
        self.logger.info(f"   📊 Embeddings válidos: {valid_embeddings}/{len(df)}")
        self.logger.info(f"   🪙 Tokens utilizados: {self.total_tokens_used:,}")
        self.logger.info(f"   💰 Costo estimado: ${self.total_cost:.4f}")
        
        # Guardar resultado
        if output_file:
            # Guardar DataFrame completo como pickle (para preservar embeddings)
            df.to_pickle(output_file.replace('.csv', '.pkl'))
            
            # Guardar versión CSV sin embeddings para inspección
            df_csv = df.drop(['embedding'], axis=1)
            df_csv.to_csv(output_file, index=False)
            
            self.logger.info(f"💾 Datos guardados: {output_file}")
        
        return df
    
    def get_statistics(self) -> Dict[str, Any]:
        """Retorna estadísticas del procesamiento"""
        return {
            'embeddings_created': self.embeddings_created,
            'total_tokens_used': self.total_tokens_used,
            'total_cost_usd': self.total_cost,
            'model_used': self.model,
            'batch_size': self.batch_size
        }

# Función de utilidad para uso rápido
def create_embeddings_for_dataset(csv_file: str, output_file: str = None) -> pd.DataFrame:
    """
    Función de conveniencia para crear embeddings de un dataset
    
    Args:
        csv_file: Archivo CSV de propiedades
        output_file: Archivo de salida (opcional)
        
    Returns:
        DataFrame con embeddings
    """
    
    generator = PropertyEmbeddingGenerator()
    
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"properties_with_embeddings_{timestamp}.csv"
    
    return generator.process_properties_dataset(csv_file, output_file)

if __name__ == "__main__":
    # Ejemplo de uso
    import sys
    
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        
        print(f"🚀 Creando embeddings para: {csv_file}")
        df = create_embeddings_for_dataset(csv_file, output_file)
        print(f"✅ Completado: {len(df)} propiedades procesadas")
    else:
        print("Uso: python embedding_generator.py <archivo_csv> [archivo_salida]")