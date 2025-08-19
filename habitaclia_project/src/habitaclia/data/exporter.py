"""Exportación de datos a múltiples formatos"""
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path

class DataExporter:
    """Maneja exportación de datos a CSV y JSON"""
    
    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else Path("data/raw")
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def save_csv(self, properties_data: List[Dict], filename: Optional[str] = None):
        """Guarda datos en CSV con formato optimizado"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"habitaclia_multicities_{timestamp}.csv"
        
        filepath = self.output_dir / filename
        
        if properties_data:
            df = pd.DataFrame(properties_data)
            
            # Reordenar columnas con ciudad al principio
            column_order = [
                'city_name', 'city_code', 'title', 'price', 'location', 
                'rooms', 'bathrooms', 'area_m2', 'url', 'timestamp'
            ]
            
            existing_cols = df.columns.tolist()
            ordered_cols = [col for col in column_order if col in existing_cols]
            remaining_cols = [col for col in existing_cols if col not in ordered_cols]
            final_cols = ordered_cols + remaining_cols
            
            df = df[final_cols]
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            
            print(f"💾 Datos guardados en: {filepath}")
            print(f"📊 {len(df)} propiedades de {df['city_name'].nunique()} ciudades")
            
            # Mostrar distribución por ciudad
            if 'city_name' in df.columns:
                city_counts = df['city_name'].value_counts()
                print("🏙️  Distribución por ciudad:")
                for city, count in city_counts.head(10).items():
                    print(f"   {city}: {count} propiedades")
        else:
            print("⚠️  No hay datos para guardar")
    
    def save_json(self, properties_data: List[Dict], filename: Optional[str] = None):
        """Guarda datos en JSON con formato estructurado"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"habitaclia_multicities_{timestamp}.json"
        
        filepath = self.output_dir / filename
        
        if properties_data:
            # Estructura mejorada para JSON
            json_data = {
                "metadata": {
                    "total_properties": len(properties_data),
                    "extraction_date": datetime.now().isoformat(),
                    "cities": list(set(prop.get('city_name', '') for prop in properties_data)),
                    "version": "1.0"
                },
                "properties": properties_data
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            print(f"💾 Datos JSON guardados en: {filepath}")
        else:
            print("⚠️  No hay datos para guardar en JSON")
    
    def get_statistics(self, properties_data: List[Dict]) -> Dict:
        """Genera estadísticas de los datos extraídos"""
        if not properties_data:
            return {"error": "No hay datos para analizar"}
        
        df = pd.DataFrame(properties_data)
        
        stats = {
            'total_properties': len(df),
            'cities_scraped': df['city_name'].nunique() if 'city_name' in df.columns else 0,
            'properties_by_city': df['city_name'].value_counts().to_dict() if 'city_name' in df.columns else {},
            'data_quality': self._calculate_data_quality(df),
        }
        
        # Estadísticas de precios
        if 'price' in df.columns and df['price'].notna().any():
            valid_prices = df['price'].dropna()
            stats['price_stats'] = {
                'avg_price': float(valid_prices.mean()),
                'min_price': float(valid_prices.min()),
                'max_price': float(valid_prices.max()),
                'median_price': float(valid_prices.median())
            }
        
        # Estadísticas de área
        if 'area_m2' in df.columns and df['area_m2'].notna().any():
            valid_areas = df['area_m2'].dropna()
            stats['area_stats'] = {
                'avg_area': float(valid_areas.mean()),
                'min_area': float(valid_areas.min()),
                'max_area': float(valid_areas.max())
            }
        
        return stats
    
    def _calculate_data_quality(self, df: pd.DataFrame) -> Dict:
        """Calcula métricas de calidad de datos"""
        total_rows = len(df)
        if total_rows == 0:
            return {}
        
        quality_metrics = {}
        essential_fields = ['title', 'price', 'location']
        
        for field in essential_fields:
            if field in df.columns:
                non_null_count = df[field].notna().sum()
                completeness = (non_null_count / total_rows) * 100
                quality_metrics[f'{field}_completeness'] = round(completeness, 2)
        
        # Score global de calidad
        completeness_scores = [v for k, v in quality_metrics.items() if k.endswith('_completeness')]
        overall_quality = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0
        quality_metrics['overall_quality_score'] = round(overall_quality, 2)
        
        return quality_metrics
