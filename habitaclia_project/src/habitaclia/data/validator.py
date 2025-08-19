"""Validación de calidad de datos extraídos"""
import pandas as pd
import logging
from typing import Dict, List, Tuple, Any

class PropertyDataValidator:
    """Valida la calidad y consistencia de los datos de propiedades"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.required_fields = ['title', 'url', 'city_name', 'timestamp']
        self.numeric_fields = ['price', 'rooms', 'bathrooms', 'area_m2']
        self.validation_rules = self._setup_validation_rules()
    
    def _setup_validation_rules(self) -> Dict:
        """Define reglas de validación"""
        return {
            'price': {'min': 50, 'max': 50000000, 'type': (int, float)},
            'rooms': {'min': 1, 'max': 20, 'type': int},
            'bathrooms': {'min': 1, 'max': 10, 'type': int},
            'area_m2': {'min': 10, 'max': 2000, 'type': int},
            'title': {'min_length': 10, 'max_length': 200},
            'location': {'min_length': 3, 'max_length': 100}
        }
    
    def validate_property(self, property_data: Dict) -> Tuple[bool, List[str]]:
        """Valida una propiedad individual"""
        errors = []
        
        # Verificar campos requeridos
        for field in self.required_fields:
            if field not in property_data or not property_data[field]:
                errors.append(f"Campo requerido faltante: {field}")
        
        # Validar campos numéricos
        for field in self.numeric_fields:
            if field in property_data and property_data[field] is not None:
                value = property_data[field]
                rules = self.validation_rules.get(field, {})
                
                # Tipo correcto
                if 'type' in rules and not isinstance(value, rules['type']):
                    errors.append(f"{field}: tipo incorrecto (esperado {rules['type'].__name__})")
                
                # Rango válido
                if isinstance(value, (int, float)):
                    if 'min' in rules and value < rules['min']:
                        errors.append(f"{field}: valor demasiado bajo ({value} < {rules['min']})")
                    if 'max' in rules and value > rules['max']:
                        errors.append(f"{field}: valor demasiado alto ({value} > {rules['max']})")
        
        # Validar campos de texto
        for field in ['title', 'location']:
            if field in property_data and property_data[field]:
                value = property_data[field]
                rules = self.validation_rules.get(field, {})
                
                if 'min_length' in rules and len(value) < rules['min_length']:
                    errors.append(f"{field}: demasiado corto")
                if 'max_length' in rules and len(value) > rules['max_length']:
                    errors.append(f"{field}: demasiado largo")
        
        return len(errors) == 0, errors
    
    def validate_dataset(self, properties_data: List[Dict]) -> Dict:
        """Valida un dataset completo"""
        if not properties_data:
            return {"error": "Dataset vacío"}
        
        total_properties = len(properties_data)
        valid_properties = 0
        all_errors = []
        
        # Validar cada propiedad
        for i, property_data in enumerate(properties_data):
            is_valid, errors = self.validate_property(property_data)
            
            if is_valid:
                valid_properties += 1
            else:
                all_errors.extend([f"Propiedad {i+1}: {error}" for error in errors])
        
        # Calcular métricas
        validity_rate = (valid_properties / total_properties) * 100
        
        # Análisis de completitud por campo
        df = pd.DataFrame(properties_data)
        completeness = {}
        for field in self.required_fields + self.numeric_fields:
            if field in df.columns:
                non_null_count = df[field].notna().sum()
                completeness[field] = (non_null_count / total_properties) * 100
        
        # Detectar duplicados
        duplicate_urls = df['url'].duplicated().sum() if 'url' in df.columns else 0
        
        report = {
            'total_properties': total_properties,
            'valid_properties': valid_properties,
            'validity_rate': round(validity_rate, 2),
            'completeness_by_field': {k: round(v, 2) for k, v in completeness.items()},
            'duplicate_urls': duplicate_urls,
            'error_count': len(all_errors),
            'sample_errors': all_errors[:10],  # Primeros 10 errores
            'recommendations': self._generate_recommendations(validity_rate, completeness, duplicate_urls)
        }
        
        return report
    
    def _generate_recommendations(self, validity_rate: float, completeness: Dict, duplicates: int) -> List[str]:
        """Genera recomendaciones basadas en la validación"""
        recommendations = []
        
        if validity_rate < 80:
            recommendations.append("❌ Baja tasa de validez (<80%). Revisar extracción de datos")
        
        if validity_rate >= 95:
            recommendations.append("✅ Excelente calidad de datos (>95% válidos)")
        
        for field, percentage in completeness.items():
            if percentage < 70:
                recommendations.append(f"⚠️  Campo '{field}': baja completitud ({percentage:.1f}%)")
            elif percentage > 95:
                recommendations.append(f"✅ Campo '{field}': excelente completitud ({percentage:.1f}%)")
        
        if duplicates > 0:
            recommendations.append(f"🔄 {duplicates} URLs duplicadas detectadas - considerar deduplicación")
        
        if not recommendations:
            recommendations.append("✅ No se detectaron problemas importantes")
        
        return recommendations
    
    def clean_dataset(self, properties_data: List[Dict]) -> Tuple[List[Dict], Dict]:
        """Limpia el dataset eliminando duplicados y datos inválidos"""
        df = pd.DataFrame(properties_data)
        original_count = len(df)
        
        # Remover duplicados por URL
        if 'url' in df.columns:
            df = df.drop_duplicates(subset=['url'], keep='first')
        
        # Convertir de vuelta a lista de diccionarios
        cleaned_data = df.to_dict('records')
        
        # Filtrar propiedades válidas
        valid_properties = []
        for prop in cleaned_data:
            is_valid, _ = self.validate_property(prop)
            if is_valid:
                valid_properties.append(prop)
        
        cleaning_report = {
            'original_count': original_count,
            'after_deduplication': len(cleaned_data),
            'after_validation': len(valid_properties),
            'duplicates_removed': original_count - len(cleaned_data),
            'invalid_removed': len(cleaned_data) - len(valid_properties)
        }
        
        return valid_properties, cleaning_report
