# src/habitaclia/data/validator.py
import pandas as pd
from typing import Dict, List

class DataValidator:
    """Valida calidad de los datos extraídos"""
    
    def __init__(self):
        self.required_fields = ['title', 'price', 'location', 'url']
        self.numeric_fields = ['price', 'rooms', 'bathrooms', 'area_m2']
    
    def validate_dataframe(self, df: pd.DataFrame) -> Dict:
        """Valida un DataFrame completo"""
        report = {
            'total_rows': len(df),
            'valid_rows': 0,
            'missing_data': {},
            'invalid_data': {},
            'quality_score': 0.0
        }
        
        # Validar campos requeridos
        for field in self.required_fields:
            if field in df.columns:
                missing = df[field].isna().sum()
                report['missing_data'][field] = missing
        
        # Validar campos numéricos
        for field in self.numeric_fields:
            if field in df.columns:
                invalid = df[field].apply(lambda x: not isinstance(x, (int, float)) if pd.notna(x) else False).sum()
                report['invalid_data'][field] = invalid
        
        # Calcular score de calidad
        valid_rows = len(df) - max(report['missing_data'].values())
        report['valid_rows'] = valid_rows
        report['quality_score'] = (valid_rows / len(df)) * 100 if len(df) > 0 else 0
        
        return report
    
    def get_recommendations(self, report: Dict) -> List[str]:
        """Genera recomendaciones basadas en el reporte"""
        recommendations = []
        
        if report['quality_score'] < 80:
            recommendations.append("❌ Calidad de datos baja (<80%)")
        
        for field, missing in report['missing_data'].items():
            if missing > 0:
                recommendations.append(f"⚠️  Campo '{field}': {missing} valores faltantes")
        
        return recommendations