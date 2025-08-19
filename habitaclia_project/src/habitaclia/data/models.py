"""Modelos de datos para propiedades inmobiliarias"""
from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime

@dataclass
class Property:
    """Modelo de datos para una propiedad inmobiliaria"""
    url: str
    city_code: str
    city_name: str
    timestamp: str
    title: Optional[str] = None
    price: Optional[float] = None
    price_raw: Optional[str] = None
    location: Optional[str] = None
    rooms: Optional[int] = None
    bathrooms: Optional[int] = None
    area_m2: Optional[int] = None
    description: Optional[str] = None
    image_urls: List[str] = field(default_factory=list)
    image_count: int = 0
    status: str = "success"
    
    def to_dict(self) -> dict:
        """Convierte a diccionario para serialización"""
        return {
            'url': self.url,
            'city_code': self.city_code,
            'city_name': self.city_name,
            'timestamp': self.timestamp,
            'title': self.title,
            'price': self.price,
            'price_raw': self.price_raw,
            'location': self.location,
            'rooms': self.rooms,
            'bathrooms': self.bathrooms,
            'area_m2': self.area_m2,
            'description': self.description,
            'image_urls': self.image_urls,
            'image_count': self.image_count,
            'status': self.status
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Property':
        """Crea instancia desde diccionario"""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
    
    def is_valid(self) -> bool:
        """Verifica si la propiedad tiene datos mínimos válidos"""
        return (
            self.url and 
            self.city_name and 
            self.timestamp and
            (self.title or self.location)  # Al menos título o ubicación
        )
    
    def get_price_per_m2(self) -> Optional[float]:
        """Calcula precio por metro cuadrado"""
        if self.price and self.area_m2 and self.area_m2 > 0:
            return round(self.price / self.area_m2, 2)
        return None