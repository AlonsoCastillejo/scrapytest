"""Setup para instalación del package Habitaclia Scraper"""
from setuptools import setup, find_packages
from pathlib import Path

# Leer README
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')

# Leer requirements
requirements = (this_directory / "requirements.txt").read_text().strip().split('\n')

setup(
    name="habitaclia-scraper",
    version="0.1.0",
    description="Sistema modular de scraping inmobiliario para investigación académica",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Tu Nombre",
    author_email="tu@email.com",
    url="https://github.com/tu-usuario/habitaclia-scraper",
    
    # Packages y estructura
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    
    # Dependencias
    install_requires=requirements,
    python_requires=">=3.8",
    
    # Scripts ejecutables
    entry_points={
        'console_scripts': [
            'habitaclia-scraper=scripts.run_scraper:main',
        ],
    },
    
    # Metadata adicional
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    
    # Archivos adicionales
    include_package_data=True,
    package_data={
        'habitaclia': ['config/*.yaml', 'config/*.json'],
    },
    
    # Extras opcionales
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'pytest-cov>=4.0.0',
            'black>=23.0.0',
            'flake8>=6.0.0',
        ],
        'analysis': [
            'jupyter>=1.0.0',
            'matplotlib>=3.7.0',
            'seaborn>=0.12.0',
            'plotly>=5.15.0',
        ]
    }
)