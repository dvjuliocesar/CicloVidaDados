# Bibliotecas
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

# Carrega variáveis de ambiente
load_dotenv(override=False)

# Configuração do banco de dados
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgresql'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', ''),
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', '')
}
