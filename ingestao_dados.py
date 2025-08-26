# Bibliotecas
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# Carrega variáveis de ambiente
load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "data/raw"))
DDL_PATH = Path("ddl_olist.sql")   # opcional: DDL completo aqui

url = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", ""),
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5433")),
    database=os.getenv("DB_NAME", "olist"),
)
engine = create_engine(url, pool_pre_ping=True)

