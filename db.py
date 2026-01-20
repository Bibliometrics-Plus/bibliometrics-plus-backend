import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine

# Always load the .env that sits in the same folder as this file
BASE_DIR = Path(__file__).resolve().parent
dotenv_path = BASE_DIR / ".env"
load_dotenv(dotenv_path=dotenv_path)

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(f"DATABASE_URL is not set. Expected it in {dotenv_path}")

engine = create_engine(DATABASE_URL)
