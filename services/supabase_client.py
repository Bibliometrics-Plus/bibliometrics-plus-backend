import os
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client, Client

# Force-load the .env file from the repo root (one level above /app and /services)
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

def get_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError(f"Missing SUPABASE_URL or SUPABASE_KEY in .env (loaded from: {ENV_PATH})")

    return create_client(url, key)