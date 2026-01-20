import os
from dotenv import load_dotenv

# Load .env from the current folder
load_dotenv()

value = os.getenv("DATABASE_URL")
print("DATABASE_URL =", repr(value))
