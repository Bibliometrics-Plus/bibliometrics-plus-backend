from db import engine
from sqlalchemy import text   
if __name__ == "__main__":
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))   
        print({"db_ok": True})
    except Exception as e:
        print({"db_ok": False, "error": str(e)})

