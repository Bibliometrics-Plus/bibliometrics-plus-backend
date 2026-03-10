from services.supabase_client import get_client

def fetch_table_preview(table_name: str, limit: int = 10):
    supabase = get_client()
    res = supabase.table(table_name).select("*").limit(limit).execute()
    return res.data