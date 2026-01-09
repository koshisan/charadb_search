import psycopg2
import json
from config import DB_CONFIG

def check_schema():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'chub_character_def'")
    cols = [r[0] for r in cur.fetchall()]
    print("Columns in chub_character_def:", cols)
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_schema()
