import sqlite3
DB_NAME = "atlys.db"
historical_load_table = "historical_stock_data"

conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

cur.execute(f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name = '{historical_load_table}' ")
if bool(cur.fetchone()[0]):
    
    cur.execute(f"""
                    SELECT
                    Date,
                    Company,
                    LAG(Volume) OVER (PARTITION BY Company ORDER BY DATE) - Volume AS DailyVolumeChange
                FROM
                    {historical_load_table} 
                ORDER BY
                    Date Desc, Company 
    """)

    rows = cur.fetchall()

    for row in rows:
        print(row)

cur.close()
print("completed")