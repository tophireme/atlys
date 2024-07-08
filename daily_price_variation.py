import sqlite3
DB_NAME = "atlys.db"
historical_load_table = "historical_stock_data"

conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

cur.execute(f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name = '{historical_load_table}' ")
if bool(cur.fetchone()[0]):
    
    cur.execute(f"""SELECT
        Date,
        Company,
        Open AS OpeningPrice,
        Close AS ClosingPrice,
        High AS HighPrice,
        Low AS LowPrice,
        ROUND((Close - Open),2) AS DailyVariation,
        ROUND((High - Low),2) AS IntradayRange
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
