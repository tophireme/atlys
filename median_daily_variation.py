import sqlite3
DB_NAME = "atlys.db"
historical_load_table = "historical_stock_data"

conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

cur.execute(f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name = '{historical_load_table}' ")
if bool(cur.fetchone()[0]):
    
    cur.execute(f"""
                SELECT Company,
                    AVG(DailyVariation) AS MedianDailyVariation
                FROM (
                    SELECT Company,
                        ABS(High - Low) AS DailyVariation,
                        ROW_NUMBER() OVER (PARTITION BY Company ORDER BY ABS(High - Low)) AS RowNum,
                        COUNT(*) OVER (PARTITION BY Company) AS TotalRows
                    FROM {historical_load_table}
                ) AS RankedData
                WHERE RowNum IN ((TotalRows + 1) / 2.0, (TotalRows + 2) / 2.0)
                GROUP BY Company
    """)

    rows = cur.fetchall()

    for row in rows:
        print(row)

cur.close()
print("completed")