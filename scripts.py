# %%
import sqlite3
import requests
import pandas as pd
from datetime import datetime
import concurrent.futures
import inspect

API_KEY = "N7VK1COZY7YSAKOK"

BSE_TOP_10 = [
    ("Reliance Industries", "RELIANCE"),
    ("Tata Consultancy Services", "TCS"),
    ("HDFC Bank", "HDFCBANK"),
    ("ICICI Bank", "ICICIBANK"),
    ("Bharti Airtel", "BHARTIARTL"),
    ("State Bank of India", "SBIN"),
    ("Infosys", "INFY"),
    ("Life Insurance Corporation of India","LICI"),
    ("Hindustan Unilever", "HINDUNILVR"),
    ("Indian Tobacco Company", "ITC")
]

DB_NAME = "atlys.db"
historical_load_table = "historical_stock_data"
yesterday_dump_table = "yesterday_stock_data"

# %%
def initialise_Database():
    try:
        conn = sqlite3.connect(DB_NAME, timeout=10)
        cursor = conn.cursor()

        table_names = [historical_load_table, yesterday_dump_table]

        for table in table_names:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    Date DATE,
                    Company VARCHAR(255),
                    Company_Symbol VARCHAR(255),
                    Open REAL,
                    Close REAL,
                    High REAL,
                    Low REAL,
                    Volume BIGINT,
                    PRIMARY KEY (Date, Company)
                )
            """)

        cursor.execute(f'CREATE INDEX IF NOT EXISTS index_company_date ON {historical_load_table} (Company, Date)')
        cursor.execute(f'CREATE INDEX IF NOT EXISTS index_company ON {yesterday_dump_table} (Company)')
        conn.commit()

    except Exception as e:
        print(f"{e} error in {inspect.currentframe().f_code.co_name}")

    finally:
        if conn:
            conn.close() 


# %%
def get_api_data(company_info,outputsize):
    try:
        company_name, company_symbol = company_info
        response = requests.get(f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={company_symbol}.BSE&outputsize={outputsize}&apikey={API_KEY}')
        data = response.json()
        time_series = data["Time Series (Daily)"]
        df = pd.DataFrame(time_series).T.reset_index()
        df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
        df["Company"] = company_name
        df["Company_Symbol"] = company_symbol
        df = df[["Date", "Company","Company_Symbol", "Open", "Close", "High", "Low", "Volume"]]
        df['Date'] = pd.to_datetime(df['Date'])
        df.drop_duplicates(inplace=True)
        return df
    except Exception as e:
        print(f"{response.json()} error in {inspect.currentframe().f_code.co_name} while fetching for {company_name}")
        return None

# %%
def daily_load_company(company_info):
    company_name, company_symbol = company_info
    try:
        con = sqlite3.connect(DB_NAME, timeout=10)
        cur = con.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {yesterday_dump_table} ")
        initialise_Database()
        outputsize='compact'
        df = get_api_data(company_info,outputsize)
        df = df.head(1)
        
        return company_name,company_symbol, df,yesterday_dump_table

    except Exception as e:
        print(f"{e} error in {inspect.currentframe().f_code.co_name} while fetching for {company_name}")
        return company_name,company_symbol, None,yesterday_dump_table

# %%
def historical_company_data(company_info):

    company_name, company_symbol = company_info
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 5, 31)

    try:
        con = sqlite3.connect(DB_NAME, timeout=10)
        cur = con.cursor()
        cur.execute(f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name = '{historical_load_table}' ")
        historical_load_table_exists = bool(cur.fetchone()[0])

        if historical_load_table_exists:
            cur.execute(f"SELECT MAX(date) from {historical_load_table} where company_symbol = '{company_symbol}' ")
            maxDataAvailable = cur.fetchone()[0]
            if maxDataAvailable == str(end_date.date()):
                return company_name,company_symbol,None,historical_load_table

        outputsize='full'
        df = get_api_data(company_info,outputsize)
        df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]

        print(f"Data Loaded for {company_name}")
        
        return company_name,company_symbol, df,historical_load_table
    
    except Exception as e:
        print(f"{e} error in {inspect.currentframe().f_code.co_name} while fetching for {company_name}")
        return company_name,company_symbol, None,historical_load_table

# %%
def generic_load_data(load_type,company_info):

    try:
        company_name, company_symbol = company_info

        if load_type == 'historical':

            return historical_company_data(company_info)
        else:
            return daily_load_company(company_info)
    
    except Exception as e:
        print(f"{e} error in {inspect.currentframe().f_code.co_name} while fetching for {company_name}")
        return company_name,company_symbol, None,historical_load_table
    

def insert_company_data(company_data):
    company_name, company_symbol, stock_data, tableName = company_data

    if stock_data is None:
        return

    print(f"Inserting data into table: {tableName}")
    print(f"Max date in the provided data: {stock_data['Date'].max()}")

    try:
        conn = sqlite3.connect(DB_NAME, timeout=10)
        cursor = conn.cursor()

        for index, row in stock_data.iterrows():
            date_str = row['Date'].strftime('%Y-%m-%d')
            cursor.execute(f"""
                INSERT INTO {tableName} (Date, Company, Company_Symbol, Open, Close, High, Low, Volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date_str,
                company_name,
                company_symbol,
                float(row['Open']),
                float(row['Close']),
                float(row['High']),
                float(row['Low']),
                int(row['Volume'])
            ))

        conn.commit()
        print("Data insertion successful.")

    except Exception as e:
        print(f"for {company_name} inside function {inspect.currentframe().f_code.co_name} error is   {e} \n")

    finally:
        if conn:
            conn.close()

# %%
def main(loadType):
    initialise_Database() 
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_company = {executor.submit(generic_load_data,loadType, company): company for company in BSE_TOP_10}
            
    for future in concurrent.futures.as_completed(future_to_company):
        company_data = future.result()
        insert_company_data(company_data)


