import adata
import psycopg2
from psycopg2 import sql
from datetime import datetime
import time


# PostgreSQL 配置
postgres_host = 'localhost'
postgres_port = 5432
postgres_database = 'DB'
postgres_user = 'postgres'
postgres_password = ''

# 创建 PostgreSQL 连接
conn = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    database=postgres_database,
    user=postgres_user,
    password=postgres_password
)
cursor = conn.cursor()

def fetch_stock_kline(stock_code, start_date='2021-01-01', k_type=1):
    # 使用 adata 获取 K 线数据
    df = adata.stock.market.get_market(stock_code=stock_code, start_date=start_date, k_type=k_type)
    return df

def insert_into_postgres(data):
    # 将数据插入 PostgreSQL
    insert_query = sql.SQL('''
        INSERT INTO stock_market_kline (stock_code, trade_date, trade_time, open, close, high, low, volume, amount, change, change_pct, turnover_ratio) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''')
    cursor.executemany(insert_query, data)
    conn.commit()

def main():
    # 获取所有股票代号
    all_stocks = adata.stock.info.all_code()
    
    # 创建 PostgreSQL 表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_codes
        (
            stock_code VARCHAR(20) PRIMARY KEY,
            short_name VARCHAR(50),
            exchange VARCHAR(20),
            list_date VARCHAR(10)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_market_kline
        (
            stock_code VARCHAR(20),
            trade_date DATE,
            trade_time TIMESTAMP,
            open NUMERIC,
            close NUMERIC,
            high NUMERIC,
            low NUMERIC,
            volume BIGINT,
            amount NUMERIC,
            change NUMERIC,
            change_pct NUMERIC,
            turnover_ratio NUMERIC,
            PRIMARY KEY (stock_code, trade_date, trade_time)
        )
    ''')
    conn.commit()
    
    # 处理日期类型并插入股票信息
    insert_query = sql.SQL('''
        INSERT INTO stock_codes (stock_code, short_name, exchange, list_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (stock_code) DO NOTHING
    ''')
    for stock in all_stocks.to_dict('records'):
        cursor.execute(insert_query, (
            stock['stock_code'],
            stock['short_name'],
            stock['exchange'],
            stock['list_date']  # 直接使用,不需要 strptime()
        ))
    conn.commit()
    
    # 遍历所有股票代号
    for stock in all_stocks.itertuples():
        stock_code = stock.stock_code
        start_date = '2021-01-01'
        
        # 检查数据库中是否已有该股票在起始日期的数据
        cursor.execute('''
            SELECT open FROM stock_market_kline
            WHERE stock_code = %s AND trade_date >= %s AND open != 0
            LIMIT 1
        ''', (stock_code, start_date))
        
        if cursor.fetchone():
            print(f"股票 {stock_code} 在 {start_date} 之后已有数据，跳过")
            continue
        
        df = fetch_stock_kline(stock_code, start_date=start_date)  # 获取数据
        if df.empty:
            print(f"股票 {stock_code} 数据为空，10秒后重试")
            time.sleep(10)
            df = fetch_stock_kline(stock_code, start_date=start_date)  # 再次尝试获取数据
        if df.empty:
            print(f"股票 {stock_code} 数据获取失败，插入空记录")
            insert_data = [(
                stock_code,
                datetime.strptime(start_date, '%Y-%m-%d').date(),
                datetime.strptime(f'{start_date} 00:00:00', '%Y-%m-%d %H:%M:%S'),
                0, 0, 0, 0, 0, 0, 0, 0, 0
            )]
        else:
            print(f"处理股票: {stock_code}")
            insert_data = [(
                entry['stock_code'],
                datetime.strptime(entry['trade_date'], '%Y-%m-%d').date(),
                datetime.strptime(entry['trade_time'], '%Y-%m-%d %H:%M:%S'),
                entry['open'],
                entry['close'],
                entry['high'],
                entry['low'],
                entry['volume'],
                entry['amount'],
                entry['change'],
                entry['change_pct'],
                entry['turnover_ratio'],
            ) for _, entry in df.iterrows()]

        # 修改插入语句，使用 ON CONFLICT 子句进行更新
        insert_query = '''
            INSERT INTO stock_market_kline 
            (stock_code, trade_date, trade_time, open, close, high, low, volume, amount, change, change_pct, turnover_ratio)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stock_code, trade_date, trade_time) 
            DO UPDATE SET
                open = EXCLUDED.open,
                close = EXCLUDED.close,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                change = EXCLUDED.change,
                change_pct = EXCLUDED.change_pct,
                turnover_ratio = EXCLUDED.turnover_ratio
        '''
        
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        print(f"股票 {stock_code} 数据已成功插入或更新")

if __name__ == '__main__':
    main()
    cursor.close()
    conn.close()
