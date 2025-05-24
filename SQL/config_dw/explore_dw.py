import duckdb
import pandas as pd

# Connect to the DuckDB database
con = duckdb.connect(database='datawarehouse.duckdb')

def show_tables():
    """Hiển thị danh sách các bảng"""
    print("Danh sách các bảng trong datawarehouse:")
    tables = con.sql("SHOW TABLES").fetchall()
    for table in tables:
        print(f"  - {table[0]}")
    print()

def describe_table(table_name):
    """Mô tả cấu trúc bảng"""
    print(f"Cấu trúc bảng {table_name}:")
    con.sql(f"DESCRIBE {table_name}").show()
    print()

def sample_data(table_name, limit=5):
    """Xem dữ liệu mẫu"""
    print(f"Dữ liệu mẫu từ {table_name} (giới hạn {limit} records):")
    con.sql(f"SELECT * FROM {table_name} LIMIT {limit}").show()
    print()

def count_records():
    """Đếm số lượng records trong các bảng"""
    print("Số lượng records trong các bảng:")
    tables = ['dim_time', 'dim_companies', 'dim_topics', 'dim_news', 
              'fact_candles', 'fact_news_companies', 'fact_news_topics']
    
    for table in tables:
        count = con.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  - {table}: {count:,} records")
    print()

def explore_companies(limit=10):
    """Khám phá dữ liệu công ty"""
    print(f"Top {limit} công ty theo sector:")
    con.sql(f"""
        SELECT company_industry_sector, COUNT(*) as company_count
        FROM dim_companies 
        GROUP BY company_industry_sector
        ORDER BY company_count DESC
        LIMIT {limit}
    """).show()
    print()

def explore_news_sentiment():
    """Phân tích sentiment của tin tức"""
    print("Phân tích sentiment tin tức:")
    con.sql("""
        SELECT 
            new_overall_sentiment_label,
            COUNT(*) as news_count,
            ROUND(AVG(new_overall_sentiment_score), 4) as avg_score
        FROM dim_news 
        GROUP BY new_overall_sentiment_label
        ORDER BY news_count DESC
    """).show()
    print()

def explore_trading_data():
    """Khám phá dữ liệu giao dịch"""
    print("Thống kê dữ liệu giao dịch:")
    con.sql("""
        SELECT 
            COUNT(*) as total_candles,
            SUM(candle_volume) as total_volume,
            ROUND(AVG(candle_close), 2) as avg_close_price,
            MIN(candle_low) as min_price,
            MAX(candle_high) as max_price
        FROM fact_candles
    """).show()
    print()

def top_companies_by_volume():
    """Top công ty theo volume giao dịch"""
    print("Top 10 công ty có volume giao dịch cao nhất:")
    con.sql("""
        SELECT 
            c.company_name,
            c.company_ticket,
            fc.candle_volume,
            fc.candle_close,
            (fc.candle_high - fc.candle_low) / fc.candle_low * 100 as price_range_pct
        FROM fact_candles fc
        JOIN dim_companies c ON fc.candle_company_id = c.company_id
        ORDER BY fc.candle_volume DESC
        LIMIT 10
    """).show()
    print()

def news_by_topics():
    """Phân tích tin tức theo chủ đề"""
    print("Số lượng tin tức theo chủ đề:")
    con.sql("""
        SELECT 
            t.topic_name,
            COUNT(fnt.new_topic_new_id) as news_count,
            ROUND(AVG(fnt.new_topic_relevance_score), 3) as avg_relevance
        FROM dim_topics t
        JOIN fact_news_topics fnt ON t.topic_id = fnt.new_topic_topic_id
        GROUP BY t.topic_name
        ORDER BY news_count DESC
    """).show()
    print()

if __name__ == "__main__":
    print("DATAWAREHOUSE EXPLORER")
    print("=" * 50)
    
    # Hiển thị tổng quan
    show_tables()
    count_records()
    
    # Khám phá từng nhóm dữ liệu
    explore_companies()
    explore_news_sentiment()
    explore_trading_data()
    top_companies_by_volume()
    news_by_topics()
    
    print("Hoàn thành khám phá datawarehouse!")
    
    # Đóng kết nối
    con.close()