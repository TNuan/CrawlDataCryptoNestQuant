import pandas as pd
import pyarrow.parquet as pq
import os
from datetime import datetime, timedelta

# Thư mục chứa các tệp Parquet
token = 'BTC'
folder_path = 'D:/Project/NestQ/CrawlDataNestquant/data/LABEL_BTCUSDT/'
# Tạo một danh sách chứa đường dẫn đến các tệp Parquet trong thư mục
parquet_files = [os.path.join(folder_path, file) for file in os.listdir(
    folder_path) if file.endswith('SYMBOL=BTCUSDT.parquet')]

# Khởi tạo DataFrame rỗng
df_combined = pd.DataFrame()

# Đọc từng tệp Parquet và kết hợp dữ liệu vào DataFrame chung
for parquet_file in parquet_files:

    pf = pq.read_table(parquet_file)
    df = pf.to_pandas(ignore_metadata=True, timestamp_as_object=True)
    print(df)
    # df['OPEN_TIME'] = pd.to_datetime(df["OPEN_TIME"], unit='ms')
    df_combined = pd.concat([df_combined, df])
    date_start = datetime(2023, 6, 1, 0, 0) + timedelta(hours=7)
    date_end = datetime(2023, 6, 30, 23, 0)+timedelta(hours=7)
    
# Chuyển đổi sang milliseconds
    index = int(date_start.timestamp() * 1000)
    while index <= date_end.timestamp() * 1000:
        df2 = pd.DataFrame([['BTCUSDT', 0.1, index]], columns=[
                           'SYMBOL', 'LABEL', 'OPEN_TIME'])
        # df2['OPEN_TIME'] = pd.to_datetime(df2["OPEN_TIME"], unit='ms')
        index += 3600000
        df_combined = pd.concat([df_combined, df2])

# Lưu DataFrame chứa dữ liệu từ tất cả các tệp Parquet thành tệp CSV
csv_file = 'D:/Project/NestQ/CrawlDataNestquant/data/LABEL_0.1_BTC_T6.csv'
df_combined.to_csv(csv_file)
