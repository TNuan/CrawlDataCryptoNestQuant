import pandas as pd

# Đọc dữ liệu từ 3 file csv
df1 = pd.read_csv('./data/new_nasdaq.csv')
df2 = pd.read_csv('./data/gold_price_hourly.csv')
df3 = pd.read_csv('./data/LABEL_BTC_1_with_BTC_new.csv')
# Ghép df1 và df2 theo trường chung
df_merged = pd.merge(df1, df2, on='date')

# Ghép df_merged và df3 theo trường chung
df_merged = pd.merge(df_merged, df3, on='date')
df_merged = df_merged .drop(['SYMBOL'],axis=1)

# In ra dataframe đã được ghép
df_merged.to_csv('Final_nasdaq_gold_btc_1.csv', index=False)
