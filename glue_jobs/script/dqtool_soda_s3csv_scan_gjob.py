import pandas as pd
import boto3
from soda.scan import Scan

# S3からファイルをダウンロードする
s3_bucket = 'dqtool'
s3_key = 'input_files/sodacheck.csv'
s3 = boto3.client('s3')

try:
    s3.download_file(s3_bucket, s3_key, 'local_file.csv')
except Exception as e:
    print(f"Failed to download file: {e}")
    raise

# 
df_budget_master = pd.read_csv('local_file.csv', encoding='utf-8')
df_budget_master.columns = ['a', 'b', 'c', 'd']
print(df_budget_master.columns)

# Sodaスキャン初期化
scan = Scan()
scan.set_scan_definition_name("dask and pandas tutorial")
scan.set_data_source_name("pandas")

# 
scan.add_pandas_dataframe(dataset_name="budget_master", pandas_df=df_budget_master, data_source_name="pandas")

# チェックルール定義
row_count_checks = """
for each dataset budget_master:
  datasets:
    - include budget_master
  checks:
    - invalid_count(c) = 0:
        valid format: date inverse
    - invalid_count(d) = 0:
        valid format: decimal
"""
scan.add_sodacl_yaml_str(row_count_checks)

# スキャン実行
try:
    scan.execute()
    print(scan.get_logs_text())
except Exception as e:
    print(f"Failed to execute scan: {e}")
