import pandas as pd
import boto3
from soda.scan import Scan
from datetime import datetime
import json
from pandas import json_normalize

# システム日時を取得する
now = datetime.now()
# S3からファイルをダウンロードする
s3_bucket = 'dqtool'
file_name = "RETURNS"
s3_key = 'testdata/' + file_name + ".csv"
s3 = boto3.client('s3')

try:
    s3.download_file(s3_bucket, s3_key, 'target.csv')
except Exception as e:
    print(f"Failed to download file: {e}")
    raise

# CSVファイルを取り込む
df_target_csv = pd.read_csv('target.csv', encoding='utf-8')
print(df_target_csv.columns)

# Soda初期化
scan = Scan()
scan.set_scan_definition_name("dask and pandas tutorial")
scan.set_data_source_name("pandas")

# Pandas DataFrame追加
scan.add_pandas_dataframe(dataset_name="target_csv", pandas_df=df_target_csv, data_source_name="pandas")

# 検査ルール定義
row_count_checks = """
for each dataset target_csv:
  datasets:
    - include target_csv
  checks:
    - duplicate_count("オーダー ID") = 0
    - missing_count("オーダー ID") = 0
    - invalid_count("オーダー ID") = 0:
        valid max length: 20
    - missing_count("返品") = 0
    - invalid_count("返品") = 0:
        valid max length: 1
        valid values: ["○", "1"]
"""
scan.add_sodacl_yaml_str(row_count_checks)

# スキャンを実施して結果データをCSVに保存する
try:
    scan.execute()
    print(scan.get_logs_text())
    
    # 検査結果を取得する
    results = scan.get_scan_results()

    # json_normalizeでJSONデータへ転換
    df = json_normalize(results)

    # CSVファイルに保存して出力する
    output_file = file_name + "_CheckResult_" + now.strftime("%Y%m%d-%H%M%S") + ".csv"
    df.to_csv(output_file, index=False)

    # S3へアップロード
    s3.upload_file(output_file, s3_bucket, 'testdata/soda_check_results/' + output_file)
    print(f"Combined scan results uploaded to s3://{s3_bucket}/testdata/soda_check_results/{output_file}")

except Exception as e:
    print(f"Failed to execute scan: {e}")
    raise
