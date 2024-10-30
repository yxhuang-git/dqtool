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
file_name = "BUDGET_MASTER"
related_person_master_file_name = "RELATED_PERSON_MASTER"
s3_key = 'testdata/' + file_name + ".csv"
related_person_master_s3_key = 'testdata/' + related_person_master_file_name + ".csv"
s3 = boto3.client('s3')

try:
    s3.download_file(s3_bucket, s3_key, 'target.csv')
    s3.download_file(s3_bucket, related_person_master_s3_key, 'reference_file.csv')
except Exception as e:
    print(f"Failed to download file: {e}")
    raise

# CSVファイルを取り込む
df_target_csv = pd.read_csv('target.csv', encoding='utf-8')
df_related_person_master_csv = pd.read_csv('reference_file.csv', encoding='utf-8')
df_related_person_master_filtered = df_related_person_master_csv[['地域']]
new_columns = [
    "area",
]
df_related_person_master_filtered.columns = new_columns
area_list = df_related_person_master_filtered["area"].tolist()
#merged_df = pd.merge(df_target_csv, df_related_person_master_filtered, left_on='エリア', right_on='area', how='right')
# Soda初期化
scan = Scan()
scan.set_scan_definition_name("dask and pandas tutorial")
scan.set_data_source_name("pandas")

# Pandas DataFrame追加
scan.add_pandas_dataframe(dataset_name="target_csv", pandas_df=df_target_csv, data_source_name="pandas")

# 検査ルール定義
row_count_checks = f"""
for each dataset target_csv:
  datasets:
    - include target_csv
  checks:
    - duplicate_count("製品カテゴリ", "エリア", "年月") = 0:
        name: 製品カテゴリ＋エリア＋年月をキーとして、予算マスタの重複件数チェック
    - missing_count("製品カテゴリ") = 0
    - invalid_count("製品カテゴリ") = 0:
        valid max length: 50
    - missing_count("エリア") = 0:
        missing values: {area_list}
    - invalid_count("エリア") = 0:
        valid max length: 50
        valid values: {area_list}
    - missing_count("年月") = 0
    - invalid_count("年月") = 0:
        valid max length: 10
    - invalid_count("年月") = 0:
        valid regex: "^[0-9]{{4}}年[0-9]{{1,2}}月$"
    - missing_count("予算") = 0
    - invalid_count("予算") = 0:
        valid format: integer
        valid max length: 38
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
