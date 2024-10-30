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
file_name = "METEOROLOGY_PRECIPITATION"
s3_key = 'testdata/' + file_name + ".csv"
s3 = boto3.client('s3')

try:
    s3.download_file(s3_bucket, s3_key, 'target.csv')
except Exception as e:
    print(f"Failed to download file: {e}")
    raise

# CSVファイルを取り込む
df_target_csv = pd.read_csv('target.csv', encoding='utf-8')
# 假设你有新的列名列表
new_columns = [
    "観測所番号",
    "都道府県",
    "地点",
    "国際地点番号",
    "現在時刻/観測時刻(年)",
    "現在時刻/観測時刻(月)",
    "現在時刻/観測時刻(日)",
    "現在時刻/観測時刻(時)",
    "現在時刻/観測時刻(分)",
    "現在値(mm)",
    "現在値の品質情報",
    "当日の最大値(mm)",
    "当日の最大値の品質情報",
    "当日の最大値起時（時）(まで)",
    "当日の最大値起時（分）(まで)",
    "当日の最大値起時(まで)の品質情報",
    "極値更新",
    "10年未満での極値更新",
    "前日までの観測史上1位の値(mm)",
    "前日までの観測史上1位の値の品質情報",
    "前日までの観測史上1位の値の年",
    "前日までの観測史上1位の値の月",
    "前日までの観測史上1位の値の日",
    "前日までの月の1位の値(mm)",
    "前日までの月の1位の値の品質情報",
    "前日までの月の1位の値の年",
    "前日までの月の1位の値の月",
    "前日までの月の1位の値の日",
    "統計開始年"
]
df_target_csv.columns = new_columns
df_target_csv['現在値(mm) > 当日の最大値(mm)'] = df_target_csv['現在値(mm)'] > df_target_csv['当日の最大値(mm)']

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
    - duplicate_count("観測所番号", "現在時刻/観測時刻(年)", "現在時刻/観測時刻(月)", "現在時刻/観測時刻(日)", "現在時刻/観測時刻(時)", "現在時刻/観測時刻(分)") = 0
    - missing_count("観測所番号") = 0
    - invalid_count("観測所番号") = 0:
        valid format: integer
        valid max length: 10
    - missing_count("都道府県") = 0
    - invalid_count("都道府県") = 0:
        valid max length: 50
    - missing_count("地点") = 0
    - invalid_count("地点") = 0:
        valid max length: 50
    - invalid_count("国際地点番号") = 0:
        valid format: integer
        valid max length: 10
    - missing_count("現在時刻/観測時刻(年)") = 0
    - invalid_count("現在時刻/観測時刻(年)") = 0:
        valid format: integer
        valid max length: 4
    - missing_count("現在時刻/観測時刻(月)") = 0
    - invalid_count("現在時刻/観測時刻(月)") = 0:
        valid format: integer
        valid max length: 2
    - missing_count("現在時刻/観測時刻(日)") = 0
    - invalid_count("現在時刻/観測時刻(日)") = 0:
        valid format: integer
        valid max length: 2
    - missing_count("現在時刻/観測時刻(時)") = 0
    - invalid_count("現在時刻/観測時刻(時)") = 0:
        valid format: integer
        valid max length: 2
    - missing_count("現在時刻/観測時刻(分)") = 0
    - invalid_count("現在時刻/観測時刻(分)") = 0:
        valid format: integer
        valid max length: 2
    - invalid_count("現在値(mm)") = 0:
        valid regex: '^[0-9]{1,37}\.[0-9]{1}$'
    - invalid_count("現在値の品質情報") = 0:
        valid format: integer
        valid max length: 1
    - invalid_count("当日の最大値(mm)") = 0:
        valid regex: '^[0-9]{1,37}\.[0-9]{1}$'
    - invalid_count("当日の最大値の品質情報") = 0:
        valid format: integer
        valid max length: 1
    - invalid_count("当日の最大値起時（時）(まで)") = 0:
        valid format: integer
        valid max length: 2
    - invalid_count("当日の最大値起時（分）(まで)") = 0:
        valid format: integer
        valid max length: 2
    - invalid_count("当日の最大値起時(まで)の品質情報") = 0:
        valid format: integer
        valid max length: 1
    - invalid_count("極値更新") = 0:
        valid format: integer
        valid max length: 2
    - invalid_count("10年未満での極値更新") = 0:
        valid format: integer
        valid max length: 2
    - invalid_count("前日までの観測史上1位の値(mm)") = 0:
        valid regex: '^[0-9]{1,37}\.[0-9]{1}$'
    - invalid_count("前日までの観測史上1位の値の品質情報") = 0:
        valid format: integer
        valid max length: 1
    - invalid_count("前日までの観測史上1位の値の年") = 0:
        valid format: integer
        valid max length: 4
    - invalid_count("前日までの観測史上1位の値の月") = 0:
        valid format: integer
        valid max length: 2
    - invalid_count("前日までの観測史上1位の値の日") = 0:
        valid format: integer
        valid max length: 2
    - invalid_count("前日までの月の1位の値(mm)") = 0:
        valid regex: '^[0-9]{1,37}\.[0-9]{1}$'
    - invalid_count("前日までの月の1位の値の品質情報") = 0:
        valid format: integer
        valid max length: 1
    - invalid_count("前日までの月の1位の値の年") = 0:
        valid format: integer
        valid max length: 4
    - invalid_count("前日までの月の1位の値の月") = 0:
        valid format: integer
        valid max length: 2
    - invalid_count("前日までの月の1位の値の日") = 0:
        valid format: integer
        valid max length: 2
    - missing_count("統計開始年") = 0
    - invalid_count("統計開始年") = 0:
        valid format: integer
        valid max length: 4
    - invalid_count("現在値(mm) > 当日の最大値(mm)") = 0:
        valid values: [false]
    - failed rows:
        fail condition: (("前日までの観測史上1位の値(mm)" is not null and ("前日までの観測史上1位の値の品質情報" is null or "前日までの観測史上1位の値の年" is null or "前日までの観測史上1位の値の月" is null or "前日までの観測史上1位の値の日" is null)) or ("前日までの月の1位の値(mm)" is not null and ("前日までの月の1位の値の品質情報" is null or "前日までの月の1位の値の年" is null or "前日までの月の1位の値の月" is null or "前日までの月の1位の値の日" is null)))
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
