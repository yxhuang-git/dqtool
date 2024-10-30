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
file_name = "CUSTOMER_MASTER"
post_code_data_file_name = "POST_CODE_DATA"
s3_key = 'testdata/' + file_name + ".csv"
post_code_data_s3_key = 'testdata/' + post_code_data_file_name + ".csv"
s3 = boto3.client('s3')

try:
    s3.download_file(s3_bucket, s3_key, 'target.csv')
    s3.download_file(s3_bucket, post_code_data_s3_key, 'reference_file.csv')
except Exception as e:
    print(f"Failed to download file: {e}")
    raise

# CSVファイルを取り込む
df_target_csv = pd.read_csv('target.csv', encoding='utf-8', dtype={'郵便番号': str})
df_post_code_csv = pd.read_csv('reference_file.csv', encoding='utf-8', dtype={'郵便番号': str})
df_post_code_csv["郵便データの組み合わせ"] = df_post_code_csv["市区町村名（漢字）"] + df_post_code_csv["町域名（漢字）"]
df_post_code_filtered = df_post_code_csv[['郵便番号', '郵便データの組み合わせ']]
merged_df = pd.merge(df_target_csv, df_post_code_filtered, on='郵便番号', how='left')
merged_df['郵便データの組み合わせとの一致チェック'] = merged_df['住所'] == merged_df['郵便データの組み合わせ']

# Soda初期化
scan = Scan()
scan.set_scan_definition_name("dask and pandas tutorial")
scan.set_data_source_name("pandas")

# Pandas DataFrame追加
scan.add_pandas_dataframe(dataset_name="target_csv", pandas_df=merged_df, data_source_name="pandas")

# 検査ルール定義
row_count_checks = """
for each dataset target_csv:
  datasets:
    - include target_csv
  checks:
    - duplicate_count("顧客 Id") = 0
    - missing_count("顧客 Id") = 0
    - invalid_count("顧客 Id") = 0:
        valid max length: 20
    - missing_count("名前") = 0
    - invalid_count("名前") = 0:
        valid max length: 20
    - missing_count("性別") = 0
    - invalid_count("性別") = 0:
        valid max length: 1
        valid values: ["男", "女", "F", "M"]
    - missing_count("誕生日") = 0
    - invalid_count("誕生日") = 0:
        valid format: date inverse
    - missing_count("婚姻") = 0
    - invalid_count("婚姻") = 0:
        valid max length: 2
        valid values: ["既婚", "未婚"]
    - missing_count("血液型") = 0
    - invalid_count("血液型") = 0:
        valid max length: 3
        valid values: ["AB型", "A型", "B型", "O型"]
    - missing_count("都道府県") = 0
    - invalid_count("都道府県") = 0:
        valid max length: 10
    - missing_count("キャリア") = 0
    - invalid_count("キャリア") = 0:
        valid max length: 100
    - missing_count("カレーの食べ方") = 0
    - invalid_count("カレーの食べ方") = 0:
        valid max length: 100
    - missing_count("郵便番号") = 0
    - invalid_count("郵便番号") = 0:
        valid regex: '^[0-9]{7}$'
    - missing_count("住所") = 0
    - invalid_count("住所") = 0:
        valid max length: 500
    - invalid_count("郵便データの組み合わせとの一致チェック") = 0:
        valid values: [true]
"""
print(row_count_checks)
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
