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
file_name = "TRANSACTION_DATA"
customer_master_file_name = "CUSTOMER_MASTER"
post_data_file_name = "POST_CODE_DATA"
related_person_file_name = "RELATED_PERSON_MASTER"
product_file_name = "PRODUCT_MASTER"
s3_key = 'testdata/' + file_name + ".csv"
customer_master_s3_key = 'testdata/' + customer_master_file_name + ".csv"
post_data_s3_key = 'testdata/' + post_data_file_name + ".csv"
related_person_s3_key = 'testdata/' + related_person_file_name + ".csv"
product_s3_key = 'testdata/' + product_file_name + ".csv"
s3 = boto3.client('s3')

try:
    s3.download_file(s3_bucket, s3_key, 'target.csv')
    s3.download_file(s3_bucket, customer_master_s3_key, 'reference_file.csv')
    s3.download_file(s3_bucket, post_data_s3_key, 'post_reference_file.csv')
    s3.download_file(s3_bucket, related_person_s3_key, 'related_person_file.csv')
    s3.download_file(s3_bucket, product_s3_key, 'product_file.csv')
except Exception as e:
    print(f"Failed to download file: {e}")
    raise

# CSVファイルを取り込む
df_target_csv = pd.read_csv('target.csv', encoding='utf-8')
df_target_csv['Order+製品ID'] = df_target_csv['オーダー ID'] + df_target_csv['製品 ID']
df_customer_master_csv = pd.read_csv('reference_file.csv', encoding='utf-8')
df_post_data_csv = pd.read_csv('post_reference_file.csv', encoding='utf-8')
df_related_person_csv = pd.read_csv('related_person_file.csv', encoding='utf-8')
df_product_csv = pd.read_csv('product_file.csv', encoding='utf-8')
df_customer_master_filtered = df_customer_master_csv[['顧客 Id', '名前', '郵便番号']]
df_post_data_filtered = df_post_data_csv[['郵便番号', '都道府県名（漢字）', '市区町村名（漢字）']]
df_product_filtered = df_product_csv[['製品 ID','製品名', 'カテゴリ', 'サブカテゴリ', '原価', '定価']]
new_columns = [
    "顧客 ID",
    "名前",
    "郵便番号"
]
df_customer_master_filtered.columns = new_columns
customer_id_list = df_customer_master_filtered["顧客 ID"].tolist()
area_list = df_related_person_csv["地域"].tolist()
product_id_list = df_product_filtered["製品 ID"].tolist()
merged_df = pd.merge(df_target_csv, df_customer_master_filtered, on='顧客 ID', how='left')
merged_df = pd.merge(merged_df, df_post_data_filtered, on='郵便番号', how='left')
merged_df = pd.merge(merged_df, df_product_filtered, on='製品 ID', how='left', suffixes=('', '_product'))
merged_df['名前との一致チェック'] = merged_df['顧客名'] == merged_df['名前']
merged_df['市区町村との一致チェック'] = merged_df['市区町村'] == merged_df['市区町村名（漢字）']
merged_df['都道府県との一致チェック'] = merged_df['都道府県'] == merged_df['都道府県名（漢字）']
merged_df['カテゴリとの一致チェック'] = merged_df['カテゴリ'] == merged_df['カテゴリ_product']
merged_df['サブカテゴリとの一致チェック'] = merged_df['サブカテゴリ'] == merged_df['サブカテゴリ_product']
merged_df['製品名との一致チェック'] = merged_df['製品名'] == merged_df['製品名_product']
merged_df['売上 > 定価'] = merged_df['売上'] > merged_df['定価']
merged_df['利益との一致チェック'] = merged_df['利益'] == (merged_df['売上'] - merged_df['原価'])

# Soda初期化
scan = Scan()
scan.set_scan_definition_name("dask and pandas tutorial")
scan.set_data_source_name("pandas")

# Pandas DataFrame追加
scan.add_pandas_dataframe(dataset_name="target_csv", pandas_df=merged_df, data_source_name="pandas")

# 検査ルール定義
row_count_checks = f"""
for each dataset target_csv:
  datasets:
    - include target_csv
  checks:
    - duplicate_count("行 ID") = 0
    - duplicate_count("Order+製品ID") = 0
    - missing_count("行 ID") = 0
    - invalid_count("行 ID") = 0:
        valid format: integer
        valid max length: 38
    - missing_count("オーダー ID") = 0
    - invalid_count("オーダー ID") = 0:
        valid max length: 20
    - missing_count("オーダー日") = 0
    - invalid_count("オーダー日") = 0:
        valid format: date inverse
    - missing_count("出荷日") = 0
    - invalid_count("出荷日") = 0:
        valid format: date inverse
    - missing_count("出荷モード") = 0
    - invalid_count("出荷モード") = 0:
        valid max length: 50
        valid values: ["セカンド クラス", "ファースト クラス", "即日配送", "通常配送"]
    - missing_count("顧客 ID") = 0
    - invalid_count("顧客 ID") = 0:
        valid max length: 20
        valid values: {customer_id_list}
    - invalid_count("名前との一致チェック") = 0:
        valid values: [true]
    - invalid_count("市区町村との一致チェック") = 0:
        valid values: [true]
    - invalid_count("都道府県との一致チェック") = 0:
        valid values: [true]
    - missing_count("国") = 0
    - invalid_count("国") = 0:
        valid max length: 50
    - missing_count("地域") = 0
    - invalid_count("地域") = 0:
        valid max length: 50
        valid values: {area_list}
    - missing_count("製品 ID") = 0
    - invalid_count("製品 ID") = 0:
        valid max length: 50
        valid values: {product_id_list}
    - invalid_count("カテゴリとの一致チェック") = 0:
        valid values: [true]
    - invalid_count("サブカテゴリとの一致チェック") = 0:
        valid values: [true]
    - invalid_count("製品名との一致チェック") = 0:
        valid values: [true]
    - missing_count("売上") = 0
    - invalid_count("売上") = 0:
        valid format: integer
        valid max length: 38
    - missing_count("数量") = 0
    - invalid_count("数量") = 0:
        valid format: integer
        valid max length: 38
    - missing_count("割引率") = 0
    - invalid_count("割引率") = 0:
        valid regex: '^[0-9]\.[0-9]{{1,2}}$'
    - missing_count("利益") = 0
    - invalid_count("利益") = 0:
        valid format: integer
        valid max length: 38
    - invalid_count("売上 > 定価") = 0:
        valid values: [false]
    - invalid_count("利益との一致チェック") = 0:
        valid values: [true]
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
