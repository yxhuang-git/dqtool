import sys
import traceback
import boto3
from awsglue.utils import getResolvedOptions
from soda.scan import Scan
from datetime import datetime
from pandas import json_normalize

# システム日時を取得する
now = datetime.now()
var_date = now.strftime('%Y-%m-%d')
s3_bucket = 'dqtool'
s3 = boto3.client('s3')
# パラメータに指定されているデータソース設定ファイルを取得する
var_args = getResolvedOptions(sys.argv, ['configuration_path'])
var_configuration_path = var_args['configuration_path']

def download_s3_file(s3_path, local_path):
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    s3.download_file(bucket, key, local_path)

# ファイルのダウンロード
local_configuration_path = "/tmp/configuration.yaml"
local_checks_path = "/tmp/distribution_reference.yaml"

download_s3_file(var_configuration_path, local_configuration_path)
download_s3_file("s3://dqtool/glue_jobs/conf/soda/distribution_reference.yaml", local_checks_path)

scan = Scan()
scan.set_verbose()
scan.add_configuration_yaml_file(local_configuration_path)
scan.set_data_source_name("my_snowflake")
# scan.add_sodacl_yaml_files(local_checks_path)
# 検査ルール定義
row_count_checks = f"""
checks for TRANSACTION_DATA:
    - missing_count(ROW_ID) = 0:
        name: 行ID空欄チェック
    - duplicate_count(ROW_ID) = 0:
        name: PK（行ID）重複件数チェック
    - missing_count(ORDER_ID) = 0:
        name: オーダー ID空欄チェック
    - invalid_count(ORDER_ID) = 0:
        valid length: 15
    - missing_count(ORDER_DAY) = 0:
        name: オーダー日空欄チェック
    - missing_count(SHIPPING_DAY) = 0:
        name: 出荷日空欄チェック
    - missing_count(SHIPPING_MODE) = 0:
        name: 出荷モード空欄チェック
    - invalid_count(SHIPPING_MODE) = 0:
        name: 出荷モードが"セカンド クラス", "ファースト クラス", "即日配送", "通常配送"のいずれなのかという有効性チェック
        valid values: ["セカンド クラス", "ファースト クラス", "即日配送", "通常配送"]
    - failed rows:
        name: 出荷モードが即日配送の場合出荷日がオーダー日と同日になっているか
        fail condition: SHIPPING_MODE = '即日配送' and SHIPPING_DAY <> ORDER_DAY
    - invalid_count(CUSTOMER_TYPE) = 0:
        name: 消費者区分が"消費者", "小規模事業所", "大企業"のいずれなのかという有効性チェック
        valid values: ["消費者", "小規模事業所", "大企業"]
    - missing_count(CUSTOMER_ID) = 0
    - values in (CUSTOMER_ID) must exist in CUSTOMER_MASTER (CUSTOMER_ID):
        name: 顧客IDが顧客マスタに存在するかどうかのチェック
    - missing_count(CUSTOMER_NAME) = 0:
        name: 消費者区分の場合、顧客名が空欄になっていないかチェック
        filter: CUSTOMER_TYPE = '消費者'
    - values in (AREA) must exist in RELATED_PERSON_MASTER (AREA):
        name: 地域が関係者マスタに存在するかどうかのチェック
    - invalid_count(PRODUCT_ID) = 0:
        name: 製品IDがXX-XX-99999999の書式に従っているか
        valid regex: "^[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]{{1,5}}-[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]{{1,5}}-[0-9]{{8}}$"
    - values in (PRODUCT_ID) must exist in PRODUCT_MASTER (PRODUCT_ID):
        name: 製品 IDが商品マスタに存在するかどうかのチェック
    - order_and_product_duplicate_count = 0:
        name: Order+製品IDの重複件数チェック
        order_and_product_duplicate_count query: |
            select count(*) - count(distinct(ORDER_ID || PRODUCT_ID)) 
            FROM TRANSACTION_DATA
    - diff_cnt = 0:
        name: データレイク層にある今日発生したデータが今日の日付で取り込まれているかチェック（両方の件数差が０と想定）
        diff_cnt query: |
            select cnt1-cnt2 from
            (select count(*) as cnt1
            from TRANSACTION_DATA where order_day >= '{var_date}')a,
            (select count(*) as cnt2
            from DQ_TOOL_DB.STG_SCHEMA.TRANSACTION_DATA where order_day >= '{var_date}')b;
    - diff_customer_name_cnt = 0:
        name: 顧客マスタの名前と一致するか
        diff_customer_name_cnt query: |
            SELECT COUNT(*) AS diff_customer_name_cnt
            FROM TRANSACTION_DATA td
            JOIN CUSTOMER_MASTER cm ON td.CUSTOMER_ID = cm.CUSTOMER_ID
            WHERE td.CUSTOMER_NAME <> cm.CUSTOMER_NAME
    - diff_municipality_cnt = 0:
        name: 顧客マスタで取得した郵便番号で取った市区町村と一致するか
        diff_municipality_cnt query: |
            SELECT COUNT(*) AS diff_municipality_cnt
            FROM TRANSACTION_DATA td
            JOIN CUSTOMER_MASTER cm ON td.CUSTOMER_ID = cm.CUSTOMER_ID
            JOIN POST_CODE_DATA pcd ON cm.POST_CODE = pcd.POST_CODE
            WHERE td.MUNICIPALITY <> pcd.MUNICIPALITY
    - diff_municipality_cnt = 0:
        name: 顧客マスタで取得した郵便番号で取った都道府県と一致するか
        diff_municipality_cnt query: |
            SELECT COUNT(*) AS diff_municipality_cnt
            FROM TRANSACTION_DATA td
            JOIN CUSTOMER_MASTER cm ON td.CUSTOMER_ID = cm.CUSTOMER_ID
            JOIN POST_CODE_DATA pcd ON cm.POST_CODE = pcd.POST_CODE
            WHERE td.PREFECTURE <> pcd.PREFECTURE
    - diff_category_cnt = 0:
        name: カテゴリが商品マスタに該当製品ID対応するカテゴリと一致するか
        diff_category_cnt query: |
            SELECT COUNT(*) AS diff_category_cnt
            FROM TRANSACTION_DATA td
            JOIN PRODUCT_MASTER pm ON td.PRODUCT_ID = pm.PRODUCT_ID
            WHERE td.CATEGORY <> pm.CATEGORY
    - diff_sub_category_cnt = 0:
        name: サブカテゴリが商品マスタに該当製品ID対応するサブカテゴリと一致するか
        diff_sub_category_cnt query: |
            SELECT COUNT(*) AS diff_sub_category_cnt
            FROM TRANSACTION_DATA td
            JOIN PRODUCT_MASTER pm ON td.PRODUCT_ID = pm.PRODUCT_ID
            WHERE td.SUB_CATEGORY <> pm.SUB_CATEGORY
    - diff_product_name_cnt = 0:
        name: 製品名が商品マスタに該当製品ID対応する製品名と一致するかどうか
        diff_product_name_cnt query: |
            SELECT COUNT(*) AS diff_product_name_cnt
            FROM TRANSACTION_DATA td
            JOIN PRODUCT_MASTER pm ON td.PRODUCT_ID = pm.PRODUCT_ID
            WHERE td.PRODUCT_NAME <> pm.PRODUCT_NAME
    - sales_over_price_cnt = 0:
        name: 売上が商品マスタの定価を超していないか
        sales_over_price_cnt query: |
            SELECT COUNT(*) AS sales_over_price_cnt
            FROM TRANSACTION_DATA td
            JOIN PRODUCT_MASTER pm ON td.PRODUCT_ID = pm.PRODUCT_ID
            WHERE td.SALES > pm.LIST_PRICE
    - diff_profit_cnt = 0:
        name: 商品マスタに該当製品ID対応する原価を利用し、売上-原価と一致するか
        diff_profit_cnt query: |
            SELECT COUNT(*) AS diff_profit_cnt
            FROM TRANSACTION_DATA td
            JOIN PRODUCT_MASTER pm ON td.PRODUCT_ID = pm.PRODUCT_ID
            WHERE td.PROFIT = td.SALES - pm.COST_PRICE
"""
scan.add_sodacl_yaml_str(row_count_checks)
scan.set_scan_definition_name("my_scan")

# スキャンを実施して結果データをCSVに保存する
try:
    scan.execute()
    print(scan.get_logs_text())
    
    # 検査結果を取得する
    results = scan.get_scan_results()

    # json_normalizeでJSONデータへ転換
    df = json_normalize(results)

    # CSVファイルに保存して出力する
    output_file = "DWH_SCHEMA.TRANSACTION_DATA_CheckResult_" + now.strftime("%Y%m%d-%H%M%S") + ".csv"
    df.to_csv(output_file, index=False)

    # S3へアップロード
    s3.upload_file(output_file, s3_bucket, 'testdata/soda_check_results/' + output_file)
    print(f"Combined scan results uploaded to s3://{s3_bucket}/testdata/soda_check_results/{output_file}")

except Exception as e:
    print(f"Failed to execute scan: {e}")
    raise