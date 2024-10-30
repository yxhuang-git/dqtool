import sys
import traceback
import boto3
from awsglue.utils import getResolvedOptions
from soda.scan import Scan
from datetime import datetime
from pandas import json_normalize

# システム日時を取得する
now = datetime.now()
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
local_checks_path = "/tmp/checks.yaml"

download_s3_file(var_configuration_path, local_configuration_path)

scan = Scan()
scan.set_verbose()
scan.add_configuration_yaml_file(local_configuration_path)
scan.set_data_source_name("my_snowflake")
# scan.add_sodacl_yaml_files(local_checks_path)
# 検査ルール定義
row_count_checks = """
checks for METEOROLOGY_PRECIPITATION:
  - duplicate_count("OBSERVATORY_NO", "YEAR_NOW", "MONTH_NOW", "DAY_NOW", "HOUR_NOW", "MINUTE_NOW") = 0
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
    output_file = "STG_SCHEMA.METEOROLOGY_PRECIPITATION_CheckResult_" + now.strftime("%Y%m%d-%H%M%S") + ".csv"
    df.to_csv(output_file, index=False)

    # S3へアップロード
    s3.upload_file(output_file, s3_bucket, 'testdata/soda_check_results/' + output_file)
    print(f"Combined scan results uploaded to s3://{s3_bucket}/testdata/soda_check_results/{output_file}")

except Exception as e:
    print(f"Failed to execute scan: {e}")
    raise