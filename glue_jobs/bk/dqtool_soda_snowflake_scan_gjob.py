import sys
import traceback
import boto3
from awsglue.utils import getResolvedOptions
from soda.scan import Scan

# # パラメータに指定されているデータソース設定ファイル、チェック設定ファイルを取得する
var_args = getResolvedOptions(sys.argv, ['configuration_path', 'checks_path'])
var_configuration_path = var_args['configuration_path']
var_checks_path = var_args['checks_path']

def download_s3_file(s3_path, local_path):
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    s3.download_file(bucket, key, local_path)

# ファイルのダウンロード
local_configuration_path = "/tmp/configuration.yaml"
local_checks_path = "/tmp/checks.yaml"

download_s3_file(var_configuration_path, local_configuration_path)
download_s3_file(var_checks_path, local_checks_path)

scan = Scan()
scan.set_verbose()
scan.add_configuration_yaml_file(local_configuration_path)
scan.set_data_source_name("my_snowflake")
scan.add_sodacl_yaml_files(local_checks_path)
scan.set_scan_definition_name("my_scan")

# スキャン実行
try:
    result = scan.execute()
    print(scan.get_logs_text())
    if result != 0:
        raise ValueError('Soda Scan failed')
except Exception as e:
    print(f"Error occurred during scan execution: {e}")
    raise Exception(traceback.format_exc())
