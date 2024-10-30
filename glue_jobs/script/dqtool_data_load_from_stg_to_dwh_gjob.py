import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
# 設定ファイル読み込みライブラリ
from LoadConfigGjob import LoadConfig
# S3ファイルチェックライブラリ
from S3FileCheckGjob import S3FileCheck
# S3コピーライブラリ
from S3CopyGjob import S3Copy
#クエリ実行ライブラリ
from ExecQueryGjob import ExecQuery

# パラメータに指定されている設定ファイル配置先、設定ファイル名、日付を取得する
var_args = getResolvedOptions(sys.argv, ['date', 'conffile_path', 'conffile_name', 'replace_or_merge'])

# 実行日付
var_date = var_args['date']
# 設定ファイル配置パス
var_conffile_path = var_args['conffile_path']
# 設定ファイル名
var_conffile_name = var_args['conffile_name']
# 登録対象テーブルに対して、全量反映なのか、それとも差分反映なのか設定値　replace：全量反映　merge：差分反映
var_replace_or_merge = var_args['replace_or_merge']

# 日付にNoneが指定されていた場合、実行日日付を取得
if var_date == 'None':
    var_date = datetime.now().strftime('%Y%m%d')
    
# 設定ファイル読み込み呼び出し処理
var_conf = LoadConfig(var_conffile_path, var_conffile_name).load_config()

var_fc = S3FileCheck(var_conf)
# ファイルチェック処理
var_file_dic = var_fc.s3_file_chek('download_sql_file')
if len(var_file_dic) == 0:
    print('files does not exist.')
    sys.exit(0)
    
# sqlファイルのダウンロード
var_s3_copy = S3Copy(var_conf)
var_s3_copy.s3_to_local('insert_dwh', var_file_dic)

# sql置換変数 ##################################
var_schemaTo = var_conf['exec_query']['table_info']['schemaTo']
var_dwh_table = var_conf['exec_query']['table_info']['dwh_table']
var_schemaFrom = var_conf['exec_query']['table_info']['schemaFrom']
var_stg_table = var_conf['exec_query']['table_info']['stg_table']
# sql置換変数 ##################################

# query実行
if var_replace_or_merge == "replace":
    var_executor = ExecQuery(var_conf, "delete", var_schemaTo, var_dwh_table)
else:
    var_executor = ExecQuery(var_conf)
var_executor.main(schemaTo=var_schemaTo, dwh_table=var_dwh_table, schemaFrom=var_schemaFrom, stg_table=var_stg_table)

