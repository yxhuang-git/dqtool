import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
# 設定ファイル読み込みライブラリ
from LoadConfigGjob import LoadConfig
# S3コピーライブラリ
from S3FileCheckGjob import S3FileCheck
from S3CopyGjob import S3Copy
from ExecQueryGjob import ExecQuery

# パラメータに指定されている設定ファイル配置先、設定ファイル名、日付を取得する
var_args = getResolvedOptions(sys.argv, ['date', 'conffile_path', 'conffile_name'])

# 実行日付
var_date = var_args['date']
# 設定ファイル配置パス
var_conffile_path = var_args['conffile_path']
# 設定ファイル名
var_conffile_name = var_args['conffile_name']

# 日付にNoneが指定されていた場合、実行日日付を取得
if var_date == 'None':
    var_date = datetime.now().strftime('%Y%m%d')
    
# 設定ファイル読み込み呼び出し処理
var_conf = LoadConfig(var_conffile_path, var_conffile_name).load_config()

var_fc = S3FileCheck(var_conf)
# sqlファイルチェック処理
var_sql_file_dic = var_fc.s3_file_chek('download_sql_file')
# copy対象ファイルチェック処理
var_copy_file_dic = var_fc.s3_file_chek('target_file')
print(var_sql_file_dic)
print(var_copy_file_dic)
if len(var_sql_file_dic) == 0 or len(var_copy_file_dic) == 0:
    print('sql or target files does not exist.')
    sys.exit(0)
    
# sqlファイルのダウンロード
var_s3_copy = S3Copy(var_conf)
var_s3_copy.s3_to_local('importCsvFile', var_sql_file_dic)

# sql置換変数 ##################################
var_schema = var_conf['exec_query']['table_info']['schema']
var_table = var_conf['exec_query']['table_info']['table']
# sql置換変数 ##################################

# query実行
var_executor = ExecQuery(var_conf, "delete", var_schema, var_table)
var_executor.main(schema=var_schema, table=var_table, filename=list(var_copy_file_dic.keys())[0])

