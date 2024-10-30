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
var_args = getResolvedOptions(sys.argv, ['date', 'conffile_path', 'conffile_name'])

# 実行日付
var_date = var_args['date']
# 設定ファイル配置パス
var_conffile_path = var_args['conffile_path']
# 設定ファイル名
var_conffile_name = var_args['conffile_name']

# 日付にNoneが指定されていた場合、実行日日付を取得
if var_date == 'None':
    var_date = datetime.now().strftime('%Y-%m-%d')
    
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
var_s3_copy.s3_to_local('insert_mart', var_file_dic)

# sql置換変数 ##################################
var_schemaTo = var_conf['exec_query']['table_info']['schemaTo']
var_mart_table = var_conf['exec_query']['table_info']['mart_table']
var_schemaFrom = var_conf['exec_query']['table_info']['schemaFrom']
var_delete_where_clause = var_conf['exec_query']['deleteWhere'] % (var_date)
# sql置換変数 ##################################

# query実行
var_executor = ExecQuery(var_conf, "delete", var_schemaTo, var_mart_table, var_delete_where_clause)
var_executor.main(schemaTo=var_schemaTo, mart_table=var_mart_table, schemaFrom=var_schemaFrom, where_date=var_date)

