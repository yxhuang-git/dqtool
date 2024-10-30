import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
# 設定ファイル読み込みライブラリ
from LoadConfigGjob import LoadConfig
# S3ファイルチェックライブラリ
from S3FileCheckGjob import S3FileCheck
# S3ファイルコピーライブラリ
from S3CopyGjob import S3Copy
# S3ファイル削除ライブラリ
from S3FileDeleteGjob import S3FileDelete

# パラメータに指定されている設定ファイル配置先、設定ファイル名、日付を取得する
var_args = getResolvedOptions(sys.argv, ['date', 'glue_conffile_path', 'glue_conffile_name'])

# 実行日付
var_date = var_args['date']
# 設定ファイル配置パス
var_glue_conffile_path = var_args['glue_conffile_path']
# 設定ファイル名
var_glue_conffile_name = var_args['glue_conffile_name']

# 日付にNoneが指定されていた場合、実行日日付を取得
if var_date == 'None':
    var_date = datetime.now().strftime('%Y%m%d')

# 設定ファイル読み込み呼び出し処理
var_conf = LoadConfig(var_glue_conffile_path, var_glue_conffile_name).load_config(year=var_date[0:4], month=var_date[4:6], day=var_date[6:8])

var_fc = S3FileCheck(var_conf)
# ファイルチェック処理
var_file_dic = var_fc.s3_file_chek('s3_to_s3')
if len(var_file_dic) == 0:
    print('files does not exist.')
    sys.exit(0)
# rawデータのs3_to_s3コピー
var_s3_copy = S3Copy(var_conf)
var_s3_copy.s3_to_s3('sample1',var_file_dic)

# 生データファイルを削除する
var_file_dic = var_fc.s3_file_chek('delete_file')
S3FileDelete(var_conf).s3_file_delete(var_file_dic)

