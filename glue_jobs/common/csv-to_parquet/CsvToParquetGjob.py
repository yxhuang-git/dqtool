import sys
import argparse
import logging
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ロガーインスタンス生成
logger = logging.getLogger(__name__)
[logger.removeHandler(h) for h in logger.handlers]
log_format = '[%(levelname)s][%(filename)s][%(funcName)s:%(lineno)d]\t%(message)s'
stdout_handler = logging.StreamHandler(stream=sys.stdout)
stdout_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(stdout_handler)
logger.setLevel(logging.INFO)


def get_struct(bucket, key):
    client = boto3.client('s3')

    obj = client.get_object(Bucket=bucket, Key=key)
    json_str = obj['Body'].read().decode()

    return json.loads(json_str)


def tranceform_parquet(src, dest, header, bucket, key, mode):
    spark = SparkSession.builder.appName('csv_to_parquet').getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")

    schema = get_struct(bucket, key)

    if 's3://' in src and 's3a://' not in src:
        src = src.replace('s3://', 's3a://')

    logger.info('変換対象： ' + src)
    df = spark.read.csv(
        src,
        header=header
    )

    if 's3://' in dest and 's3a://' not in dest:
        dest = dest.replace('s3://', 's3a://')

    for k, v in schema.items():
        df = df.withColumn(k, col(k).cast(v))
        
    df.write.mode(mode).parquet(dest)

    logger.info('Parquet変換完了')
    logger.info('Parquetファイル出力先 ' + dest)


if __name__ in "__main__":
    parser = argparse.ArgumentParser(
        description='Parser for Command Arguments.')
    parser.add_argument('--src', dest='src')
    parser.add_argument('--dest', dest='dest')
    parser.add_argument('--header', dest='header')
    parser.add_argument('--struct-conf-bucket', dest='struct_conf_bucket')
    parser.add_argument('--struct-conf-key', dest='struct_conf_key')
    parser.add_argument('--mode', dest='mode', default='overwrite')

    args = parser.parse_args()

    src = args.src
    dest = args.dest
    header = args.header
    if header == '0':
        header = True
    elif header == '1':
        header = False

    bucket = args.struct_conf_bucket
    key = args.struct_conf_key
    mode = args.mode
    logger.info('Parquet変換処理開始')

    tranceform_parquet(src, dest, header, bucket, key, mode)
