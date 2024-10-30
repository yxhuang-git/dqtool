import boto3


class S3FileDelete:
    def __init__(self, conf) -> None:
        self.conf = conf

    def s3_file_delete(self, file_dic):
        """ S3オブジェクトを削除する
        """
        exec_flg = False
        s3_client = boto3.client('s3')

        if len(file_dic) != 0:
            for filename, location in file_dic.items():
                # bucketおよびprefix取得処理
                li = location.split('/')
                s3_bucket = li.pop(0)
                s3_prefix = '/'.join(li)

                # delete処理
                s3_client.delete_object(Bucket=s3_bucket, Key=s3_prefix + filename)
                exec_flg = True

        # deleteが行われなかった場合エラーとする
        if not exec_flg:
            raise Exception('The file has not been delete. Check the configuration file.')