import boto3
import S3ListObjectsGjob


class S3FileCheck:
    def __init__(self, conf) -> None:
        self.conf = conf
        self.s3_list = S3ListObjectsGjob.S3ListObjects()

    def s3_file_chek(self, key_flg):
        client = boto3.client('s3')
        file_dic = {}
        info = self.conf['s3_file_chek']
        root_key = 's3_file_chek'
        keys = info.keys()

        if key_flg not in keys:
            raise Exception('key ' + key_flg + ' is not contains config file. check config file.')

        s3_bucket = self.conf[root_key][key_flg]['bucket']
        s3_prefix = self.conf[root_key][key_flg]['prefix']
        keyword = self.conf[root_key][key_flg]['keyword']

        # ファイル一覧取得処理を呼び出す
        file_dic_list = self.s3_list.s3_list_objects(client, s3_bucket, s3_prefix, keyword)
        for filename in file_dic_list.keys():
            # 取得したファイル一覧を部分一致または完全一致で検索する
            if any(checkstr in filename for checkstr in keyword):
                    file_dic.setdefault(filename, s3_bucket + '/' + s3_prefix)

        return file_dic