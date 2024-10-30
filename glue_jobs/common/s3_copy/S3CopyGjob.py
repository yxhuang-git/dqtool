import os
import boto3
import glob


class S3Copy:
    def __init__(self, conf) -> None:
        self.conf = conf

    def s3_to_s3(self, key_Flg, file_dic):
        """ S3からS3へコピー処理を行う

        Raises:
            Exception: コピーが行われなかった場合エラーとする
        """
        exec_flg = False
        s3_client = boto3.client('s3')
        root_key = 's3_to_s3'

        s3_output_bucket = self.conf[root_key][key_Flg]['output']['bucket']
        s3_output_prefix = self.conf[root_key][key_Flg]['output']['prefix']

        if len(file_dic) != 0:
            # rawデータ用バケットにある対象データ分コピーを行う
            for filename, location in file_dic.items():
                # bucketおよびprefix取得処理
                li = location.split('/')
                s3_input_bucket = li.pop(0)
                s3_input_prefix = '/'.join(li)
                
                # rawデータのファイル名を取得
                s3_output_key = s3_output_prefix + filename
                s3_input_key = s3_input_prefix + filename
                # 生データをinputバケットからoutputバケットに移動する　
                s3_client.copy_object(Bucket=s3_output_bucket,
                                    Key=s3_output_key,
                                    CopySource={
                                        'Bucket': s3_input_bucket,
                                        'Key': s3_input_key
                                    }
                                    )
                exec_flg = True

        # copyが行われなかった場合エラーとする
        if not exec_flg:
            raise Exception(
                'The file has not been copy. Check the configuration file.')

    def s3_to_local(self, key_flg, file_dic):
        """ S3からローカルへオブジェクトをダウンロードする
        Raises:
            Exception: ダウンロードが行われなかった場合エラー
        """
        exec_flg = False
        s3_client = boto3.client('s3')
        root_key = 'download_file'

        local = self.conf[root_key][key_flg]['output']['local']

        # sqlダウンロード先フォルダが存在しない場合作成する。
        if not os.path.exists(local):
            os.makedirs(local)

        if len(file_dic) != 0:
            for filename, location in file_dic.items():
                # bucketおよびprefix取得処理
                li = location.split('/')
                s3_bucket = li.pop(0)
                s3_prefix = '/'.join(li)
                # download処理
                s3_client.download_file(s3_bucket, s3_prefix + filename, local + filename)
                exec_flg = True

        # downloadが行われなかった場合エラーとする
        if not exec_flg:
            raise Exception(
                'The file has not been downloaded. Check the configuration file.')

    def local_to_s3(self, key_flg):
        """ ローカルからS3へオブジェクトをアップロードする

        Raises:
            Exception: アップロードが行われなかった場合エラー
        """
        exec_flg = False
        s3 = boto3.client('s3')
        root_key = 'upload_file'
        
        # 設定ファイルの値を取得する
        bucket = self.conf[root_key][key_flg]['output']['bucket']
        prefix = self.conf[root_key][key_flg]['output']['prefix']
        local = self.conf[root_key][key_flg]['input']['local']
        keyword = self.conf[root_key][key_flg]['input']['keyword']

        upload_files = glob.glob(local + '*')
        for upload_file in upload_files:
            filename = upload_file.split('/')[-1]
            # 取得したいファイルを部分一致または完全一致で取得する
            if any(checkstr in filename for checkstr in keyword):
                s3.upload_file(upload_file, bucket, prefix + filename)
                exec_flg = True

        # uploadが行われなかった場合エラーとする
        if not exec_flg:
            raise Exception(
                'The file has not been upload. Check the configuration file.')


