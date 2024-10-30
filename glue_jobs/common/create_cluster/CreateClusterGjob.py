import sys
import boto3
import logging
import traceback
import time

class CreateCluster:

    def __init__(self, conf) -> None:
        self.var_conf = conf    
        
        # ロガーインスタンス生成
        self.logger = logging.getLogger(__name__)
        [self.logger.removeHandler(h) for h in self.logger.handlers]
        log_format = '[%(levelname)s][%(filename)s][%(funcName)s:%(lineno)d]\t%(message)s'
        stdout_handler = logging.StreamHandler(stream=sys.stdout)
        stdout_handler.setFormatter(logging.Formatter(log_format))
        self.logger.addHandler(stdout_handler)
        self.logger.setLevel(logging.INFO)

    def check_status(self, client, var_jobflowid):
        # EMRクラスタの起動ステータスが「WAITING」もしくは「TERMINATED_WITH_ERRORS」になるまで
        # 1分ごとにログ出力を繰り返す
        while True:
            # cluster起動状態を監視
            # 起動中⇒何もしない
            # 待機中⇒while文から抜ける
            # 起動失敗⇒エラーとして処理
            var_response = client.describe_cluster(
                ClusterId=var_jobflowid)
            state = var_response['Cluster']['Status']['State']

            # EMRクラスタの起動ステータスが「WAITING」の場合、ループを抜ける
            if 'WAITING' == state:
                self.logger.info(
                    "running emr cluster. id:" + str(var_jobflowid))
                break

            # EMRクラスタの起動ステータスが「STARTING」の場合、ログを出力
            elif 'STARTING' == state:
                self.logger.info(
                    "starting emr cluster. id:" + str(var_jobflowid))

            # EMRクラスタの起動ステータスが「BOOTSTRAPPING」の場合、ログを出力
            elif 'BOOTSTRAPPING' == state:
                self.logger.info(
                    "bootstrapping emr cluster. id:" +
                    str(var_jobflowid))

            # EMRクラスタの起動ステータスが「TERMINATED_WITH_ERRORS」の場合、例外処理を実施
            elif 'TERMINATED_WITH_ERRORS' == state:
                err_msg = var_response['Cluster']['Status']['StateChangeReason']['Message']
                raise Exception(err_msg)

            # 大量のAPI実行によるエラーを防ぐためsleep
            time.sleep(60)

    def create_cluster(self):
        # EMR起動処理
        self.logger.info("EMRクラスター起動")
        var_response = None
        var_jobflowid = None
        emr_client = boto3.client('emr')
        try:
        # cluster起動
            var_response = emr_client.run_job_flow(
                Name=self.var_conf['Name'],
                LogUri=self.var_conf['LogUri'],
                ReleaseLabel=self.var_conf['ReleaseLabel'],
                Instances=self.var_conf['Instances'],
                BootstrapActions=self.var_conf['BootstrapActions'],
                Applications=self.var_conf['Applications'],
                Configurations=self.var_conf['Configurations'],
                VisibleToAllUsers=self.var_conf['VisibleToAllUsers'],
                JobFlowRole=self.var_conf['JobFlowRole'],
                ServiceRole=self.var_conf['ServiceRole'],
                AutoScalingRole=self.var_conf['AutoScalingRole']
            )

            if var_response is not None:
                for key in var_response.keys():
                    if 'JobFlowId' in key:
                        var_jobflowid = var_response[key]
            
            if var_jobflowid is not None:
                self.check_status(emr_client, var_jobflowid)
                self.logger.info("EMRクラスター起動完了")
                return var_jobflowid
            else:
                raise Exception('JobFlowidが存在しません。')
        # エラーが発生した場合、例外処理を実施
        except Exception as e:
            raise e