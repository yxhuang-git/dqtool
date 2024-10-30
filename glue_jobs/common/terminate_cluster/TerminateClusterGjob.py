import sys
import boto3
import logging
import traceback


class TerminateCluster:

    def __init__(self) -> None:
        # ロガーインスタンス生成
        self.logger = logging.getLogger(__name__)
        [self.logger.removeHandler(h) for h in self.logger.handlers]
        log_format = '[%(levelname)s][%(filename)s][%(funcName)s:%(lineno)d]\t%(message)s'
        stdout_handler = logging.StreamHandler(stream=sys.stdout)
        stdout_handler.setFormatter(logging.Formatter(log_format))
        self.logger.addHandler(stdout_handler)
        self.logger.setLevel(logging.INFO)

    def terminate_cluster(self, jobflowid):
        var_jobflowid = jobflowid
        self.logger.info("EMR停止処理開始")

        emr_client = boto3.client('emr')
        try:
            
            self.logger.info("EMRクラスター停止処理")
            if var_jobflowid is not None:
                # clusterが停止していない場合、clusterを停止する
                var_response = emr_client.describe_cluster(ClusterId=var_jobflowid)
                state = var_response['Cluster']['Status']['State']

                if state not in ['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'] :
                        emr_client.terminate_job_flows(JobFlowIds=[var_jobflowid])
                else:
                    self.logger.info("cluster " + jobflowid + " is already stopped.")
                self.logger.info("EMR停止成功 clusterid = " + var_jobflowid)
        except Exception as e:
            self.logger.error("EMR停止失敗 clusterid = " + var_jobflowid)
            raise e
