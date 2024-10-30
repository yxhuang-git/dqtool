import sys
import boto3
import logging
import traceback
import time


class ExecuteSteps:

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

    def check_status(self, client, var_jobflowid, var_stepId):

        while True:
            # プログラム実行結果取得処理
            # COMPLETE⇒正常終了
            # FAILED⇒異常終了
            var_stepId_response = client.describe_step(
                ClusterId=var_jobflowid, StepId=var_stepId)
            var_state = var_stepId_response['Step']['Status']['State']
            # プログラム実行結果が成功の場合
            if 'COMPLETED' in var_state:
                self.logger.info("EMRプログラム実行結果ステータス:" + str(var_state))
                break
            elif 'FAILED' in var_state:
                self.logger.info("EMRプログラム実行結果ステータス:" + str(var_state))
                var_reason = None
                if 'Reason' in var_stepId_response['Step']['Status']['FailureDetails']:
                    var_reason = var_stepId_response['Step']['Status']['FailureDetails']['Reason']
                logfile = var_stepId_response['Step']['Status']['FailureDetails']['LogFile']
                raise Exception('step is fail. reason = ' + str(var_reason) + '\nlogurl = ' + str(logfile))

            time.sleep(60)

    def execute_steps(self, jobflowid, target):
        var_jobflowid = jobflowid
        var_target = target
        self.logger.info("EMRプログラム実行")
        self.logger.info("clusterid = " + var_jobflowid)
        self.logger.info("実行対象 = " + var_target)

        emr_client = boto3.client('emr')
        try:
            # EMRプログラム実行処理
            var_response = emr_client.add_job_flow_steps(
                JobFlowId=var_jobflowid,
                Steps=self.var_conf[var_target]
            )

            var_stepId = var_response['StepIds'][0]
            self.logger.info("step id = " + var_stepId)
            self.check_status(emr_client, var_jobflowid, var_stepId)
            self.logger.info("EMRプログラム実行正常終了")
            # エラーが発生した場合、例外処理を実施
        except Exception as e:
            self.logger.error(e)
            raise Exception('EMRプログラム実行異常終了')
