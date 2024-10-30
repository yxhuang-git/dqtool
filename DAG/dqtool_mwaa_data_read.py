from airflow import DAG
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 20),
    'depends_on_past': True,
    'retries': 0,
}

dag = DAG(
    'dqtool_mwaa_data_read',
    default_args=default_args,
    description='予算、トランザクションデータ、降水状況、気温状況、返品実績を取り込んで、集計を行うプロセス',
    schedule_interval=None,
    max_active_runs=1,
)

# 予算
budget_master_task = StepFunctionStartExecutionOperator(
    task_id='start_budget_master_state_machine',
    state_machine_arn='arn:aws:states:ap-northeast-1:324836110893:stateMachine:dqtool_state_machine_budget_master',
    trigger_rule = "all_success",
    dag=dag,
)

# トランザクションデータ
transaction_data_task = StepFunctionStartExecutionOperator(
    task_id='start_transaction_data_state_machine',
    state_machine_arn='arn:aws:states:ap-northeast-1:324836110893:stateMachine:dqtool_state_machine_transaction_data',
    trigger_rule = "all_success",
    dag=dag,
)

# 降水状況
meteorology_precipitation_task = StepFunctionStartExecutionOperator(
    task_id='start_meteorology_precipitation_state_machine',
    state_machine_arn='arn:aws:states:ap-northeast-1:324836110893:stateMachine:dqtool_state_machine_meteorology_precipitation',
    trigger_rule = "all_success",
    dag=dag,
)

# 気温状況
meteorology_temperature_task = StepFunctionStartExecutionOperator(
    task_id='start_meteorology_temperature_state_machine',
    state_machine_arn='arn:aws:states:ap-northeast-1:324836110893:stateMachine:dqtool_state_machine_meteorology_temperature',
    trigger_rule = "all_success",
    dag=dag,
)

# 返品実績
returns_task = StepFunctionStartExecutionOperator(
    task_id='start_returns_state_machine',
    state_machine_arn='arn:aws:states:ap-northeast-1:324836110893:stateMachine:dqtool_state_machine_returns',
    trigger_rule = "all_success",
    dag=dag,
)

# 集計
dwh_to_mart_task = StepFunctionStartExecutionOperator(
    task_id='start_dwh_to_mart_state_machine',
    state_machine_arn='arn:aws:states:ap-northeast-1:324836110893:stateMachine:dqtool_state_machine_dwh_to_mart',
    trigger_rule = "all_success",
    dag=dag,
)

budget_master_task.set_downstream(transaction_data_task)
transaction_data_task.set_downstream(meteorology_precipitation_task)
meteorology_precipitation_task.set_downstream(meteorology_temperature_task)
meteorology_temperature_task.set_downstream(returns_task)
returns_task.set_downstream(dwh_to_mart_task)
# タスク実行順番
chain(budget_master_task, transaction_data_task, meteorology_precipitation_task, meteorology_temperature_task, returns_task, dwh_to_mart_task )
