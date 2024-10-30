from airflow import DAG
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 20),  # 使用过去的日期
    'retries': 0,
}

dag = DAG(
    'dqtool_mwaa_master_read',
    default_args=default_args,
    description='メーカ、関係者、郵便番号データ、顧客マスタ、商品マスタデータを取り込むプロセス',
    schedule_interval=None,  # 立即运行
    max_active_runs=1,
)

# メーカ
maker_master_task = StepFunctionStartExecutionOperator(
    task_id='start_maker_master_state_machine',
    state_machine_arn='arn:aws:states:ap-northeast-1:324836110893:stateMachine:dqtool_state_machine_maker_master',
    trigger_rule = "all_success",
    dag=dag,
)

# 関係者
related_person_master_task = StepFunctionStartExecutionOperator(
    task_id='start_related_person_master_state_machine',
    state_machine_arn='arn:aws:states:ap-northeast-1:324836110893:stateMachine:dqtool_state_machine_related_person_master',
    trigger_rule = "all_success",
    dag=dag,
)

# 郵便番号データ
post_code_data_task = StepFunctionStartExecutionOperator(
    task_id='start_post_code_data_state_machine',
    state_machine_arn='arn:aws:states:ap-northeast-1:324836110893:stateMachine:dqtool_state_machine_post_code_data',
    trigger_rule = "all_success",
    dag=dag,
)

# 顧客
customer_master_task = StepFunctionStartExecutionOperator(
    task_id='start_customer_master_state_machine',
    state_machine_arn='arn:aws:states:ap-northeast-1:324836110893:stateMachine:dqtool_state_machine_customer_master',
    trigger_rule = "all_success",
    dag=dag,
)

# 商品
product_master_task = StepFunctionStartExecutionOperator(
    task_id='start_product_master_state_machine',
    state_machine_arn='arn:aws:states:ap-northeast-1:324836110893:stateMachine:dqtool_state_machine_product_master',
    trigger_rule = "all_success",
    dag=dag,
)
maker_master_task.set_downstream(related_person_master_task)
related_person_master_task.set_downstream(post_code_data_task)
post_code_data_task.set_downstream(customer_master_task)
customer_master_task.set_downstream(product_master_task)
# タスク実行順番
chain(maker_master_task, related_person_master_task, post_code_data_task, customer_master_task, product_master_task )
