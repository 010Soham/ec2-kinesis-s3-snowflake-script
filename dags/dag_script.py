import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
#from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import datetime

SNOWFLAKE_CONN_ID = 'snowflake_conn'

default_args = {
    "owner": "snowflakekinesis-admuser-apsouth1",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "customer_orders_datapipeline_dynamic_batch_id",
    default_args=default_args,
    description="Runs data pipeline",
    schedule_interval=None,
    is_paused_upon_creation=False,
)

bash_task = BashOperator(task_id="run_bash_echo", bash_command="echo 1", dag=dag)

post_task = BashOperator(task_id="post_dbt", bash_command="echo 0", dag=dag)

batch_id =str(datetime.datetime.now().strftime("%Y%m%d%H%M"))
print("BATCH_ID = " + batch_id)


task_customer_landing_to_processing = BashOperator(
 task_id="customer_landing_to_processing",
 bash_command='aws s3 mv s3://snowflakekinesis-admuser-apsouth1/firehose/customers/landing/ s3://snowflakekinesis-admuser-apsouth1/firehose/customers/processing/{0}/ --recursive'.format(batch_id),
 dag=dag
)
 
task_customers_processing_to_processed = BashOperator(
 task_id="customer_processing_to_processed",
 bash_command='aws s3 mv s3://snowflakekinesis-admuser-apsouth1/firehose/customers/processing/{0}/ s3://snowflakekinesis-admuser-apsouth1/firehose/customers/processed/{0}/ --recursive'.format(batch_id),
 dag=dag
)
 
task_orders_landing_to_processing = BashOperator(
 task_id="orders_landing_to_processing",
 bash_command='aws s3 mv s3://snowflakekinesis-admuser-apsouth1/firehose/orders/landing/ s3://snowflakekinesis-admuser-apsouth1/firehose/orders/processing/{0}/ --recursive'.format(batch_id),
 dag=dag
)
 
task_orders_processing_to_processed = BashOperator(
 task_id="orders_processing_to_processed",
 bash_command='aws s3 mv s3://snowflakekinesis-admuser-apsouth1/firehose/orders/processing/{0}/ s3://snowflakekinesis-admuser-apsouth1/firehose/orders/processed/{0}/ --recursive'.format(batch_id),
 dag=dag
)
 
snowflake_query_orders = [
    """copy into PRO_DB.PRO_SCHEMA.ORDERS_RAW
(O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT, BATCH_ID) from
( select t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,'{0}' from @ORDERS_RAW_STAGE t);""".format(batch_id),
]

snowflake_query_customers = [
    """copy into PRO_DB.PRO_SCHEMA.CUSTOMER_RAW
(C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT, BATCH_ID) from
( select t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,'{0}' from @CUSTOMER_RAW_STAGE t);""".format(batch_id),
]


snowflake_query_customer_orders_small_transformation = [
    """insert into ORDER_CUSTOMER_DATE_PRICE (CUSTOMER_NAME, ORDER_DATE, ORDER_TOTAL_PRICE, BATCH_ID) 
select c.c_name as customer_name, o.o_orderdate as order_date,sum(o.o_totalprice) as order_total_price, c.batch_id
from orders_raw o join customer_raw c on o.o_custkey = c.C_custkey and o.batch_id = c.batch_id
where o_orderstatus= 'F'
group by c_name,o_orderdate, c.batch_id
order by o_orderdate;""",
]




snowflake_orders_sql_str = SQLExecuteQueryOperator(
    task_id='snowflake_raw_insert_order',
    dag=dag,
    conn_id=SNOWFLAKE_CONN_ID,
    sql=snowflake_query_orders,
)

snowflake_customers_sql_str = SQLExecuteQueryOperator(
    task_id='snowflake_raw_insert_customers',
    dag=dag,
    sql=snowflake_query_customers,
    conn_id=SNOWFLAKE_CONN_ID,
   
)

snowflake_order_customers_small_transformation = SQLExecuteQueryOperator(
    task_id='snowflake_order_customers_small_transformation',
    dag=dag,
    sql=snowflake_query_customer_orders_small_transformation,
    conn_id=SNOWFLAKE_CONN_ID,
    
)

[task_orders_landing_to_processing >> snowflake_orders_sql_str >> task_orders_processing_to_processed,task_customer_landing_to_processing >> snowflake_customers_sql_str >> task_customers_processing_to_processed] >> snowflake_order_customers_small_transformation >> post_task