from airflow import DAG
from airflow.operators import HadoopStreamingOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'wangjunwei',
    'depends_on_past': False,
    'email': ['name@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.now(),
}


dag = DAG('hadoop_test', default_args=default_args, schedule_interval=None)

hadoop_input = [
    '/hdfs/path/to/folder',
    '/hdfs/path/to/file',
]

op = HadoopStreamingOperator(
    task_id='hadoop_streaming_test',
    mapper='"python mapper.py"',
    input=hadoop_input,
    output='/hdfs/ouput/path',
    files='mapper.py',
    archives='/path/to/python/archives',
    dag=dag)
