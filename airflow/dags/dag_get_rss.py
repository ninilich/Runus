from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from cfg_dags import FEEDS_LIST, START_DATE

# Function to create DAGs
def create_dag(dag_id,
               schedule,
               default_args,
               url,
               src_code):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              catchup=False)

    command = 'python3 /runus/python/get_rss.py ' + url + ' ' + src_code

    # printing for logs
    print('Command: ' + command)

    with dag:
        t1 = BashOperator(
            task_id="get_rss",
            bash_command=command,
            dag=dag
        )

    return dag


# build a dag for each RSS-source
for source in FEEDS_LIST:

    url, src_code, schedule_interval = source
    dag_id = f'get_rss_{src_code}'

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": START_DATE,
        "email": ["airflow@airflow.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        'sla': timedelta(minutes=3)
    }

    globals()[dag_id] = create_dag(dag_id,
                                   schedule_interval,
                                   default_args,
                                   url,
                                   src_code)