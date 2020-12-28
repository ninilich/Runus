from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta
from cfg_dags import START_DATE


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": START_DATE,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    'sla': timedelta(minutes=15)
}

dag_params = {
    "dag_id": 'Data_Processing',
    "schedule_interval": "40 0-22/2 * * *",
    "catchup": False
}

with DAG(default_args=default_args, **dag_params) as dag:

    xml_parse = BashOperator(
        task_id="xml_parse",
        bash_command='python3 /runus/python/parse_xml.py',
        dag=dag
    )

    fill_entity = PostgresOperator(
        task_id='fill_entity',
        postgres_conn_id='postgres_runus',
        sql='CALL public.fill_entity();',
        autocommit=True
    )

    fill_news = PostgresOperator(
        task_id='fill_news',
        postgres_conn_id='postgres_runus',
        sql='CALL public.fill_news();',
        autocommit=True
    )

    fill_news_x_entity = PostgresOperator(
        task_id='fill_entity_x_news',
        postgres_conn_id='postgres_runus',
        sql='CALL public.fill_news_x_entity();',
        autocommit=True
    )

    truncate_stage = PostgresOperator(
        task_id='truncate_stage',
        postgres_conn_id='postgres_runus',
        sql='TRUNCATE public.stg_news;',
        autocommit=True
    )

    fill_dm_mentions = PostgresOperator(
        task_id='fill_dm_mentions',
        postgres_conn_id='postgres_runus',
        sql='CALL public.fill_dm_mentions();',
        autocommit=True
    )

    fill_dm_mentions_stat = PostgresOperator(
        task_id='fill_dm_mentions_stat',
        postgres_conn_id='postgres_runus',
        sql='CALL public.fill_dm_mentions_stat();',
        autocommit=True
    )

    xml_parse >> (fill_entity, fill_news)
    (fill_entity, fill_news) >> fill_news_x_entity
    fill_news_x_entity >> truncate_stage
    fill_news_x_entity >> fill_dm_mentions
    fill_dm_mentions >> fill_dm_mentions_stat