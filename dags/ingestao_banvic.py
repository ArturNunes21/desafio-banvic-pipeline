from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'artur',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_ingestao_banvic',
    default_args=default_args,
    description='Pipeline que dispara a ingestão do Meltano (CSV para Postgres) de dentro do Kubernetes',
    schedule_interval=None,  # Disparo manual por enquanto
    catchup=False,
    tags=['meltano', 'banvic'],
) as dag:

    # Task que executa o comando do Meltano usando o venv interno do container
    executar_meltano = BashOperator(
        task_id='executar_meltano_elt',
        bash_command='cd /opt/airflow/prod-banvic && /home/airflow/meltano_env/bin/meltano install && /home/airflow/meltano_env/bin/meltano run tap-csv target-postgres',
    )
    
    executar_meltano