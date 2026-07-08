from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'artur',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_ingestao_banvic',
    default_args=default_args,
    description='Pipeline resiliente que valida, limpa e dispara a ingestão do Meltano',
    schedule_interval=None,
    catchup=False,
    tags=['meltano', 'banvic'],
) as dag:

    with TaskGroup("validar_arquivos_csv") as sensor_group:
        arquivos = [
            "agencias", "clientes", "colaborador_agencia", 
            "colaboradores", "contas", "propostas_credito", "transacoes"
        ]
        
        for arquivo in arquivos:
            FileSensor(
                task_id=f'aguardar_{arquivo}_csv',
                filepath=f'/opt/airflow/prod-banvic/data/raw/{arquivo}.csv',
                fs_conn_id='fs_default',
                poke_interval=30,
                timeout=240,       # Dá timeout após 4 minutos
            )

    limpar_tabelas_postgres = SQLExecuteQueryOperator(
        task_id='limpar_tabelas_brutas',
        conn_id='postgres_default',
        sql="""
        DROP TABLE IF EXISTS public.agencias CASCADE;
        DROP TABLE IF EXISTS public.clientes CASCADE;
        DROP TABLE IF EXISTS public.colaborador_agencia CASCADE;
        DROP TABLE IF EXISTS public.colaboradores CASCADE;
        DROP TABLE IF EXISTS public.contas CASCADE;
        DROP TABLE IF EXISTS public.propostas_credito CASCADE;
        DROP TABLE IF EXISTS public.transacoes CASCADE;
        """,
    )

    executar_meltano = BashOperator(
        task_id='executar_meltano_elt',
        bash_command='cd /opt/airflow/prod-banvic && /home/airflow/meltano_env/bin/meltano install && /home/airflow/meltano_env/bin/meltano run tap-csv target-postgres',
    )
    
    sensor_group >> limpar_tabelas_postgres >> executar_meltano