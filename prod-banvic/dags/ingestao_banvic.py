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
    schedule_interval=None,  # Disparo manual por enquanto
    catchup=False,
    tags=['meltano', 'banvic'],
) as dag:

    # ETAPA 1: Grupo de Sensores para garantir que os 7 arquivos CSV existem
    with TaskGroup("validar_arquivos_csv") as sensor_group:
        arquivos = [
            "agencias", "clientes", "colaborador_agencia", 
            "colaboradores", "contas", "propostas_credito", "transacoes"
        ]
        
        for arquivo in arquivos:
            FileSensor(
                task_id=f'aguardar_{arquivo}_csv',
                filepath=f'/opt/airflow/prod-banvic/data/raw/{arquivo}.csv',  # Ajuste o caminho se necessário dentro do container
                fs_conn_id='fs_default',  # ID da conexão de File System no Airflow
                poke_interval=30,  # Checa a cada 30 segundos
                timeout=600,       # Dá timeout após 10 minutos
            )

    # ETAPA 2: Limpeza das tabelas para evitar dados duplicados (Idempotência)
    limpar_tabelas_postgres = SQLExecuteQueryOperator(
        task_id='limpar_tabelas_brutas',
        conn_id='postgres_default',  # ID da conexão do seu Postgres no Airflow
        sql="""
        TRUNCATE TABLE public.agencias CASCADE;
        TRUNCATE TABLE public.clientes CASCADE;
        TRUNCATE TABLE public.colaborador_agencia CASCADE;
        TRUNCATE TABLE public.colaboradores CASCADE;
        TRUNCATE TABLE public.contas CASCADE;
        TRUNCATE TABLE public.propostas_credito CASCADE;
        TRUNCATE TABLE public.transacoes CASCADE;
        """,
    )

    # ETAPA 3: Ingestão com o Meltano
    executar_meltano = BashOperator(
        task_id='executar_meltano_elt',
        bash_command='cd /opt/airflow/prod-banvic && /home/airflow/meltano_env/bin/meltano install && /home/airflow/meltano_env/bin/meltano run tap-csv target-postgres',
    )
    
    # Definição do fluxo do pipeline (Orquestração)
    sensor_group >> limpar_tabelas_postgres >> executar_meltano