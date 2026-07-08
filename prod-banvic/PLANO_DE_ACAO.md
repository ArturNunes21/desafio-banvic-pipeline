# Plano de Ação: Foco em Engenharia de Dados para o Projeto BanVic

Este documento detalha o plano de reestruturação do projeto BanVic para focar estritamente em Engenharia de Dados: Ingestão (EL), Orquestração e Resiliência. O objetivo é abandonar a modelagem analítica e entregar um pipeline de dados robusto e confiável que disponibilize as 7 tabelas brutas do BanVic no PostgreSQL para consumo de analistas.

---

## 1. Idempotência: Garantindo Cargas de Dados Consistentes

**Objetivo:** Evitar a duplicação de dados no PostgreSQL, mesmo que o pipeline seja executado múltiplas vezes. A solução é garantir que cada execução do Meltano resulte no mesmo estado final.

**Estratégia:** Utilizaremos o método de carga `truncate-load`. A cada execução da DAG, as tabelas de destino no PostgreSQL serão primeiramente truncadas (esvaziadas) antes que o Meltano insira os novos dados extraídos dos arquivos CSV. Isso garante uma recarga completa e limpa a cada vez.

### ✔️ To-Dos:

- [ ] **Configurar o Loader no Meltano:** No arquivo `meltano.yml`, vamos configurar o `target-postgres` para usar um schema dedicado aos dados brutos (ex: `raw_banvic`) e garantir que ele possa realizar a limpeza. A idempotência será alcançada na própria DAG do Airflow.
- [ ] **Criar Tarefa de Limpeza no Airflow:** Antes da tarefa que executa `meltano run ...`, adicione uma tarefa `PostgresOperator` que execute o comando `TRUNCATE TABLE raw_banvic.tabela_exemplo CASCADE;` para cada uma das 7 tabelas. Isso torna a operação explícita e fácil de monitorar.

**Exemplo de Task de Limpeza (Airflow):**

```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ... dentro da sua DAG ...

truncate_tables_task = PostgresOperator(
    task_id='truncate_raw_tables',
    postgres_conn_id='sua_postgres_conn_id', # ID da conexão do Airflow com o Postgres
    sql="""
    TRUNCATE TABLE raw_banvic.agencias CASCADE;
    TRUNCATE TABLE raw_banvic.clientes CASCADE;
    TRUNCATE TABLE raw_banvic.colaborador_agencia CASCADE;
    TRUNCATE TABLE raw_banvic.colaboradores CASCADE;
    TRUNCATE TABLE raw_banvic.contas CASCADE;
    TRUNCATE TABLE raw_banvic.propostas_credito CASCADE;
    TRUNCATE TABLE raw_banvic.transacoes CASCADE;
    """,
)

# A task do Meltano executará depois desta
truncate_tables_task >> meltano_task
```

---

## 2. Sensores de Arquivos: Ingestão Orientada por Eventos

**Objetivo:** Iniciar o pipeline de ingestão apenas quando os 7 arquivos CSV estiverem disponíveis no diretório `data/raw/`, evitando execuções desnecessárias e falhas.

**Estratégia:** Implementaremos `FileSensor` no Airflow. Criaremos um sensor para cada arquivo CSV. Para manter a DAG organizada, agruparemos esses 7 sensores em um `TaskGroup`. A tarefa de ingestão do Meltano só será acionada após todos os sensores confirmarem a presença dos seus respectivos arquivos.

### ✔️ To-Dos:

- [ ] **Implementar `FileSensor`:** Na DAG de ingestão, crie uma task `FileSensor` para cada um dos 7 arquivos CSV.
- [ ] **Apontar para o Caminho Correto:** Configure o `filepath` do sensor para o caminho onde os arquivos CSV são esperados dentro do ambiente do Airflow (ex: `/path/to/project/data/raw/`).
- [ ] **Usar `TaskGroup`:** Organize os 7 sensores dentro de um `TaskGroup` para melhor visualização na UI do Airflow.
- [ ] **Definir a Dependência:** A `TaskGroup` de sensores deve ser uma dependência para a próxima etapa (a tarefa de limpeza e, em seguida, a de ingestão do Meltano).

**Exemplo de Implementação (Airflow):**

```python
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

# ... dentro da sua DAG ...

with TaskGroup("file_sensors") as sensor_group:
    files = [
        "agencias", "clientes", "colaborador_agencia", 
        "colaboradores", "contas", "propostas_credito", "transacoes"
    ]
    for file in files:
        FileSensor(
            task_id=f'wait_for_{file}_csv',
            filepath=f'/opt/airflow/dags/repo/data/raw/{file}.csv', # Ajuste o caminho conforme sua estrutura
            fs_conn_id='your_filesystem_conn_id', # Conexão com o filesystem
            poke_interval=30, # Verificar a cada 30 segundos
            timeout=600,      # Timeout após 10 minutos
        )

# A DAG fluirá assim:
sensor_group >> truncate_tables_task >> meltano_task
```

---

## 3. Tratamento de Falhas: Construindo um Pipeline Resiliente

**Objetivo:** Garantir que o pipeline possa se recuperar de falhas transitórias (ex: instabilidade de rede, picos de uso do banco de dados) e notificar sobre erros persistentes.

**Estratégia:** Configuraremos políticas de `retry` (reexecução) e `delay` (intervalo) diretamente nos `default_args` da DAG do Airflow. Isso aplicará a política a todas as tarefas, garantindo um comportamento de resiliência uniforme.

### ✔️ To-Dos:

- [ ] **Configurar `default_args` na DAG:** Defina os seguintes parâmetros no dicionário `default_args` da sua DAG:
    - `retries`: Número de tentativas de reexecução em caso de falha (sugestão: 2).
    - `retry_delay`: Tempo de espera entre as tentativas (sugestão: `timedelta(minutes=5)`).
- [ ] **(Opcional) Configurar Alertas:** Configure `email_on_failure` para `True` e preencha os campos de e-mail para receber notificações quando uma tarefa falhar após todas as tentativas.

**Exemplo de `default_args` (Airflow):**

```python
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False, # Mude para True e configure o SMTP do Airflow se desejar emails
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_ingestao_banvic',
    default_args=default_args,
    # ... resto da configuração da DAG
)
```

---

## 4. Organização e Limpeza do Projeto

**Objetivo:** Remover artefatos desnecessários do escopo anterior e garantir que a estrutura do projeto e a documentação reflitam o novo foco em Engenharia de Dados.

### ✔️ Checklist de Tarefas de Limpeza:

- [ ] **Remover Modelagem dbt:**
    - [ ] Apague o diretório `transform/models/`.
    - [ ] Apague o arquivo `transform/dbt_project.yml` (se existir).
    - [ ] Remova a dependência do `dbt` do Meltano (se `dbt` foi adicionado como plugin no `meltano.yml`).
- [ ] **Limpar Diretórios Não Utilizados:**
    - [ ] Apague o conteúdo do diretório `analyze/`. Mantenha o `.gitkeep` se o diretório for útil no futuro.
    - [ ] Apague o conteúdo do diretório `notebook/` se não houver notebooks de exploração relevantes.
- [ ] **Atualizar a Documentação Principal (`README.md`):**
    - [ ] **Arquitetura:** Atualize o diagrama e a descrição para refletir a ausência da camada de transformação (dbt). O foco é **EL (Extract, Load)**.
    - [ ] **Estratégia de Ingestão:** Reescreva a seção "Estratégia de Ingestão", removendo a menção à etapa "Transform". Deixe claro que o objetivo do projeto é entregar os dados brutos no PostgreSQL.
    - [ ] **Limpeza Geral:** Revise todo o `README.md` e remova termos como "tabelas fato/dimensão", "marts", "analytics" e "dbt".
- [ ] **Revisar `meltano.yml`:**
    - [ ] Garanta que apenas os plugins necessários (`tap-csv`, `target-postgres`) estejam configurados. Remova quaisquer outros (como `dbt` ou analisadores).
- [ ] **Salvar este Plano:**
    - [ ] Confirme que este arquivo (`PLANO_DE_ACAO.md`) foi salvo na raiz do projeto.
