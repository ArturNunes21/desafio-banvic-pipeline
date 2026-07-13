# Projeto de Engenharia de Dados - BanVic

Este projeto é uma Prova de Conceito (PoC) para a certificação de Engenharia de Dados da Indicium, demonstrando a construção de uma infraestrutura e um pipeline de dados para o Banco Vitória S.A. (BanVic).

## 1. Arquitetura da Solução

![Arquitetura do Projeto](docs/diagrama-arquitetura.svg)

O diagrama acima representa o fluxo completo da solução:

1. **Fonte:** 7 arquivos CSV do ERP simulado em `data/raw/`.
2. **Orquestração:** a DAG `pipeline_ingestao_banvic` no Apache Airflow valida a presença dos arquivos (FileSensors), limpa as tabelas de destino e dispara o Meltano.
3. **Ingestão (EL):** o Meltano extrai via `tap-csv` e carrega via `target-postgres`.
4. **Destino:** PostgreSQL armazena as 7 tabelas brutas para consumo por analistas.

A arquitetura desta solução foi desenhada para ser moderna, escalável e replicável, utilizando ferramentas de código aberto amplamente adotadas no mercado de engenharia de dados.

*   **Conteinerização e IaC:** O ambiente é totalmente gerenciado por `Docker` e `Kubernetes` (usando `Kind` para o cluster local) e provisionado via `Helm`. Isso garante que qualquer pessoa possa replicar a infraestrutura de forma idêntica.
*   **Orquestração:** O `Apache Airflow` é o orquestrador central, responsável por agendar, executar e monitorar os pipelines de dados.
*   **Ingestão de Dados (EL):** O `Meltano` foi escolhido como a ferramenta de EL para extrair dados de diversas fontes (neste caso, arquivos CSV) e carregá-los em um destino centralizado.
*   **Armazenamento:** O `PostgreSQL` é utilizado como Data Warehouse para armazenar os dados brutos e prepará-los para análise.

## 2. Estratégia de Ingestão (EL)

A estratégia de ingestão segue o paradigma **EL (Extract, Load)**:

1.  **Extract & Load:** O Meltano, através do `tap-csv`, extrai os dados dos 7 arquivos `.csv` brutos localizados no diretório `data/raw/`. Em seguida, o `target-postgres` carrega esses dados no schema `raw_banvic` do PostgreSQL. O objetivo é disponibilizar as 7 tabelas brutas de forma confiável para consumo. Esta etapa é orquestrada pela DAG `pipeline_ingestao_banvic` no Airflow.

Este processo garante que os dados brutos estejam sempre disponíveis no Data Warehouse para auditoria e reprocessamento, se necessário. Qualquer transformação, modelagem ou enriquecimento de dados será responsabilidade de camadas analíticas subsequentes, utilizando as tabelas brutas disponibilizadas.

## 3. Como Executar o Projeto Localmente

Siga os passos abaixo para configurar e executar o ambiente e o pipeline de dados.

### Pré-requisitos

*   Docker
*   Kubernetes (Kind)
*   Helm
*   `kubectl`

### Passo a Passo

1.  **Clone o Repositório**
    ```bash
    git clone <URL_DO_SEU_REPOSITORIO>
    cd <NOME_DO_SEU_REPOSITORIO>
    ```

2.  **Crie o Cluster Kubernetes Local**
    ```bash
    # (Se ainda não tiver o cluster do desafio)
    kind create cluster --name banvic-cluster
    ```

3.  **Construa a Imagem Docker Personalizada**
    A imagem customizada inclui o Airflow, o Meltano (com plugins já instalados) e os dados CSV empacotados em `data/raw/`. Não é necessário volume persistente — tudo roda a partir da imagem.
    ```bash
    docker build -t banvic-airflow-custom:v1 .
    ```

4.  **Carregue a Imagem no Cluster Kind**
    ```bash
    kind load docker-image banvic-airflow-custom:v1 --name banvic-cluster
    ```

5.  **Configure as Variáveis de Ambiente e Secrets**

    **Desenvolvimento local / Meltano:** copie o template e preencha os valores:

    ```bash
    cp .env.example .env
    ```

    O arquivo `.env` é ignorado pelo Git. Variáveis necessárias:

    | Variável | Descrição |
    |----------|-----------|
    | `DB_HOST` | Host do PostgreSQL (`airflow-postgresql` no cluster) |
    | `DB_PORT` | Porta (`5432`) |
    | `DB_USER` | Usuário do banco |
    | `DB_PASSWORD` | Senha do banco |
    | `DB_DATABASE` | Nome do banco |
    | `WEBSERVER_SECRET_KEY` | Chave Flask do Airflow Webserver |
    | `AIRFLOW_CONN_POSTGRES_DEFAULT` | URI da conexão Airflow (mesma senha de `DB_PASSWORD`) |

    Valores padrão para o PostgreSQL do Helm: `airflow-postgresql`, `5432`, `postgres`, `postgres`, `postgres`.

    **Kubernetes:** antes do Helm install, crie o Secret a partir do template:

    ```bash
    cd kubernetes
    cp banvic-secrets.example.yaml banvic-secrets.yaml
    # Edite banvic-secrets.yaml com suas credenciais
    kubectl apply -f banvic-secrets.yaml
    ```

    O [`meltano.yml`](meltano.yml) lê `$DB_HOST`, `$DB_PASSWORD`, etc. dos pods do Airflow. Nenhuma credencial fica hardcoded no código versionado.

6.  **Instale e Configure o Airflow com Helm**
    Navegue até o diretório do Kubernetes e atualize a imagem no `values.yaml` se necessário.
    ```bash
    cd kubernetes
    helm upgrade --install airflow apache-airflow/airflow -n airflow --create-namespace -f values.yaml
    ```
    O arquivo [`kubernetes/values.yaml`](kubernetes/values.yaml) injeta as variáveis `DB_*` nos pods e carrega a conexão `postgres_default` e a chave do Webserver a partir do Secret `banvic-secrets`. Não é necessário criar a conexão manualmente na UI do Airflow.

7.  **Acesse a UI do Airflow**
    Aguarde os pods ficarem no estado "Running".
    ```bash
    kubectl get pods -n airflow
    ```
    Em seguida, abra um túnel para a interface do Airflow.
    ```bash
    kubectl port-forward svc/airflow-webserver 8888:8080 -n airflow
    ```
    Abra `http://localhost:8888` no seu navegador (usuário/senha padrão: `airflow`/`airflow`).

8.  **Execute a DAG**
    Na UI do Airflow, ative e dispare a DAG `pipeline_ingestao_banvic`. Verifique os logs para confirmar que a execução foi bem-sucedida.

9.  **Verifique os Dados no PostgreSQL**
    Você pode se conectar ao banco de dados para verificar se as tabelas foram criadas e populadas no schema `raw_banvic`.
    ```bash
    # Encaminha a porta do PostgreSQL
    kubectl port-forward svc/airflow-postgresql 5432:5432 -n airflow
    
    # Agora, conecte-se usando sua ferramenta de banco de dados preferida
    # Host: localhost, Port: 5432, User/Pass: postgres/postgres
    # Schema: raw_banvic
    ```

## 4. Monitoramento e Resiliência

O pipeline implementa estratégias básicas de monitoramento e tratamento de falhas:

### Resiliência (Airflow)

| Mecanismo | Configuração | Onde |
|-----------|--------------|------|
| Retries | 2 tentativas | `default_args` da DAG |
| Retry delay | 5 minutos entre tentativas | `default_args` da DAG |
| Idempotência | `DROP TABLE IF EXISTS` no schema `raw_banvic` antes de cada carga | task `limpar_tabelas_brutas` |
| Validação de fonte | 7 FileSensors aguardam os CSVs | TaskGroup `validar_arquivos_csv` |

### Monitoramento

1. **UI do Airflow** — acompanhe o status das tasks (verde/vermelho), duração e histórico de execuções em `http://localhost:8888`.
2. **Logs das tasks** — clique em cada task na UI e abra "Log" para ver saída do Meltano e erros de conexão.
3. **Saúde dos pods** — verifique se todos os pods estão `Running`:
   ```bash
   kubectl get pods -n airflow
   kubectl logs -n airflow <nome-do-pod-scheduler> --tail=50
   ```
4. **Validação dos dados** — após execução bem-sucedida, confira contagens no PostgreSQL:
   ```sql
   SELECT table_name FROM information_schema.tables
   WHERE table_schema = 'raw_banvic' ORDER BY table_name;
   ```

Em caso de falha persistente após todas as retries, investigue os logs da task que falhou (sensor, limpeza ou Meltano) antes de reexecutar a DAG.
