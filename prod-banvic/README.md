# Projeto de Engenharia de Dados - BanVic

Este projeto é uma Prova de Conceito (PoC) para a certificação de Engenharia de Dados da Indicium, demonstrando a construção de uma infraestrutura e um pipeline de dados para o Banco Vitória S.A. (BanVic).

## 1. Arquitetura da Solução

**(Observação: Substitua a URL abaixo pela URL da imagem do seu diagrama de arquitetura. Você pode fazer o upload do diagrama no seu repositório GitHub e usar o link aqui.)**

![Arquitetura do Projeto](https://via.placeholder.com/800x400.png?text=Insira+o+Diagrama+da+Arquitetura+Aqui)

A arquitetura desta solução foi desenhada para ser moderna, escalável e replicável, utilizando ferramentas de código aberto amplamente adotadas no mercado de engenharia de dados.

*   **Conteinerização e IaC:** O ambiente é totalmente gerenciado por `Docker` e `Kubernetes` (usando `Kind` para o cluster local) e provisionado via `Helm`. Isso garante que qualquer pessoa possa replicar a infraestrutura de forma idêntica.
*   **Orquestração:** O `Apache Airflow` é o orquestrador central, responsável por agendar, executar e monitorar os pipelines de dados.
*   **Ingestão de Dados (ELT):** O `Meltano` foi escolhido como a ferramenta de ELT para extrair dados de diversas fontes (neste caso, arquivos CSV) e carregá-los em um destino centralizado.
*   **Armazenamento:** O `PostgreSQL` é utilizado como Data Warehouse para armazenar os dados brutos e prepará-los para análise.

## 2. Estratégia de Ingestão

A estratégia de ingestão segue o paradigma **ELT (Extract, Load, Transform)**:

1.  **Extract & Load:** O Meltano, através do `tap-csv`, extrai os dados dos arquivos `.csv` localizados no diretório `data/raw/`. Em seguida, o `target-postgres` carrega esses dados diretamente em um schema no banco de dados PostgreSQL. Esta etapa é orquestrada pela DAG `pipeline_ingestao_banvic` no Airflow.
2.  **Transform:** As transformações dos dados (limpeza, modelagem, enriquecimento) não fazem parte do escopo inicial de ingestão e devem ser realizadas posteriormente, utilizando ferramentas como dbt, Pandas ou Spark, criando tabelas modeladas e prontas para o consumo pela área de negócios.

Este processo garante que os dados brutos estejam sempre disponíveis no Data Warehouse para auditoria e reprocessamento, se necessário.

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
    A imagem customizada do Airflow inclui o projeto Meltano.
    ```bash
    docker build -t banvic-airflow-custom:v1 .
    ```

4.  **Carregue a Imagem no Cluster Kind**
    ```bash
    kind load docker-image banvic-airflow-custom:v1 --name banvic-cluster
    ```

5.  **Configure as Variáveis de Ambiente**
    Crie um arquivo `.env` na raiz do projeto. O Meltano e o Airflow o utilizarão para configurar a conexão com o banco de dados.
    ```env
    # Credenciais para o PostgreSQL
    DB_HOST=airflow-postgresql
    DB_PORT=5432
    DB_USER=postgres
    DB_PASSWORD=postgres
    DB_DATABASE=postgres
    ```
    **(Observação: Estas são credenciais de desenvolvimento. Em um ambiente de produção, use senhas fortes e um sistema de gerenciamento de segredos.)**

6.  **Instale e Configure o Airflow com Helm**
    Navegue até o diretório do Kubernetes e atualize a imagem no `values.yaml` se necessário.
    ```bash
    cd kubernetes
    helm upgrade --install airflow apache-airflow/airflow -n airflow --create-namespace -f values.yaml
    ```

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
    Você pode se conectar ao banco de dados para verificar se as tabelas foram criadas e populadas.
    ```bash
    # Encaminha a porta do PostgreSQL
    kubectl port-forward svc/airflow-postgresql 5432:5432 -n airflow
    
    # Agora, conecte-se usando sua ferramenta de banco de dados preferida
    # Host: localhost, Port: 5432, User/Pass: postgres/postgres
    ```
