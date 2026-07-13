# Correções e Checklist de Entrega — BanVic

Documento gerado a partir da auditoria do projeto contra os requisitos de [`desafio.txt`](desafio.txt), [`PROJECT_CONTEXT.md`](PROJECT_CONTEXT.md) e [`PLANO_DE_ACAO.md`](PLANO_DE_ACAO.md).

**Data da auditoria:** Julho/2026  
**Status geral:** ~80% completo — núcleo técnico pronto, entrega final incompleta.

---

## 1. Resumo Executivo

| Área | Status | Prioridade |
|------|--------|------------|
| Infraestrutura (Docker + K8s + Helm) | Parcialmente completo | Alta |
| Pipeline de ingestão (Meltano) | Completo | — |
| Orquestração (Airflow DAG) | Completo | — |
| Resiliência (retries + idempotência) | Completo | — |
| Sensores de arquivos | Completo | — |
| Documentação técnica | Incompleto | Crítica |
| Apresentação / vídeo | Não entregue | Crítica |
| Limpeza do escopo antigo (dbt) | Parcialmente feito | Média |
| Segurança de credenciais | Atenção necessária | Alta |

**Veredito:** o pipeline de engenharia de dados está funcional na maior parte, mas o projeto **não está pronto para submissão** enquanto faltarem o diagrama de arquitetura, o vídeo e os ajustes de conexão/segurança.

---

## 2. O que já está pronto (não refazer)

Estes itens estão implementados e alinhados ao desafio. Evite alterações desnecessárias.

- **Meltano:** [`meltano.yml`](meltano.yml) com `tap-csv` e `target-postgres`, 7 entidades com chaves primárias definidas.
- **Dados fonte:** 7 CSVs em [`data/raw/`](data/raw/) (agencias, clientes, colaborador_agencia, colaboradores, contas, propostas_credito, transacoes).
- **DAG Airflow:** [`dags/ingestao_banvic.py`](dags/ingestao_banvic.py) — `pipeline_ingestao_banvic` com FileSensors em TaskGroup, limpeza de tabelas, execução Meltano, retries (2) e retry_delay (5 min).
- **Infraestrutura base:** [`Dockerfile`](Dockerfile) (Airflow + Meltano), [`kubernetes/values.yaml`](kubernetes/values.yaml) (Helm + PostgreSQL).
- **Documentação base:** [`README.md`](README.md) com estratégia EL e passo a passo de execução.
- **Segredos locais:** [`.env`](.env) está no [`.gitignore`](.gitignore).

---

## 3. Arquitetura atual (referência)

```mermaid
flowchart LR
    csvFiles["CSV data/raw"]
    fileSensors["FileSensors Airflow"]
    truncateTask["Limpar tabelas"]
    meltano["Meltano tap-csv"]
    postgres["PostgreSQL"]
    airflow["Apache Airflow DAG"]

    csvFiles --> fileSensors
    fileSensors --> truncateTask
    truncateTask --> meltano
    meltano --> postgres
    airflow --> fileSensors
```

**Fluxo da DAG:**

```
validar_arquivos_csv (7 FileSensors)
        ↓
limpar_tabelas_brutas (DROP TABLE)
        ↓
executar_meltano_elt (meltano run tap-csv target-postgres)
```

---

## 4. Correções necessárias

Cada item segue: **Problema → Impacto → Como corrigir → Arquivos afetados**.

---

### 4.1 Diagrama de arquitetura real

| | |
|---|---|
| **Problema** | O [`README.md`](README.md) (linha 9) usa um placeholder (`via.placeholder.com`) em vez de um diagrama real. |
| **Impacto** | Entrega **obrigatória** da certificação. Sem diagrama, a documentação técnica está incompleta. |
| **Prioridade** | Crítica |

**Como corrigir:**

1. Criar o diagrama com draw.io, Excalidraw ou similar. Elementos mínimos:
   - Fonte: 7 CSVs em `data/raw/`
   - Orquestrador: Apache Airflow (Kind/Kubernetes)
   - Ingestão: Meltano (`tap-csv` → `target-postgres`)
   - Destino: PostgreSQL (Data Warehouse simulado)
2. Salvar em `docs/diagrama-arquitetura.png` (ou `.svg`).
3. Substituir no README:

```markdown
![Arquitetura do Projeto](docs/diagrama-arquitetura.png)
```

4. Remover a observação "Substitua a URL abaixo..." do README.

**Arquivos afetados:** [`README.md`](README.md), novo arquivo `docs/diagrama-arquitetura.png`

---

### 4.2 Vídeo de apresentação (3–5 minutos)

| | |
|---|---|
| **Problema** | Nenhum vídeo encontrado no repositório. |
| **Impacto** | Entrega **obrigatória** da certificação. |
| **Prioridade** | Crítica |

**Como corrigir:**

Gravar vídeo de 3–5 minutos cobrindo:

1. **Deploy do ambiente** (~1 min): Kind cluster, build da imagem, Helm install.
2. **Airflow UI** (~1 min): DAG `pipeline_ingestao_banvic` ativa e executando.
3. **Sucesso da DAG** (~1 min): tasks verdes nos logs.
4. **Verificação no PostgreSQL** (~1 min): `SELECT COUNT(*)` nas 7 tabelas.

Adicionar link do vídeo (YouTube, Google Drive, etc.) no README', 'README.md`:

```markdown
## Apresentação em Vídeo
[Link para o vídeo de demonstração](https://...)
```

**Arquivos afetados:** [`README.md`](README.md) (link externo; o vídeo não precisa estar no repo)

---

### 4.3 Conexão `postgres_default` no Airflow

| | |
|---|---|
| **Problema** | A DAG usa `conn_id='postgres_default'` em [`dags/ingestao_banvic.py`](dags/ingestao_banvic.py), mas [`kubernetes/values.yaml`](kubernetes/values.yaml) não define essa conexão. |
| **Impacto** | A task `limpar_tabelas_brutas` pode falhar em runtime por conexão inexistente. |
| **Prioridade** | Alta |

**Como corrigir (recomendado — via Helm):**

Adicionar em [`kubernetes/values.yaml`](kubernetes/values.yaml):

```yaml
env:
  - name: AIRFLOW_CONN_POSTGRES_DEFAULT
    value: "postgresql://postgres:postgres@airflow-postgresql:5432/postgres"
```

> **Nota:** Em produção, use Kubernetes Secrets em vez de valor inline.

**Alternativa:** criar a conexão manualmente na UI do Airflow (Admin → Connections) e documentar no README.

**Arquivos afetados:** [`kubernetes/values.yaml`](kubernetes/values.yaml), opcionalmente [`README.md`](README.md)

---

### 4.4 Credenciais expostas no código versionado

| | |
|---|---|
| **Problema** | Senha `postgres` hardcoded em [`meltano.yml`](meltano.yml) (linhas 42–43). `webserverSecretKey` fixa em [`kubernetes/values.yaml`](kubernetes/values.yaml) (linha 3). |
| **Impacto** | Critério de avaliação: **"Segurança e Gerenciamento de Segredos — não expor credenciais no código"**. |
| **Prioridade** | Alta |

**Como corrigir:**

1. **Meltano** — usar variáveis de ambiente no `meltano.yml`:

```yaml
config:
  host: ${DB_HOST}
  port: ${DB_PORT}
  user: ${DB_USER}
  password: ${DB_PASSWORD}
  database: ${DB_DATABASE}
  schema: raw_banvic
```

2. **Helm** — injetar variáveis a partir do `.env` ou de um Secret:

```yaml
env:
  - name: DB_HOST
    value: "airflow-postgresql"
  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: banvic-secrets
        key: db-password
```

3. Criar **`.env.example`** (sem valores reais) na raiz:

```
DB_HOST=airflow-postgresql
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=<sua_senha>
DB_DATABASE=postgres
```

4. Confirmar que `.env` permanece no `.gitignore` (já está).

**Arquivos afetados:** [`meltano.yml`](meltano.yml), [`kubernetes/values.yaml`](kubernetes/values.yaml), novo `.env.example`, [`README.md`](README.md)

---

### 4.5 Inconsistência de tag da imagem Docker

| | |
|---|---|
| **Problema** | [`README.md`](README.md) instrui build com tag `banvic-airflow-custom:v1`, mas [`kubernetes/values.yaml`](kubernetes/values.yaml) usa tag `v18`. |
| **Impacto** | Quem seguir o README pode subir a imagem errada ou inexistente no cluster. |
| **Prioridade** | Alta |

**Como corrigir:**

Padronizar uma única tag em todos os lugares (ex.: `v1`):

```bash
# README — build
docker build -t banvic-airflow-custom:v1 .
kind load docker-image banvic-airflow-custom:v1 --name banvic-cluster
```

```yaml
# kubernetes/values.yaml
images:
  airflow:
    repository: banvic-airflow-custom
    tag: v1
```

**Arquivos afetados:** [`README.md`](README.md), [`kubernetes/values.yaml`](kubernetes/values.yaml)

---

### 4.6 Schema e idempotência alinhados ao plano

| | |
|---|---|
| **Problema** | [`PLANO_DE_ACAO.md`](PLANO_DE_ACAO.md) prevê schema `raw_banvic` + `TRUNCATE`. Implementação atual usa schema `public` + `DROP TABLE`. |
| **Impacto** | Funciona, mas diverge do plano e mistura tabelas brutas com o schema padrão do banco. |
| **Prioridade** | Média |

**Como corrigir:**

1. Em [`meltano.yml`](meltano.yml), alterar `schema: public` para `schema: raw_banvic`.

2. Em [`dags/ingestao_banvic.py`](dags/ingestao_banvic.py), trocar o SQL:

```python
sql="""
CREATE SCHEMA IF NOT EXISTS raw_banvic;
TRUNCATE TABLE raw_banvic.agencias CASCADE;
TRUNCATE TABLE raw_banvic.clientes CASCADE;
TRUNCATE TABLE raw_banvic.colaborador_agencia CASCADE;
TRUNCATE TABLE raw_banvic.colaboradores CASCADE;
TRUNCATE TABLE raw_banvic.contas CASCADE;
TRUNCATE TABLE raw_banvic.propostas_credito CASCADE;
TRUNCATE TABLE raw_banvic.transacoes CASCADE;
""",
```

> **Nota:** na primeira execução, as tabelas ainda não existem — use `TRUNCATE` apenas se as tabelas já existirem, ou mantenha `DROP TABLE IF EXISTS` na primeira run e `TRUNCATE` nas subsequentes. Alternativa simples: manter `DROP TABLE IF EXISTS raw_banvic.<tabela> CASCADE` com schema explícito.

**Arquivos afetados:** [`meltano.yml`](meltano.yml), [`dags/ingestao_banvic.py`](dags/ingestao_banvic.py)

---

### 4.7 Limpeza de artefatos do escopo antigo (dbt)

| | |
|---|---|
| **Problema** | Restam `transform/logs/dbt.log`, `logs/dbt.log`. [`requirements.txt`](requirements.txt) contém libs de análise fora do escopo (pandas, jupyter, matplotlib). |
| **Impacto** | Confunde avaliadores sobre o escopo real do projeto (EL only). |
| **Prioridade** | Média |

**Como corrigir:**

1. Remover arquivos:
   - `transform/logs/dbt.log`
   - `logs/dbt.log`
   - Diretório `transform/` (se vazio após remoção)

2. Limpar [`requirements.txt`](requirements.txt) — manter apenas o necessário para o pipeline:

```
apache-airflow-providers-postgres
meltano
```

**Arquivos afetados:** `transform/`, `logs/`, [`requirements.txt`](requirements.txt)

---

### 4.8 `volumes.yaml` desalinhado

| | |
|---|---|
| **Problema** | [`kubernetes/volumes.yaml`](kubernetes/volumes.yaml) aponta para `/home/artur-ubuntu/desafio-banvic` (path incorreto; projeto está em `prod-banvic`). Além disso, `dags.persistence.enabled: false` no values torna o volume não utilizado. |
| **Impacto** | Baixo — os dados já vêm via `COPY` no Dockerfile. Volume incorreto só causaria problema se persistência fosse habilitada. |
| **Prioridade** | Baixa |

**Como corrigir (escolher uma opção):**

- **Opção A (recomendada):** remover [`kubernetes/volumes.yaml`](kubernetes/volumes.yaml) e documentar no README que os dados são empacotados na imagem Docker.
- **Opção B:** corrigir o path para `/home/artur-ubuntu/desafio-banvic/prod-banvic`, habilitar `dags.persistence.enabled: true` e montar o PVC nos pods.

**Arquivos afetados:** [`kubernetes/volumes.yaml`](kubernetes/volumes.yaml), [`kubernetes/values.yaml`](kubernetes/values.yaml)

---

### 4.9 Monitoramento básico

| | |
|---|---|
| **Problema** | Apenas retries configurados; sem documentação de estratégia de monitoramento. |
| **Impacto** | Critério de avaliação pede "estratégias básicas de monitoramento". Retries atendem parcialmente. |
| **Prioridade** | Baixa |

**Como corrigir:**

1. Adicionar seção "Monitoramento" no [`README.md`](README.md) explicando:
   - Logs das tasks na UI do Airflow
   - Status dos pods via `kubectl get pods -n airflow`
   - Verificação de dados via query SQL no PostgreSQL

2. (Opcional) Habilitar alertas por e-mail nos `default_args` da DAG:

```python
default_args = {
    ...
    'email_on_failure': True,
    'email': ['seu-email@exemplo.com'],
}
```

**Arquivos afetados:** [`README.md`](README.md), opcionalmente [`dags/ingestao_banvic.py`](dags/ingestao_banvic.py)

---

### 4.10 `meltano install` a cada execução da DAG

| | |
|---|---|
| **Problema** | O BashOperator executa `meltano install && meltano run ...` a cada run da DAG. |
| **Impacto** | Desperdício de tempo e risco de falha desnecessária em rede. |
| **Prioridade** | Baixa |

**Como corrigir:**

1. Mover `meltano install` para o [`Dockerfile`](Dockerfile) (build time):

```dockerfile
WORKDIR /opt/airflow/prod-banvic
RUN /home/airflow/meltano_env/bin/meltano install
```

2. Simplificar o BashOperator na DAG:

```python
bash_command='cd /opt/airflow/prod-banvic && /home/airflow/meltano_env/bin/meltano run tap-csv target-postgres',
```

**Arquivos afetados:** [`Dockerfile`](Dockerfile), [`dags/ingestao_banvic.py`](dags/ingestao_banvic.py)

---

## 5. Checklist To-Do (priorizado)

### Crítico — bloqueia entrega

- [x] **4.1** Criar diagrama de arquitetura real e substituir placeholder no README
- [ ] **4.2** Gravar vídeo de 3–5 min e adicionar link no README *(pulado por enquanto)*

### Alta — funcionalidade e segurança

- [x] **4.3** Configurar conexão `postgres_default` no Helm (`AIRFLOW_CONN_POSTGRES_DEFAULT`)
- [x] **4.4** Externalizar credenciais do `meltano.yml` e `values.yaml`; criar `.env.example`
- [x] **4.5** Alinhar tag da imagem Docker entre README e `values.yaml`
- [ ] Testar DAG end-to-end após as correções acima

### Média — qualidade e aderência ao plano

- [x] **4.6** Migrar para schema `raw_banvic` e usar TRUNCATE/DROP explícito no schema
- [x] **4.7** Remover artefatos dbt (`transform/logs/`, `logs/dbt.log`) e limpar `requirements.txt`

### Baixa — melhorias

- [x] **4.8** Resolver ou remover `kubernetes/volumes.yaml`
- [x] **4.9** Documentar estratégia de monitoramento no README
- [x] **4.10** Mover `meltano install` para o Dockerfile

---

## 6. Roteiro de validação final

Execute na ordem após aplicar as correções:

```bash
# 1. Cluster Kind
kind create cluster --name banvic-cluster

# 2. Build e load da imagem (tag alinhada)
docker build -t banvic-airflow-custom:v1 .
kind load docker-image banvic-airflow-custom:v1 --name banvic-cluster

# 3. Deploy Helm
cd kubernetes
helm upgrade --install airflow apache-airflow/airflow -n airflow --create-namespace -f values.yaml

# 4. Aguardar pods
kubectl get pods -n airflow -w

# 5. Acessar Airflow UI
kubectl port-forward svc/airflow-webserver 8888:8080 -n airflow
# Abrir http://localhost:8888 (airflow/airflow)

# 6. Disparar DAG pipeline_ingestao_banvic e verificar tasks verdes

# 7. Verificar dados no PostgreSQL
kubectl port-forward svc/airflow-postgresql 5432:5432 -n airflow
```

Queries de validação:

```sql
-- Verificar schema e tabelas
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'raw_banvic'
ORDER BY table_name;

-- Contagem esperada (aproximada)
SELECT 'agencias' AS tabela, COUNT(*) FROM raw_banvic.agencias
UNION ALL SELECT 'clientes', COUNT(*) FROM raw_banvic.clientes
UNION ALL SELECT 'colaborador_agencia', COUNT(*) FROM raw_banvic.colaborador_agencia
UNION ALL SELECT 'colaboradores', COUNT(*) FROM raw_banvic.colaboradores
UNION ALL SELECT 'contas', COUNT(*) FROM raw_banvic.contas
UNION ALL SELECT 'propostas_credito', COUNT(*) FROM raw_banvic.propostas_credito
UNION ALL SELECT 'transacoes', COUNT(*) FROM raw_banvic.transacoes;
```

Contagens esperadas dos CSVs:

| Tabela | Linhas (sem header) |
|--------|---------------------|
| agencias | 10 |
| clientes | 998 |
| colaborador_agencia | 100 |
| colaboradores | 100 |
| contas | 999 |
| propostas_credito | 2.000 |
| transacoes | 71.999 |

---

## 7. Ordem sugerida de execução

```mermaid
flowchart TD
    start["Auditoria concluida"]
    critico["Critico: diagrama + video"]
    alta["Alta: conexao + secrets + tag"]
    media["Media: schema + limpeza"]
    baixa["Baixa: volumes + otimizacoes"]
    validacao["Validacao end-to-end"]
    entrega["Pronto para entrega"]

    start --> critico
    critico --> alta
    alta --> validacao
    validacao --> media
    media --> baixa
    baixa --> entrega
```

---

## 8. Referências

| Documento | Descrição |
|-----------|-----------|
| [`desafio.txt`](desafio.txt) | Requisitos oficiais da certificação |
| [`PROJECT_CONTEXT.md`](PROJECT_CONTEXT.md) | Contexto do BanVic e critérios de avaliação |
| [`PLANO_DE_ACAO.md`](PLANO_DE_ACAO.md) | Plano interno de engenharia (EL, sensores, idempotência) |
| [`README.md`](README.md) | Documentação principal do projeto |
