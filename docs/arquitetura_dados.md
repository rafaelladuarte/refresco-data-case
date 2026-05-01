# Documentação da Arquitetura e Pipeline de Dados

## 1. Diagrama Entidade-Relacionamento (Star Schema)

A modelagem dimensional segue o padrão Estrela (Star Schema), construída no schema `gold` do PostgreSQL. Ela centraliza as métricas de negócio na tabela de fatos `fato_vendas`, que se relaciona com as dimensões de contexto.

```mermaid
erDiagram
    fato_vendas {
        int sk_venda PK
        varchar id_venda
        int sk_cliente FK
        int sk_produto FK
        int sk_representante FK
        int sk_tempo FK
        numeric qtd_caixas
        numeric preco_unitario
        numeric desconto_pct
        numeric receita_bruta
        numeric receita_liquida
        numeric custo_total
        numeric margem_bruta
        varchar tipo_lancamento
        varchar status_financeiro
    }

    dim_cliente {
        int sk_cliente PK
        varchar id_cliente
        varchar nome_cliente
        varchar canal
        varchar regiao
        varchar cidade
        varchar uf
        boolean ativo
    }

    dim_produto {
        int sk_produto PK
        varchar id_produto
        varchar nome_produto
        varchar familia
        varchar marca
        varchar embalagem
        numeric volume_ml
        numeric preco_tabela
        numeric custo_unitario
    }

    dim_tempo {
        int sk_tempo PK
        date data_referencia
        int ano
        int mes
        int dia
        varchar dia_semana
        numeric clima_temp_max
        numeric clima_precipitacao
    }

    dim_representante {
        int sk_representante PK
        int sk_representante_id
        varchar nome_representante
        varchar canal_venda
    }

    fato_vendas }|--|| dim_cliente : "possui"
    fato_vendas }|--|| dim_produto : "contém"
    fato_vendas }|--|| dim_tempo : "ocorre em"
    fato_vendas }|--|| dim_representante : "realizada por"
```

### Descrição dos Componentes (Modelo de Dados)
- **fato_vendas**: Tabela central que consolida as métricas transacionais de vendas (quantidades, receitas, custos) enriquecida com dados do sistema financeiro (status do lançamento).
- **dim_cliente**: Armazena o cadastro de clientes, dados demográficos e de localização.
- **dim_produto**: Catálogo de produtos (Refreshments), categorizando itens por família, marca e volume.
- **dim_tempo**: Dimensão calendário que inclui métricas de clima agregadas (temperatura e precipitação) para análise de correlação com vendas.
- **dim_representante**: Dimensão para análise de performance da força de vendas baseada no nome e canal de atuação.

---

## 2. Diagrama de Arquitetura e Infraestrutura

A infraestrutura é inteiramente baseada em containers Docker, orquestrada pelo Airflow e utilizando o dbt para as transformações dentro do Data Warehouse.

```mermaid
graph TD
    %% Estilos - Tema Profissional
    classDef default fill:#f8f9fa,stroke:#343a40,stroke-width:1px
    classDef source fill:#d1ecf1,stroke:#0c5460,stroke-width:2px,color:#0c5460
    classDef orchestrator fill:#fff3cd,stroke:#856404,stroke-width:2px,color:#856404
    classDef storage fill:#d4edda,stroke:#155724,stroke-width:2px,color:#155724
    classDef warehouse fill:#e2d5f1,stroke:#4a235a,stroke-width:2px,color:#4a235a
    classDef compute fill:#f8d7da,stroke:#721c24,stroke-width:2px,color:#721c24
    classDef socket fill:#e2e3e5,stroke:#383d41,stroke-width:1.5px,stroke-dasharray:4 2
    classDef analytics fill:#ffe6f0,stroke:#d63384,stroke-width:2px,color:#d63384
    classDef visualization fill:#d5f4e6,stroke:#198754,stroke-width:2px,color:#198754

    %% Nós
    CSV["Arquivos CSV Brutos"] -->|Ingestão| Airflow["Airflow Orchestrator"]
    class CSV source
    class Airflow orchestrator

    subgraph Docker["Ambiente Docker"]
        direction TB
        
        Airflow
        DockerSock["/var/run/docker.sock"]
        DBT["dbt-core Container (Efêmero)"]
        
        subgraph MinIO["Object Storage (MinIO)"]
            Bronze["Camada Bronze (Raw)"]
            Silver["Camada Silver (Parquet)"]
        end
        
        subgraph PostgreSQL["PostgreSQL DWH"]
            LoadStage["Schema: load_stage"]
            Gold["Schema: gold"]
        end
        
        subgraph Streamlit["Streamlit Analytics"]
            Dashboard["Dashboard Interativo<br/>Gráficos e Métricas"]
            APILayer["API Layer<br/>(Altair/Plotly)"]
        end
    end

    %% Fluxo da Pipeline
    Airflow -->|Pandas/S3FS| Bronze
    Bronze -->|Validação| Silver
    Silver -->|Carga Inicial| LoadStage
    Airflow -->|DockerOperator| DockerSock
    DockerSock -->|Executa| DBT
    DBT -->|Lê dados| LoadStage
    DBT -->|Transforma| Gold
    
    %% Fluxo de Disponibilização
    Gold -->|Consulta SQL| APILayer
    APILayer -->|Renderiza| Dashboard
    Dashboard ==>|Visualização| Usuario["👤 Usuário Final"]

    %% Aplicando estilos dos subgráficos
    style Docker fill:#e9ecef,stroke:#495057,stroke-width:2px
    style MinIO fill:#d4edda,stroke:#28a745,stroke-width:2px
    style PostgreSQL fill:#cfe2ff,stroke:#0d6efd,stroke-width:2px
    style Streamlit fill:#ffe6f0,stroke:#d63384,stroke-width:2px
    
    class DockerSock socket
    class DBT compute
    class Bronze,Silver storage
    class LoadStage,Gold warehouse
    class APILayer analytics
    class Dashboard visualization
```

### Descrição dos Componentes (Infraestrutura)
- **Origem (CSVs)**: Dados transacionais gerados pelos sistemas fonte.
- **Container Airflow (Orquestrador)**: O coração da operação. Executa scripts Python para ingestão (Bronze) e tratamento (Silver). O Airflow possui acesso ao grupo Docker (gid suplementar) para interagir com o daemon host.
- **MinIO**: Armazenamento de objetos simulando S3. Guarda os arquivos brutos particionados (Bronze) e os dados limpos em formato colunar Parquet (Silver).
- **Container Postgres (Data Warehouse)**: Banco de dados relacional hospedando a área de staging (`load_stage`) — alimentada pelo Airflow — e a camada de negócios dimensional (`gold`).
- **Comunicação docker.sock**: Interface crucial montada no container do Airflow. Permite que as tasks `DockerOperator` subam novos containers efêmeros do dbt-core na mesma rede do DWH de forma isolada e segura.
- **Container dbt-core (Transformador)**: Container gerado dinamicamente pelo Airflow apenas durante a execução. Ele lê o código no diretório do projeto, se conecta ao Postgres e materializa as regras de negócio de `load_stage` para `gold`.
