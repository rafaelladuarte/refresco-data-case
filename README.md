# Project: Refresco Data Case - Uberlandia

## Estrutura do Projeto

O projeto segue uma arquitetura de medalhão (Bronze, Silver, Gold) para o processamento de dados.

### Componentes Principais

- **Airflow DAGs**: Localizadas em `dags/`
- **Pipeline de Dados**: Implementação modular em `pipeline/`
- **Dados**: Armazenados em `data/` (Raw, Bronze, Silver, Gold)
- **Infraestrutura**: `docker-compose.yml` e `init/`

## Como executar

1. Configure o arquivo `.env` baseado no `.env.example`.
2. Execute `docker-compose up -d`.
3. Acesse o Airflow e ative a DAG `pipeline_uberlandia`.
