# Decisões de tratamento de dados

Registro das decisões técnicas e de negócio do pipeline, no formato ADR (*Architecture Decision Record*): contexto → decisão → consequências.
As decisões D1, D2 e D3 foram aprovadas no [plano da Sprint 1](backlog/SPRINT-01.md).

| ADR | Tema | Camada | Status |
|---|---|---|---|
| [001](#adr-001--encoding-definido-por-fonte) | Encoding definido por fonte | Bronze | Aceita |
| [002](#adr-002--nulos-continuam-nulos-e-membro-desconhecido) | Nulos continuam nulos e membro desconhecido | Silver / Gold | Aceita |
| [003](#adr-003--quarentena-em-vez-de-descarte-silencioso) | Quarentena em vez de descarte silencioso | Silver | Aceita |
| [004](#adr-004--clientes-com-cnpj-duplicado-d1) | Clientes com CNPJ duplicado (D1) | Silver | Aceita |
| [005](#adr-005--regra-de-negócio-por-tipo_lancamento-d2) | Regra de negócio por `tipo_lancamento` (D2) | Gold | Aceita |
| [006](#adr-006--metas-fictícias-via-seed-d3) | Metas fictícias via seed (D3) | Gold | Aceita |
| [007](#adr-007--clima-diário-de-uberlândia) | Clima diário de Uberlândia | Bronze / Gold | Aceita |
| [008](#adr-008--configuração-e-credenciais-via-ambiente) | Configuração e credenciais via ambiente | Infra | Aceita |

---

## ADR-001 · Encoding definido por fonte

**Contexto.** Todos os CSVs eram lidos como `latin1`, mas só `clientes.csv` é ISO-8859-1; `vendas`, `produtos` e `financeiro` são UTF-8. Com isso, `Conveniência` virava `ConveniÃªncia`. Funções `fix_encoding` na Silver corrigiam só algumas colunas. Além disso, `clientes.csv` tem 4 nomes com dupla codificação **na própria origem** (ex.: `Mercado do ZÃ©`).

**Decisão.**
- Mapa explícito `SOURCE_ENCODINGS` em `pipeline/bronze/ingest_csv.py`, que funciona como contrato da fonte. Tabela sem encoding definido gera erro. A Bronze é sempre gravada em UTF-8.
- Mapa explícito, e não detecção automática, porque detecção é heurística e pode mudar o resultado sem aviso.
- Na Silver de clientes, `repair_double_encoding` corrige apenas textos com marcadores de mojibake (`Ã`, `Â`) e mantém o original se a conversão falhar. A Bronze continua fiel à origem.

**Consequências.** Nenhum caractere corrompido na Silver ou na Gold. Uma fonte nova exige declarar o encoding, o que é intencional.

## ADR-002 · Nulos continuam nulos e membro desconhecido

**Contexto.** `astype(str)` transformava `NaN` na string `"nan"`. Isso anulava filtros `notna()` e fazia `nan` chegar ao warehouse como valor de negócio (a view Q2 tinha `WHEN canal = 'nan' THEN 'Outros'`).

**Decisão.**
- Helper `clean_text` (`pipeline/silver/text.py`): dtype `string` do pandas (nullable), `strip`, e vazio tratado como nulo.
- IDs continuam como string vazia (`""`), porque as regras de validação comparam com vazio. Também são padronizados em maiúsculas.
- Na dimensão, atributo ausente vira membro desconhecido: `coalesce(canal, 'Não informado')` em `dim_cliente`, protegido por teste `not_null`.

**Consequências.** 3 clientes aparecem como `Não informado`, em vez de NULL ou `nan`. Sem remendos nas views.

## ADR-003 · Quarentena em vez de descarte silencioso

**Contexto.** Vendas com data em formato não reconhecido (`DD/MM/AA`, `DD-MM-AAAA`) viravam NaT e eram removidas por `dropna`, sem log nem quarentena: **125 vendas (3,6%)**. Medidas inválidas (`desconto_pct` = 20,73; `qtd_caixas` negativa) passavam e geravam receita negativa, escondida por um filtro na view Q4.

**Decisão.**
- `parse_mixed_dates` aceita 4 formatos mutuamente exclusivos (verificado: cada string casa com no máximo um).
- `validate_vendas` (`pipeline/silver/clean_vendas.py`) aplica uma lista única de regras: obrigatórios, data, `qtd_caixas > 0`, `preco_unitario > 0`, `0 ≤ desconto_pct < 1` e integridade referencial. Toda rejeição vai para `silver/_quarentena/vendas/`, com `motivo_rejeicao` (todas as regras violadas) e o texto original da data.
- O dbt repete as regras de medida em `stg_vendas` (`accepted_range`, `expression_is_true`) como defesa em profundidade.

**Consequências.** A fato passou de 3.366 para 3.482 vendas (+125 datas, −9 medidas inválidas). A quarentena tem 18 registros, todos explicados. O filtro `receita_liquida > 0` da Q4 foi removido.

## ADR-004 · Clientes com CNPJ duplicado (D1)

**Contexto.** 5 empresas estão cadastradas duas vezes (C0001–C0005 e C0200–C0204, com o mesmo CNPJ). O dedup por `cnpj` com `keep="last"` mantinha os **recadastros sem vendas** e descartava os originais, mandando as vendas deles para a quarentena como "sem cadastro". Como `_ingested_at` empatava, o cadastro que sobrevivia variava entre execuções.

**Decisão (opção A da D1).**
- Chave natural = `id_cliente`, com ordenação estável (resultado determinístico).
- Para CNPJ duplicado, o **menor `id_cliente`** (cadastro mais antigo) é o canônico. Os demais vão para `silver/_quarentena/clientes/` com motivo `"CNPJ duplicado de C000x"`.
- Regras extraídas para a função pura `deduplicate_clientes`.

**Alternativa descartada.** Manter todos os cadastros com a flag `cnpj_duplicado`. Seria mais simples, mas deixaria a mesma empresa em duas linhas da dimensão.

**Consequências.** 141 vendas recuperadas. `dim_cliente` com 115 clientes e 115 CNPJs distintos. Os "órfãos C0004/C0005" do diagnóstico antigo eram gerados pelo pipeline, não pela origem.

## ADR-005 · Regra de negócio por `tipo_lancamento` (D2)

**Contexto.** Cada venda tem no máximo um lançamento financeiro, então o tipo **classifica a própria venda**: uma venda com lançamento `DEVOLUCAO` foi devolvida. As views só filtravam `status = 'PAGO'` e contavam devoluções e bonificações como receita.

**Decisão.** Seed `dbt/seeds/regra_tipo_lancamento.csv` como fonte única da regra, exposta na `fato_vendas` como `entra_receita` / `entra_volume`:

| tipo_lancamento | entra_receita | entra_volume | Justificativa |
|---|---|---|---|
| VENDA | ✅ | ✅ | Venda faturada |
| BONIFICACAO | ❌ | ✅ | Entregue sem cobrança: conta em volume e custo, não em receita |
| DEVOLUCAO | ❌ | ❌ | Venda desfeita |
| AJUSTE | ❌ | ❌ | Lançamento contábil, não operação comercial |
| OUTRO | ❌ | ❌ | Sem semântica definida |

- Receita (Q1, Q2, Q4): `entra_receita`. Volume (Q2, Q3): `entra_volume`.
- Margem (Q4): receita de VENDA menos custo de VENDA + BONIFICACAO.
- O teste de `tipo_lancamento` na fato é um `relationships` com o seed. Um tipo novo sem regra quebra o build.

**Consequências.** Vendas PAGO consideradas: de 849 para 223 em receita e 436 em volume. A queda é esperada: os dados sintéticos distribuem os tipos de forma uniforme. A regra muda editando um CSV versionado, sem tocar em SQL.

## ADR-006 · Metas fictícias via seed (D3)

**Contexto.** A meta da Q2 era `realizado × fator`, então o atingimento era sempre `1/fator`. Qualquer meta derivada do próprio realizado continua circular (no agregado, `Σ realizado / Σ (realizado × fator) = 1/fator`). A base não tem metas.

**Decisão (opção A da D3).**
- Seed `dbt/seeds/metas_canal_mes.csv` com valores **fictícios**, fixos e redondos por canal e mês. Eles foram escolhidos uma única vez e não são recalculados a partir dos dados.
- A ficção é declarada no YAML do seed, na view e no dashboard.
- Testes: combinação única (canal, ano, mês) e `relationships` de `canal` com `dim_cliente.canal`.
- A `vw_q2` cruza meta e realizado no grão canal × ano × mês (`FULL OUTER JOIN`): mês sem venda conta como meta não atingida.

**Consequências.** O atingimento varia de verdade entre os canais (92,97% a 104,22%). Para usar metas reais, basta substituir o CSV.

## ADR-007 · Clima diário de Uberlândia

**Contexto.** A ingestão usava `-21.78, -43.35` (Juiz de Fora). A `dim_tempo` ligava o clima por (ano, mês), então cada dia carregava a precipitação acumulada do mês inteiro.

**Decisão.**
- Coordenadas de Uberlândia (`-18.9186, -48.2772`) e período configuráveis via `CLIMA_*` no `.env`.
- Timeout e `raise_for_status` nas chamadas. Falha parcial vira warning; falha total interrompe a ingestão.
- `dim_tempo` com clima no grão diário, mais colunas `*_mes` com o agregado mensal.
- Teste singular `assert_clima_cobre_dias_com_venda`: todo dia com venda precisa ter clima.

**Limitação aceita.** Uma única coordenada para clientes espalhados em 4 mesorregiões de MG. Clima por região fica no backlog.

## ADR-008 · Configuração e credenciais via ambiente

**Contexto.** Fernet key, senhas do Airflow, pgAdmin e MinIO estavam fixas no `docker-compose.yml` e como padrão nos scripts. As versões não estavam fixadas, e o `requirements.txt` incluía `apache-airflow`, que o pip podia atualizar no build.

**Decisão.**
- Credenciais só via `.env`. O compose usa `${VAR:?}` e falha com mensagem clara. Os scripts usam `require_env` (`pipeline/settings.py`), sem senha padrão.
- Imagem `apache/airflow:2.9.0-python3.12` com o constraints oficial. Dependências e imagens com tags fixas.
- A imagem `minio/minio` deixou de ser distribuída publicamente, então foi adotada a `bitnamilegacy/minio:2025.7.23-debian-12-r5`.
- `docker.sock` montado com leitura e escrita: `:ro` não restringe a API do Docker e daria falsa sensação de segurança.

**Consequências.** O ambiente é reproduzível em um clone novo. Os riscos aceitos para uso local (`docker.sock` equivale a root no host; imagem do MinIO congelada) estão documentados no README.
