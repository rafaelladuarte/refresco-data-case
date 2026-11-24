"""
Dashboard Executivo – Refresco Data Case
Consome as 4 views do schema gold no postgres-gold.
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="Refresco LTDA | Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        /* ── Importa fonte moderna ─────────────────────────────── */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

        html, body, [data-testid="stAppViewContainer"] {
            font-family: 'Inter', sans-serif;
            background-color: #FFFFFF;
            color: #1A1A2E;
        }

        /* ── Sidebar ───────────────────────────────────────────── */
        [data-testid="stSidebar"] {
            background-color: #FAF1F2;
            border-right: 1px solid #F0D9DC;
        }
        [data-testid="stSidebar"] .stMarkdown h2,
        [data-testid="stSidebar"] .stMarkdown h3 {
            color: #C0394B;
        }

        /* ── Cards de métrica ──────────────────────────────────── */
        [data-testid="stMetric"] {
            background-color: #FFF5F6;
            border: 1px solid #F5C6CB;
            border-radius: 14px;
            padding: 18px 22px;
            box-shadow: 0 2px 8px rgba(224, 92, 110, 0.08);
            transition: box-shadow 0.2s ease;
        }
        [data-testid="stMetric"]:hover {
            box-shadow: 0 4px 16px rgba(224, 92, 110, 0.16);
        }
        [data-testid="stMetricLabel"] {
            color: #7A7A8C;
            font-size: 13px;
            font-weight: 500;
        }
        [data-testid="stMetricValue"] {
            color: #E05C6E;
            font-weight: 700;
            font-size: 26px;
        }
        [data-testid="stMetricDelta"] { font-size: 13px; }

        /* ── Título de seção ───────────────────────────────────── */
        .section-title {
            font-size: 19px;
            font-weight: 700;
            color: #C0394B;
            border-left: 4px solid #E05C6E;
            padding-left: 12px;
            margin: 10px 0 4px 0;
            letter-spacing: -0.3px;
        }
        .section-desc {
            font-size: 13px;
            color: #6B6B7D;
            margin-bottom: 14px;
            padding-left: 4px;
        }

        /* ── Divisor ───────────────────────────────────────────── */
        hr { border-color: #F0D9DC; }

        /* ── Botões (bordas arredondadas) ──────────────────────── */
        .stButton > button {
            border-radius: 10px !important;
            border: 1.5px solid #E05C6E !important;
            color: #E05C6E !important;
            background-color: #FFFFFF !important;
            font-weight: 600;
            transition: all 0.2s ease;
        }
        .stButton > button:hover {
            background-color: #FFF0F2 !important;
            box-shadow: 0 2px 8px rgba(224, 92, 110, 0.2);
        }

        /* ── Inputs e selectbox ────────────────────────────────── */
        [data-testid="stSelectbox"] > div > div,
        [data-baseweb="select"] {
            border-radius: 10px !important;
            border-color: #F5C6CB !important;
        }

        /* ── Contêiner dos gráficos Plotly ─────────────────────── */
        [data-testid="stPlotlyChart"] > div {
            border-radius: 16px !important;
            overflow: hidden;
            border: 1px solid #F5E6E8;
            box-shadow: 0 2px 12px rgba(224, 92, 110, 0.07);
        }

        /* ── Tabela de dados ───────────────────────────────────── */
        [data-testid="stDataFrame"] {
            border-radius: 14px !important;
            overflow: hidden;
            border: 1px solid #F5E6E8;
            box-shadow: 0 2px 12px rgba(224, 92, 110, 0.07);
        }
        [data-testid="stDataFrame"] th {
            background-color: #FFF0F2 !important;
            color: #C0394B !important;
            font-weight: 600;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

@st.cache_resource(show_spinner="Conectando ao banco de dados...")
def get_engine():
    missing = [v for v in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB") if not os.getenv(v)]
    if missing:
        st.error(f"Variáveis de ambiente obrigatórias não definidas: {', '.join(missing)}")
        st.stop()
    user     = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host     = os.getenv("POSTGRES_HOST", "postgres-gold")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.environ["POSTGRES_DB"]
    url      = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


PALETTE    = ["#E05C6E", "#E8798A", "#F09AA8", "#F5BDC7", "#FAD9DF", "#FFF0F2"]
COLOR_MAIN = "#E05C6E"   # soft red — barras e linhas principais
COLOR_GOAL = "#C0394B"   # vermelho mais escuro — linha de meta
PLOT_BG    = "#FFFFFF"
PAPER_BG   = "#FFFFFF"
FONT_COLOR = "#1A1A2E"

PLOT_LAYOUT = dict(
    paper_bgcolor=PAPER_BG,
    plot_bgcolor=PLOT_BG,
    font=dict(color=FONT_COLOR, family="Inter, sans-serif", size=13),
    margin=dict(l=10, r=10, t=44, b=10),
    xaxis=dict(gridcolor="#F0D9DC", zerolinecolor="#F0D9DC", linecolor="#F0D9DC"),
    yaxis=dict(gridcolor="#F0D9DC", zerolinecolor="#F0D9DC", linecolor="#F0D9DC"),
)

try:
    df_q1 = run_query("SELECT * FROM gold.vw_q1_ranking_vendas ORDER BY regiao, ranking")
    df_q2 = run_query("SELECT * FROM gold.vw_q2_volume_vs_meta ORDER BY canal")
    df_q3 = run_query("SELECT * FROM gold.vw_q3_tendencia_volume ORDER BY nome_produto, data_completa")
    df_q4 = run_query("SELECT * FROM gold.vw_q4_margem_bruta ORDER BY margem_bruta_percentual DESC")
    db_ok = True
except Exception as exc:
    st.error(f" Falha ao conectar ao banco de dados: `{exc}`")
    st.info(
        "Verifique as variáveis de ambiente `POSTGRES_HOST`, `POSTGRES_USER`, "
        "`POSTGRES_PASSWORD`, `POSTGRES_PORT` e `POSTGRES_DB`."
    )
    db_ok = False
    st.stop()

with st.sidebar:
    st.markdown("## Refresco Analytics")
    st.markdown("---")
    st.markdown("### Filtros Globais")

    regioes = sorted(df_q1["regiao"].dropna().unique().tolist())
    regiao_sel = st.selectbox(
        "Região",
        options=["Todas"] + regioes,
        index=0,
        help="Filtra o ranking Q1 pela região selecionada.",
    )

    st.markdown("---")
    st.caption("Dados: schema `gold` · postgres-gold")
    st.caption("Cache: 5 minutos")

st.markdown("# Dashboard Executivo — Refresco LTDA")
st.markdown(
    "Visão consolidada de vendas, metas, tendências e rentabilidade "
    "com base na camada **Gold** do Data Warehouse."
)
st.markdown("---")

st.markdown('<p class="section-title">Q1 — Ranking de Vendas por Produto e Região</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="section-desc">Top 10 produtos por receita líquida de clientes ativos com pagamento confirmado, '
    'segmentado por região geográfica.</p>',
    unsafe_allow_html=True,
)

df_q1_fil = (
    df_q1[df_q1["regiao"] == regiao_sel].copy()
    if regiao_sel != "Todas"
    else df_q1.copy()
)

if df_q1_fil.empty:
    st.warning("Nenhum dado encontrado para a região selecionada.")
else:
    # Se "Todas", pega o top-10 geral por receita
    if regiao_sel == "Todas":
        df_q1_plot = (
            df_q1_fil.groupby("nome_produto", as_index=False)["total_receita_liquida"]
            .sum()
            .sort_values("total_receita_liquida", ascending=False)
            .head(10)
        )
        title_q1 = "Top 10 Produtos · Todas as Regiões"
    else:
        df_q1_plot = df_q1_fil.sort_values("total_receita_liquida", ascending=False).head(10)
        title_q1 = f"Top 10 Produtos · {regiao_sel}"

    # Ordena do maior para o menor (o líder ficará no topo do gráfico)
    df_q1_plot = df_q1_plot.sort_values("total_receita_liquida", ascending=True)

    # Cores: grafite para todas as barras; vermelho suave apenas no líder (último na lista ascendente)
    COLOR_BASE = "#495057"
    COLOR_LEADER = "#FF6B6B"
    bar_colors = [COLOR_BASE] * (len(df_q1_plot) - 1) + [COLOR_LEADER]

    fig_q1 = go.Figure(go.Bar(
        x=df_q1_plot["total_receita_liquida"],
        y=df_q1_plot["nome_produto"],
        orientation="h",
        marker_color=bar_colors,
        marker_line_width=0,
        text=df_q1_plot["total_receita_liquida"].apply(lambda v: f"R$ {v:,.0f}"),
        textposition="outside",
        textfont=dict(color=FONT_COLOR, size=12),
    ))
    fig_q1.update_layout(
        **PLOT_LAYOUT,
        title=title_q1,
        height=420,
    )
    fig_q1.update_xaxes(
        showgrid=False,
        showline=False,
        zeroline=False,
        showticklabels=False,
        title_text="",
    )
    fig_q1.update_yaxes(
        title_text="",
        showgrid=False,
        tickfont=dict(color="#1A1A2E", size=13),
    )
    st.plotly_chart(fig_q1, use_container_width=True)

st.markdown("---")

st.markdown('<p class="section-title">Q2 — Volume Realizado vs. Meta por Canal</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="section-desc">Comparativo entre o volume de caixas efetivamente vendido e a meta projetada '
    'por canal de distribuição.</p>',
    unsafe_allow_html=True,
)

# KPIs de atingimento
col_m1, col_m2, col_m3, col_m4 = st.columns(4)
ating_medio = df_q2["percentual_atingimento"].mean() if not df_q2.empty else 0

with col_m1:
    st.metric(
        label="Atingimento Médio",
        value=f"{ating_medio:.1f}%",
        delta=f"{ating_medio - 100:.1f}% vs meta",
        delta_color="normal",
    )
with col_m2:
    total_realizado = df_q2["volume_realizado_caixas"].sum()
    st.metric("Volume Total Realizado", f"{total_realizado:,.0f} cx")
with col_m3:
    total_meta = df_q2["meta_volume_caixas"].sum()
    st.metric("Meta Total (caixas)", f"{total_meta:,.0f} cx")
with col_m4:
    total_receita = df_q2["receita_realizada"].sum()
    st.metric("Receita Total Realizada", f"R$ {total_receita:,.0f}")

st.markdown("")

# Gráfico agrupado Realizado x Meta
fig_q2 = go.Figure()
fig_q2.add_trace(go.Bar(
    name="Volume Realizado",
    x=df_q2["canal"],
    y=df_q2["volume_realizado_caixas"],
    marker_color="#FF6B6B",
    text=df_q2["volume_realizado_caixas"].apply(lambda v: f"{v:,.0f}"),
    textposition="outside",
))
fig_q2.add_trace(go.Bar(
    name="Meta",
    x=df_q2["canal"],
    y=df_q2["meta_volume_caixas"],
    marker_color="#ADB5BD",
    text=df_q2["meta_volume_caixas"].apply(lambda v: f"{v:,.0f}"),
    textposition="outside",
))
fig_q2.update_layout(
    **PLOT_LAYOUT,
    barmode="group",
    title="Volume Realizado vs. Meta por Canal (Simulado)",
    xaxis_title="",
    yaxis_title="",
    legend=dict(
        orientation="h", 
        yanchor="bottom", 
        y=1.02, 
        xanchor="right", 
        x=1,
        title_text=""
    ),
    height=380,
    barcornerradius=6, # Arredondamento do topo das barras (suportado no Plotly 5.22+)
)
fig_q2.update_traces(marker_line_width=0)
fig_q2.update_yaxes(
    showgrid=False,
    showticklabels=False, # Eixo removido pois já temos data labels
)
fig_q2.update_xaxes(
    showgrid=False,
)
st.plotly_chart(fig_q2, use_container_width=True)

st.markdown("---")

st.markdown('<p class="section-title">Q3 — Tendência de Volume por Produto</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="section-desc">Evolução temporal do volume diário e da média móvel de 7 dias por produto. '
    'Utilize o seletor para explorar cada SKU individualmente.</p>',
    unsafe_allow_html=True,
)

produtos_q3 = sorted(df_q3["nome_produto"].dropna().unique().tolist())
col_sel, _ = st.columns([2, 3])
with col_sel:
    produto_sel = st.selectbox("Selecione o Produto", options=produtos_q3)

df_q3_fil = df_q3[df_q3["nome_produto"] == produto_sel].copy()
df_q3_fil["data_completa"] = pd.to_datetime(df_q3_fil["data_completa"])

fig_q3 = go.Figure()
fig_q3.add_trace(go.Scatter(
    x=df_q3_fil["data_completa"],
    y=df_q3_fil["volume_diario_caixas"],
    mode="lines",
    name="Volume Diário",
    line=dict(color="#8b949e", width=1, dash="dot"),
    opacity=0.6,
))
fig_q3.add_trace(go.Scatter(
    x=df_q3_fil["data_completa"],
    y=df_q3_fil["media_movel_7dias"],
    mode="lines",
    name="Média Móvel 7 dias",
    line=dict(color=COLOR_MAIN, width=2.5),
    fill="tozeroy",
    fillcolor="rgba(88,166,255,0.08)",
))
fig_q3.update_layout(
    **PLOT_LAYOUT,
    title=f"Tendência de Volume — {produto_sel}",
    xaxis_title="Data",
    yaxis_title="Caixas",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    height=400,
)
st.plotly_chart(fig_q3, use_container_width=True)

st.markdown("---")


st.markdown('<p class="section-title">Q4 — Rentabilidade por SKU</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="section-desc">Margem bruta percentual de cada produto, ordenada da maior para a menor. '
    'Identifique os SKUs mais e menos rentáveis do portfólio.</p>',
    unsafe_allow_html=True,
)

col_t, col_chart = st.columns([1, 1])

with col_t:
    # Performance: limita a 20 SKUs mais relevantes antes de renderizar
    df_q4_display = (
        df_q4
        .sort_values("margem_bruta_percentual", ascending=False)
        .head(20)[["nome_produto", "receita_liquida_total", "custo_total", "margem_bruta", "margem_bruta_percentual"]]
        .reset_index(drop=True)
    )

    # Valor máximo para normalizar a barra de progresso
    margem_max = float(df_q4_display["margem_bruta_percentual"].max())

    st.dataframe(
        df_q4_display,
        use_container_width=True,
        height=380,
        column_config={
            "nome_produto": st.column_config.TextColumn(
                "Produto",
                help="Nome do SKU",
            ),
            "receita_liquida_total": st.column_config.NumberColumn(
                "Receita Líq.",
                format="R$ %.2f",
                help="Receita líquida total (R$)",
            ),
            "custo_total": st.column_config.NumberColumn(
                "Custo Total",
                format="R$ %.2f",
                help="Custo total apurado (R$)",
            ),
            "margem_bruta": st.column_config.NumberColumn(
                "Margem Bruta",
                format="R$ %.2f",
                help="Receita líquida − Custo total",
            ),
            "margem_bruta_percentual": st.column_config.ProgressColumn(
                "Margem %",
                help="Margem bruta como % da receita líquida",
                format="%.2f%%",
                min_value=0,
                max_value=margem_max,
            ),
        },
        hide_index=True,
    )

with col_chart:
    # Ordena ascendente para o Plotly renderizar o mais rentável no topo
    df_q4_sorted = (
        df_q4
        .sort_values("margem_bruta_percentual", ascending=True)
        .tail(20)
        .reset_index(drop=True)
    )

    # Cores: top 3 em vermelho suave, restante em grafite
    n = len(df_q4_sorted)
    bar_colors_q4 = [
        "#FF6B6B" if i >= n - 3 else "#495057"
        for i in range(n)
    ]

    fig_q4 = go.Figure(go.Bar(
        x=df_q4_sorted["margem_bruta_percentual"],
        y=df_q4_sorted["nome_produto"],
        orientation="h",
        marker_color=bar_colors_q4,
        marker_line_width=0,
        text=df_q4_sorted["margem_bruta_percentual"].apply(lambda v: f"{v:.1f}%"),
        textposition="outside",
        textfont=dict(color=FONT_COLOR, size=12),
    ))
    fig_q4.update_layout(
        **PLOT_LAYOUT,
        title="Margem Bruta % por SKU",
        height=380,
    )
    fig_q4.update_xaxes(
        showgrid=False,
        showline=False,
        zeroline=False,
        showticklabels=False,
        title_text="",
    )
    fig_q4.update_yaxes(
        title_text="",
        showgrid=False,
        tickfont=dict(color="#1A1A2E", size=12),
    )
    st.plotly_chart(fig_q4, use_container_width=True)


st.markdown("---")
st.caption("Refresco Data Case · Pipeline Medallion (Bronze → Silver → Gold) · Powered by Streamlit + Plotly")
