import streamlit as st
import pandas as pd
import psycopg2
import os
import altair as alt
from dotenv import load_dotenv

# 1. Configurações da Página
st.set_page_config(page_title="Auditoria de Contratos - CE", layout="wide")
load_dotenv()

# 2. Função para conectar ao banco (Postgres do Docker)
def get_data():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'), # Se rodar fora do docker, use localhost
        database=os.getenv('DB_NAME', 'aula'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASS', '1234'),
        port=os.getenv('DB_PORT', '5432')
    )
    query = "SELECT * FROM anomalias_contratos ORDER BY score_anomalia ASC"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# 3. Interface do Streamlit
st.title("🔍 Painel de Detecção de Anomalias")
st.markdown("Monitoramento diário de contratos baseados em **Isolation Forest**.")

try:
    df = get_data()

    # Filtros na Barra Lateral
    st.sidebar.header("Filtros")
    orgao = st.sidebar.multiselect("Selecione o Órgão", options=df['orgao_nome'].unique())
    risco = st.sidebar.multiselect("Nível de Risco", options=['ALTO', 'MÉDIO', 'BAIXO'], default=['ALTO'])

    if orgao:
        df = df[df['orgao_nome'].isin(orgao)]
    if risco:
        df = df[df['nivel_risco'].isin(risco)]

    if df.empty:
        st.warning("Nenhuma anomalia encontrada no momento.")
    else:
        # KPIs (Métricas Principais)
        total_risco_alto = len(df[df['nivel_risco'] == 'ALTO'])
        total_valor = df['valor_global'].sum()
        media_score = df['score_anomalia'].mean()

        col1, col2, col3, col4 = st.columns([1, 1, 1.8, 1])
        col1.metric("Total de Anomalias", len(df))
        col2.metric("Risco ALTO", total_risco_alto)
        col3.metric("Valor Total Anômalo", f"R$ {total_valor:,.2f}")
        col4.metric("Score Médio", f"{media_score:.4f}")

        # Gráfico de Distribuição de Risco
        st.subheader("Distribuição de Anomalias por Nível de Risco")
        risk_counts = df['nivel_risco'].value_counts().rename_axis('nivel_risco').reset_index(name='count')
        risk_chart = alt.Chart(risk_counts).mark_bar().encode(
            x=alt.X('nivel_risco:N', sort=['ALTO', 'MÉDIO', 'BAIXO'], title='Nível de Risco'),
            y=alt.Y('count:Q', title='Quantidade'),
            color=alt.Color('nivel_risco:N', scale=alt.Scale(domain=['ALTO','MÉDIO','BAIXO'], range=['#d62728','#ff7f0e','#2ca02c']), legend=alt.Legend(title='Nível de Risco')),
            tooltip=['nivel_risco', 'count']
        ).properties(height=350)
        risk_text = alt.Chart(risk_counts).mark_text(dx=0, dy=-10, color='black').encode(
            x=alt.X('nivel_risco:N', sort=['ALTO', 'MÉDIO', 'BAIXO']),
            y=alt.Y('count:Q'),
            text=alt.Text('count:Q')
        )
        st.altair_chart((risk_chart + risk_text).configure_view(strokeWidth=0), use_container_width=True)

        # Gráfico de Score de Anomalia
        st.subheader("Score de Anomalia por Contrato")
        score_chart = alt.Chart(df).mark_circle(size=60).encode(
            x=alt.X('score_anomalia:Q', title='Score de Anomalia'),
            y=alt.Y('valor_global:Q', title='Valor Global (R$)', scale=alt.Scale(type='log')),
            color=alt.Color('nivel_risco:N', scale=alt.Scale(domain=['ALTO','MÉDIO','BAIXO'], range=['#d62728','#ff7f0e','#2ca02c']), legend=alt.Legend(title='Nível de Risco')),
            tooltip=['isn_sic', 'fornecedor_nome', 'valor_global', 'percentil_risco', 'nivel_risco']
        ).properties(height=400)
        st.altair_chart(score_chart, use_container_width=True)

        # Top 10 Órgãos com mais Anomalias
        st.subheader("Top 10 Órgãos com Mais Anomalias")
        top_orgao = df['orgao_nome'].value_counts().reset_index()
        top_orgao.columns = ['orgao_nome', 'count']
        top_orgao = top_orgao.head(10)
        orgao_chart = alt.Chart(top_orgao).mark_bar(color='#1f77b4').encode(
            x=alt.X('count:Q', title='Quantidade'),
            y=alt.Y('orgao_nome:N', sort='-x', title='Órgão'),
            tooltip=['orgao_nome', 'count']
        ).properties(height=400)
        orgao_text = alt.Chart(top_orgao).mark_text(dx=5, dy=0, color='black').encode(
            x=alt.X('count:Q'),
            y=alt.Y('orgao_nome:N', sort='-x'),
            text=alt.Text('count:Q')
        )
        st.altair_chart((orgao_chart + orgao_text).configure_view(strokeWidth=0), use_container_width=True)

        # Tabela de Dados (O item 2 da atividade: ISN_SIC aparece aqui!)
        st.subheader("Detalhes dos Contratos Suspeitos")
        st.dataframe(
            df[['isn_sic', 'numero_contrato', 'fornecedor_nome', 'orgao_nome', 'valor_global', 'nivel_risco', 'percentil_risco', 'score_anomalia']],
            use_container_width=True
        )

except Exception as e:
    st.error(f"Erro ao conectar ao banco de dados: {e}")
    st.info("Certifique-se que o seu banco de dados no Docker está rodando e as credenciais no .env estão corretas.")