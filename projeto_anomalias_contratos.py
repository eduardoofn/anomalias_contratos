import streamlit as st
import pandas as pd
import psycopg2
import os
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

    # KPIs (Métricas Principais)
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Analisado", len(df))
    col2.metric("Alertas de Risco Alto", len(df[df['nivel_risco'] == 'ALTO']))
    col3.metric("Valor Total sob Risco", f"R$ {df['valor_global'].sum():,.2f}")

    # Gráfico de Distribuição
    st.subheader("Distribuição de Risco")
    st.bar_chart(df['nivel_risco'].value_counts())

    # Tabela de Dados (O item 2 da atividade: ISN_SIC aparece aqui!)
    st.subheader("Detalhes dos Contratos Suspeitos")
    st.dataframe(df[['isn_sic', 'numero_contrato', 'fornecedor_nome', 'valor_global', 'nivel_risco', 'score_anomalia']], use_container_width=True)

except Exception as e:
    st.error(f"Erro ao conectar ao banco de dados: {e}")
    st.info("Certifique-se que o seu banco de dados no Docker está rodando e as credenciais no .env estão corretas.")