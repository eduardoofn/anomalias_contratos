# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS PADRÃO
# ─────────────────────────────────────────────────────────────────────────────
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import requests
from psycopg2.extras import execute_values
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from airflow import DAG
from airflow.operators.python import PythonOperator

# Logger padrão do Airflow — os logs aparecem na interface web
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES GLOBAIS
# ─────────────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": HOST,  # ou "localhost" fora do Docker
    "port": PORT,
    "database": BASE,
    "user": USER,
    "password": PSSWD,
}

# URL base da API do Ceará Transparente
API_BASE_URL = (
    "https://api-dados-abertos.cearatransparente.ce.gov.br"
    "/transparencia/contratos/contratos"
)

# Janela de datas: busca contratos assinados nos últimos 365 dias
# Em produção, você pode parametrizar isso via Airflow Variables
DIAS_RETROATIVOS = 365

# Janela de análise do modelo de anomalias
PERIODO_ANALISE_DIAS = 365

# Parâmetro de contaminação do Isolation Forest
# 0.05 = esperamos que ~5% dos contratos sejam anômalos
# Ajuste conforme o contexto do negócio (entre 0.01 e 0.5)
CONTAMINACAO = 0.05

# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÕES AUXILIARES
# ─────────────────────────────────────────────────────────────────────────────
def get_db_connection():
    """
    Retorna uma conexão psycopg2 com o PostgreSQL.
    Centralizar aqui facilita trocar a configuração em um só lugar.
    """
    return psycopg2.connect(**DB_CONFIG)

def get_temp_contracts_file_path():
    """
    Gera um caminho único para o arquivo temporário de contratos
    no mesmo diretório desta DAG.
    """
    dag_dir = Path(__file__).resolve().parent
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return dag_dir / f"contratos_tmp_{timestamp}.jsonl"

def criar_tabelas_se_nao_existirem():
    """
    Cria as tabelas necessárias caso não existam.
    Usar IF NOT EXISTS torna a operação idempotente (pode rodar N vezes sem erro).
    """
    ddl_contratos = """
        CREATE TABLE IF NOT EXISTS contratos (
            id                  BIGSERIAL PRIMARY KEY,
            isn_sic             TEXT,    -- identificador único da API (usado para idempotência)
            numero_contrato     TEXT,
            objeto              TEXT,
            fornecedor_nome     TEXT,
            fornecedor_cnpj     TEXT,
            orgao_nome          TEXT,
            modalidade          TEXT,
            valor_inicial       NUMERIC(18, 2),
            valor_global        NUMERIC(18, 2),
            data_assinatura     DATE,
            data_inicio_vigencia DATE,
            data_fim_vigencia    DATE,
            prazo_vigencia_dias  INTEGER,       -- feature derivada: duração em dias
            json_original        JSONB,          -- dado bruto completo para rastreabilidade
            inserido_em          TIMESTAMP DEFAULT NOW(),
            UNIQUE(isn_sic)  -- evita duplicatas em re-execuções
        );
    """

    ddl_anomalias = """
        CREATE TABLE IF NOT EXISTS anomalias_contratos (
            id                  BIGSERIAL PRIMARY KEY,
            isn_sic             TEXT,
            numero_contrato     TEXT,
            objeto              TEXT,
            fornecedor_nome     TEXT,
            orgao_nome          TEXT,
            valor_global        NUMERIC(18, 2),
            prazo_vigencia_dias INTEGER,
            score_anomalia      NUMERIC(10, 6),  -- quanto mais negativo, mais anômalo
            percentil_risco     INTEGER,          -- 0-100; 100 = mais anômalo
            nivel_risco         TEXT,             -- ALTO / MÉDIO / BAIXO
            data_assinatura     DATE,
            detectado_em        TIMESTAMP DEFAULT NOW()
        );
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_contratos)
            cur.execute(ddl_anomalias)
        conn.commit()
    logger.info("Tabelas verificadas/criadas com sucesso.")

# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — EXTRAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

def extrair_contratos(**context):
    # Calcula o intervalo de datas dinamicamente
    hoje = datetime.now()
    data_fim = hoje.strftime("%d/%m/%Y")
    data_inicio = (hoje - timedelta(days=DIAS_RETROATIVOS)).strftime("%d/%m/%Y")

    logger.info(f"Buscando contratos de {data_inicio} até {data_fim}")

    arquivo_temporario = get_temp_contracts_file_path()
    total_contratos = 0
    pagina_atual = 1
    total_paginas = None  # Será descoberto na primeira requisição

    with arquivo_temporario.open("w", encoding="utf-8") as arquivo_saida:
        while True:
            params = {
                "page": pagina_atual,
                "data_assinatura_inicio": data_inicio,
                "data_assinatura_fim": data_fim,
            }

            try:
                response = requests.get(
                    API_BASE_URL,
                    params=params,
                    timeout=30,  # segundos — importante para não travar o worker
                )
                response.raise_for_status()  # levanta exceção para status 4xx/5xx

            except requests.exceptions.Timeout:
                logger.error(f"Timeout na página {pagina_atual}. Encerrando extração.")
                break
            except requests.exceptions.HTTPError as e:
                logger.error(f"Erro HTTP {e.response.status_code} na página {pagina_atual}: {e}")
                break

            dados = response.json()

            registros = dados.get("data", [])
            meta = dados.get("sumary", {})  # API retorna "sumary" (sem 'm' duplo)

            if total_paginas is None:
                total_paginas = meta.get("total_pages", 1)
                total_registros = meta.get("total_records", 0)
                logger.info(
                    f"Total de registros: {total_registros} | "
                    f"Total de páginas: {total_paginas}"
                )

            for registro in registros:
                arquivo_saida.write(json.dumps(registro, ensure_ascii=False) + "\n")

            total_contratos += len(registros)
            logger.info(
                f"Página {pagina_atual}/{total_paginas} — "
                f"{len(registros)} registros coletados"
            )

            if pagina_atual >= total_paginas or not registros:
                break
            pagina_atual += 1

    logger.info(
        f"Extração concluída: {total_contratos} contratos no total. "
        f"Arquivo temporário: {arquivo_temporario}"
    )
    context["ti"].xcom_push(key="contratos_arquivo", value=str(arquivo_temporario))

    return total_contratos  # retorno também vai para XCom (key='return_value')

# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — ARMAZENAMENTO
# ─────────────────────────────────────────────────────────────────────────────

def salvar_postgres(**context):
    criar_tabelas_se_nao_existirem()

    ti = context["ti"]
    arquivo_temporario = ti.xcom_pull(task_ids="extrair_contratos", key="contratos_arquivo")

    if not arquivo_temporario:
        logger.warning("Nenhum arquivo temporário recebido para salvar.")
        return 0

    caminho_arquivo = Path(arquivo_temporario)

    if not caminho_arquivo.exists():
        logger.warning(f"Arquivo temporário não encontrado: {caminho_arquivo}")
        return 0

    def parse_data(valor):
        """Tenta converter string de data para objeto date."""
        if not valor:
            return None
        s = str(valor)
        if "T" in s:
            s = s.split("T")[0]
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                d = datetime.strptime(s, fmt).date()
                if d.year < 1900:
                    return None
                return d
            except ValueError:
                continue
        return None

    def parse_valor(v):
        """Remove formatação BR e converte para float."""
        if v is None:
            return None
        try:
            return float(str(v).replace("R$", "").replace(".", "").replace(",", ".").strip())
        except (ValueError, AttributeError):
            return None

    registros = []
    try:
        with caminho_arquivo.open("r", encoding="utf-8") as arquivo_entrada:
            for linha in arquivo_entrada:
                linha = linha.strip()
                if not linha:
                    continue

                c = json.loads(linha)
                data_assinatura = parse_data(c.get("data_assinatura"))
                data_inicio = parse_data(c.get("data_inicio"))
                data_fim = parse_data(c.get("data_termino"))

                # Essa é uma das principais features para o modelo de anomalia
                prazo_dias = None
                if data_inicio and data_fim and data_fim > data_inicio:
                    prazo_dias = (data_fim - data_inicio).days

                # valor_contrato = valor original; valor_atualizado_concedente = com aditivos
                valor_inicial = parse_valor(c.get("valor_contrato"))
                valor_global = parse_valor(
                    c.get("valor_atualizado_concedente") or c.get("valor_contrato")
                )

                registros.append((
                    c.get("isn_sic"),
                    c.get("num_contrato"),
                    c.get("descricao_objeto"),
                    c.get("descricao_nome_credor"),
                    c.get("plain_cpf_cnpj_financiador") or c.get("cpf_cnpj_financiador"),
                    c.get("cod_orgao"),
                    c.get("descricao_modalidade"),
                    valor_inicial,
                    valor_global,
                    data_assinatura,
                    data_inicio,
                    data_fim,
                    prazo_dias,
                    json.dumps(c, ensure_ascii=False),  # serializa o JSON original
                ))

        if not registros:
            logger.warning("Arquivo temporário sem contratos para salvar.")
            return 0

        # ── Inserção em lote com ON CONFLICT para idempotência ────────────────────
        sql = """
            INSERT INTO contratos (
                isn_sic, numero_contrato, objeto, fornecedor_nome, fornecedor_cnpj,
                orgao_nome, modalidade, valor_inicial, valor_global,
                data_assinatura, data_inicio_vigencia, data_fim_vigencia,
                prazo_vigencia_dias, json_original
            ) VALUES %s
            ON CONFLICT (isn_sic) DO NOTHING
        """

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, registros, page_size=500)
                inseridos = cur.rowcount
            conn.commit()

        logger.info(
            f"{len(registros)} contratos processados | "
            f"{inseridos} novos inseridos no PostgreSQL."
        )

        return inseridos
    finally:
        if caminho_arquivo.exists():
            caminho_arquivo.unlink()
            logger.info(f"Arquivo temporário removido: {caminho_arquivo}")

# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 — DETECÇÃO DE ANOMALIAS
# ─────────────────────────────────────────────────────────────────────────────

def detectar_anomalias(**context):
    """
    Aplica Isolation Forest sobre os contratos dos últimos 365 dias para
    identificar comportamentos financeiros atípicos.

    CONCEITOS ABORDADOS:
      - Isolation Forest: algoritmo não supervisionado baseado em árvores
        que isola anomalias por serem pontos "fáceis de separar" do restante
      - StandardScaler: normalização das features (necessário pois valor e
        prazo têm escalas muito diferentes)
      - Score de anomalia: quanto mais negativo, mais isolado/anômalo
      - Percentil de risco: transformamos o score em percentil 0-100 para
        facilitar comunicação com o negócio

    POR QUE ISOLATION FOREST?
      - Não precisa de dados rotulados (não supervisionado)
      - Eficiente em grandes volumes (complexidade O(n log n))
      - Funciona bem com features numéricas de escalas diferentes
      - Resistente a outliers extremos (não se baseia em distância)
    """

    logger.info("Carregando contratos do PostgreSQL para detecção de anomalias...")

    # Carrega contratos do período configurado para análise
    sql_leitura = """
        SELECT
            id,
            isn_sic,
            numero_contrato,
            objeto,
            fornecedor_nome,
            orgao_nome,
            valor_global,
            valor_inicial,
            prazo_vigencia_dias,
            data_assinatura,
            modalidade
        FROM contratos
        WHERE
            data_assinatura >= CURRENT_DATE - INTERVAL '%s days'
            AND valor_global IS NOT NULL
            AND valor_global > 0
        ORDER BY data_assinatura DESC
    """ % PERIODO_ANALISE_DIAS

    with get_db_connection() as conn:
        df = pd.read_sql(sql_leitura, conn)

    logger.info(f"Contratos carregados para análise: {len(df)} registros")

    if len(df) < 10:
        logger.warning(
            "Poucos contratos para treinar o modelo (mínimo recomendado: 10). "
            "Abortando detecção."
        )
        return 0

    df["valor_por_dia"] = df.apply(
        lambda row: row["valor_global"] / row["prazo_vigencia_dias"]
        if row["prazo_vigencia_dias"] and row["prazo_vigencia_dias"] > 0
        else row["valor_global"],
        axis=1,
    )

    # Feature 4: log do valor (reduz a influência de outliers extremos no treinamento)
    # log1p = log(1 + x), evita log(0)
    df["log_valor_global"] = np.log1p(df["valor_global"])
    df["log_valor_por_dia"] = np.log1p(df["valor_por_dia"])

    # Seleciona as features para o modelo
    features_modelo = [
        "log_valor_global",    # magnitude do contrato (escala logarítmica)
        "log_valor_por_dia",   # eficiência financeira diária (escala logarítmica)
        "prazo_vigencia_dias", # duração do contrato
    ]

    # Remove linhas com NaN nas features (contratos sem prazo definido)
    df_modelo = df[features_modelo + ["id", "numero_contrato", "isn_sic"]].dropna()
    X = df_modelo[features_modelo].values

    logger.info(
        f"Features: {features_modelo} | "
        f"Contratos válidos para o modelo: {len(X)}"
    )

    # ── Normalização ──────────────────────────────────────────────────────────
    # StandardScaler: transforma cada feature para média=0, desvio=1
    # Necessário porque valor (R$ milhões) e prazo (dias) têm escalas muito diferentes
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # ── Treinamento do Isolation Forest ──────────────────────────────────────
    modelo = IsolationForest(
        n_estimators=200,        # número de árvores — mais = mais estável, mais lento
        contamination=CONTAMINACAO,  # fração esperada de anomalias
        random_state=42,         # reprodutibilidade
        n_jobs=-1,               # usa todos os núcleos disponíveis
    )
    modelo.fit(X_scaled)

    # ── Scoring ───────────────────────────────────────────────────────────────
    # predict: 1 = normal, -1 = anomalia
    # score_samples: quanto mais negativo = mais anômalo
    df_modelo = df_modelo.copy()
    df_modelo["predicao"] = modelo.predict(X_scaled)
    df_modelo["score_anomalia"] = modelo.score_samples(X_scaled)

    # Junta as informações originais ao resultado
    # Nota: prazo_vigencia_dias NÃO é incluído aqui porque já existe em df_modelo;
    # incluir causaria colunas duplicadas (_x/_y) e quebraria o acesso posterior.
    df_resultado = df_modelo.merge(
        df[["id", "objeto", "fornecedor_nome", "orgao_nome",
            "valor_global", "data_assinatura"]],
        on="id",
        how="left",
    )

    # Filtra apenas os contratos marcados como anômalos pelo modelo
    df_anomalias = df_resultado[df_resultado["predicao"] == -1].copy()

    # ── Percentil de risco ────────────────────────────────────────────────────
    # O percentil é calculado ENTRE AS ANOMALIAS, não sobre todo o conjunto.
    # Motivo: as anomalias são por definição o top ~5% do modelo (contamination=0.05),
    # então se calculado sobre todos os contratos elas ficariam todas acima do
    # percentil 95 → todas "ALTO". Ranquear entre si distribui ALTO/MÉDIO/BAIXO
    # de forma significativa para priorização de auditorias.
    scores_invertidos = -df_anomalias["score_anomalia"]
    df_anomalias["percentil_risco"] = (
        scores_invertidos.rank(pct=True) * 100
    ).astype(int)

    # ── Classificação de risco ────────────────────────────────────────────────
    def classificar_risco(percentil):
        if percentil >= 90:
            return "ALTO"
        elif percentil >= 70:
            return "MÉDIO"
        else:
            return "BAIXO"

    df_anomalias["nivel_risco"] = df_anomalias["percentil_risco"].apply(classificar_risco)

    qtd_anomalias = len(df_anomalias)
    logger.info(
        f"Anomalias detectadas: {qtd_anomalias} de {len(df_modelo)} contratos analisados "
        f"({qtd_anomalias/len(df_modelo)*100:.1f}%)"
    )

    # Passa o resultado para a próxima task via XCom
    context["ti"].xcom_push(
        key="anomalias",
        value=df_anomalias.to_dict(orient="records"),
    )

    return qtd_anomalias

# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 — SALVAR ANOMALIAS
# ─────────────────────────────────────────────────────────────────────────────

def salvar_anomalias(**context):
    """
    Persiste os contratos anômalos na tabela anomalias_contratos.

    A cada execução, a tabela é limpa e recarregada (truncate + insert).
    Isso garante que reclassificações do modelo reflitam sempre o estado atual.

    ALTERNATIVA PRODUÇÃO: usar INSERT ... ON CONFLICT com data de detecção
    para manter histórico de quando cada contrato foi considerado anômalo.
    """

    ti = context["ti"]
    anomalias = ti.xcom_pull(task_ids="detectar_anomalias", key="anomalias")

    if not anomalias:
        logger.info("Nenhuma anomalia para salvar.")
        return 0

    registros = [
        (
            a.get("isn_sic"),
            a.get("numero_contrato"),
            a.get("objeto"),
            a.get("fornecedor_nome"),
            a.get("orgao_nome"),
            a.get("valor_global"),
            a.get("prazo_vigencia_dias"),
            float(a.get("score_anomalia", 0)),
            int(a.get("percentil_risco", 0)),
            a.get("nivel_risco"),
            a.get("data_assinatura"),
        )
        for a in anomalias
    ]

    sql_truncate = "TRUNCATE TABLE anomalias_contratos RESTART IDENTITY;"

    sql_insert = """
        INSERT INTO anomalias_contratos (
            isn_sic, numero_contrato, objeto, fornecedor_nome, orgao_nome,
            valor_global, prazo_vigencia_dias, score_anomalia,
            percentil_risco, nivel_risco, data_assinatura
        ) VALUES %s
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_truncate)
            execute_values(cur, sql_insert, registros)
        conn.commit()

    # Resumo por nível de risco para o log
    df = pd.DataFrame(anomalias)
    resumo = df["nivel_risco"].value_counts().to_dict() if "nivel_risco" in df else {}
    logger.info(
        f"{len(registros)} anomalias salvas. "
        f"Resumo por risco: ALTO={resumo.get('ALTO', 0)}, "
        f"MÉDIO={resumo.get('MÉDIO', 0)}, "
        f"BAIXO={resumo.get('BAIXO', 0)}"
    )

    return len(registros)

# ─────────────────────────────────────────────────────────────────────────────
# ENVIAR PARA SUPABASE (ATUALIZA O STREAMLIT)
# ─────────────────────────────────────────────────────────────────────────────

def sincronizar_com_supabase(**context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from sqlalchemy import create_engine
    import pandas as pd
    import psycopg2
    from psycopg2.extras import execute_values

    # 1. Conexão Local
    hook_local = PostgresHook(postgres_conn_id="postgres_default")
    engine_local = hook_local.get_sqlalchemy_engine()

    try:
        print("Lendo contrato de teste do Postgres Local...")

        query = """
        SELECT *
        FROM anomalias_contratos
        """
        with engine_local.connect() as conn:
            df = pd.read_sql(query, conn.connection)

            if df.empty:
                print("Contrato não encontrado. Pegando amostra...")
                df = pd.read_sql(
                    "SELECT * FROM anomalias_contratos",
                    conn.connection
                )

        # Remove qualquer duplicata no conjunto de dados local antes de enviar
        if not df.empty:
            df = df.drop_duplicates(subset=["isn_sic"])

        supabase_uri = (
            "postgresql://user:psswd@host:porta/base"
        )

        conn = psycopg2.connect(supabase_uri)
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE anomalias_contratos")

        if not df.empty:
            columns = list(df.columns)
            insert_sql = f"INSERT INTO anomalias_contratos ({', '.join(columns)}) VALUES %s"
            records = [tuple(row) for row in df.to_numpy()]
            execute_values(cur, insert_sql, records)

        conn.commit()
        cur.close()
        conn.close()

        print("Sucesso! O Supabase foi atualizado.")

    except Exception as e:
        print(f"Erro na sincronização: {e}")
        raise e

# ─────────────────────────────────────────────────────────────────────────────
# ENVIAR E-MAIL
# ─────────────────────────────────────────────────────────────────────────────

def enviar_email_relatorio_anomalias(**context):
    import pandas as pd
    import smtplib
    import os
    import logging
    from email.message import EmailMessage
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.models import Variable

    logger = logging.getLogger(__name__)

    # =========================
    # VARIÁVEIS
    # =========================
    try:
        email_user = Variable.get("EMAIL_USER")
        email_pass = Variable.get("EMAIL_PASS")
        email_to = Variable.get(
            "EMAIL_TO",
            default_var="eduardo.ofn@gmail.com"
        )
        smtp_server = Variable.get("EMAIL_SMTP")
        smtp_port = int(Variable.get("EMAIL_PORT"))
    except Exception as e:
        logger.error(f"Erro ao carregar variáveis: {e}")
        return

    # =========================
    # DESTINATÁRIOS
    # =========================
    if isinstance(email_to, str):
        email_to = [email.strip() for email in email_to.replace(";", ",").split(",") if email.strip()]
    elif not isinstance(email_to, (list, tuple)):
        email_to = [str(email_to)]

    if not email_to:
        logger.error("Nenhum destinatário configurado em EMAIL_TO.")
        return

    # =========================
    # BANCO
    # =========================
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT * FROM anomalias_contratos 
            WHERE DATE(detectado_em) = CURRENT_DATE
        """, conn.connection)

    if df.empty:
        logger.info("Nenhuma anomalia encontrada hoje. E-mail não enviado.")
        return

    # =========================
    # EXCEL
    # =========================
    file_path = f"/tmp/anomalias_{context['ds']}.xlsx"
    df.to_excel(file_path, index=False)

    # =========================
    # RESUMO
    # =========================
    resumo = df['nivel_risco'].value_counts().to_dict()
    total_valor = df['valor_global'].sum()
    top5 = df.sort_values(
        by=['percentil_risco', 'score_anomalia'],
        ascending=[False, True]
    ).head(5)

    top5_linhas = []
    for idx, row in enumerate(top5.to_dict(orient='records'), start=1):
        top5_linhas.append(
            f"{idx}. ISN {row.get('isn_sic', 'N/A')} | "
            f"{row.get('fornecedor_nome', 'N/A')} | "
            f"R${row.get('valor_global', 0):,.2f} | "
            f"{row.get('percentil_risco', 0)}% | "
            f"{row.get('nivel_risco', 'N/A')}"
        )

    body_top5 = "\n".join(top5_linhas) if top5_linhas else "Nenhuma anomalia listada."

    corpo_email = f"""
Relatório de Auditoria de Contratos - CE
Data de execução: {context['ds']}

TOTAL DE ANOMALIAS IDENTIFICADAS: {len(df)}
Valor total anômalo: R${total_valor:,.2f}

Distribuição por nível de risco:
- ALTO:  {resumo.get('ALTO', 0)}
- MÉDIO: {resumo.get('MÉDIO', 0)}
- BAIXO: {resumo.get('BAIXO', 0)}

Acesse o dashboard em:
https://anomaliascontratos.streamlit.app/

O arquivo em anexo contém todos os contratos anômalos identificados hoje.
"""

    # =========================
    # EMAIL
    # =========================
    msg = EmailMessage()
    msg['Subject'] = f"Relatório de Anomalias - {context['ds']} ({len(df)} anomalias)"
    msg['From'] = email_user
    msg['To'] = ", ".join(email_to)
    msg.set_content(corpo_email, charset="utf-8")

    with open(file_path, 'rb') as f:
        msg.add_attachment(
            f.read(),
            maintype='application',
            subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            filename=f"relatorio_anomalias_{context['ds']}.xlsx"
        )

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(email_user, email_pass)
        server.send_message(msg)

    # =========================
    # LIMPEZA
    # =========================
    if os.path.exists(file_path):
        os.remove(file_path)

    logger.info(f"E-mail enviado com sucesso para {email_to}!")

# ─────────────────────────────────────────────────────────────────────────────
# DEFINIÇÃO DA DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="anomalias_contratos_ceara",
    description=(
        "Pipeline diário: extrai contratos da API do Ceará Transparente, "
        "armazena no PostgreSQL e detecta anomalias financeiras com Isolation Forest."
    ),
    schedule="0 6 * * *",       # todo dia às 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,               # não executa datas passadas retroativamente
    tags=["contratos", "anomalias", "ceara-transparente", "ml"],
    
    default_args={
        "owner": "daniel_teofilo",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
) as dag:

    # ── Task 1: Extração ──────────────────────────────────────────────────────
    task_extrair = PythonOperator(
        task_id="extrair_contratos",
        python_callable=extrair_contratos,
        doc_md="""
        **Extração paginada da API de contratos do Ceará Transparente.**
        Coleta todos os contratos assinados nos últimos 365 dias.
        Salva os dados brutos em arquivo temporário e passa via XCom apenas o caminho
        desse arquivo (key: `contratos_arquivo`).
        """,
    )

    # ── Task 2: Armazenamento ─────────────────────────────────────────────────
    task_salvar = PythonOperator(
        task_id="salvar_postgres",
        python_callable=salvar_postgres,
        doc_md="""
        **Persistência no PostgreSQL.**
        Lê o arquivo temporário gerado pela extração, normaliza e insere os contratos
        na tabela `contratos`.
        Operação idempotente via ON CONFLICT DO NOTHING.
        """,
    )

    # ── Task 3: Detecção de Anomalias ─────────────────────────────────────────
    task_anomalias = PythonOperator(
        task_id="detectar_anomalias",
        python_callable=detectar_anomalias,
        doc_md="""
        **Detecção de anomalias com Isolation Forest.**
        Analisa os contratos dos últimos 365 dias.
        Features: log(valor_global), log(valor/dia), prazo_vigencia_dias.
        Resultado passado para próxima task via XCom (key: `anomalias`).
        """,
    )

    # ── Task 4: Salvar Anomalias ──────────────────────────────────────────────
    task_salvar_anomalias = PythonOperator(
        task_id="salvar_anomalias",
        python_callable=salvar_anomalias,
        doc_md="""
        **Persistência das anomalias detectadas.**
        Salva na tabela `anomalias_contratos` com score e nível de risco.
        Tabela é recarregada a cada execução (truncate + insert).
        """,
    )

    # ── Task 5: Envio de E-mail (ITEM 3 DA ATIVIDADE) ──────────────────────────
    task_email = PythonOperator(
        task_id="enviar_email",
        python_callable=enviar_email_relatorio_anomalias,
        doc_md="""
        **Envio diário de relatório por e-mail.**
        Gera um arquivo .xlsx com as anomalias do dia e envia para o Yahoo
        contendo o resumo de riscos no corpo da mensagem.
        """,
    )

    task_sync_supabase = PythonOperator(
        task_id="sincronizar_supabase",
        python_callable=sincronizar_com_supabase
    )

    # ── Dependências (ordem de execução) ─────────────────────────────────
    task_extrair >> task_salvar >> task_anomalias >> task_salvar_anomalias >> task_sync_supabase >> task_email
