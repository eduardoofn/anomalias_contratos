# 🚀 Projeto de Engenharia de Dados: Detecção de Anomalias em Contratos Públicos (Ceará)

Pipeline ETL automatizado focado em transparência pública e auditoria automatizada, utilizando Machine Learning para identificar contratos com comportamento financeiro atípico no estado do Ceará.

## 📋 Índice

* [Sobre o Projeto](#-sobre-o-projeto)
* [Arquitetura do Pipeline](#️-arquitetura-do-pipeline)
* [Stack Tecnológica](#️-stack-tecnológica)
* [Detalhamento das Etapas](#-detalhamento-das-etapas)
* [Pré-requisitos](#-pré-requisitos)
* [Como Executar](#-como-executar)
* [Resultados Alcançados](#-resultados-alcançados)
* [Referência](#-referência)

## 🎯 Sobre o Projeto

Este projeto foi desenvolvido como desafio final do curso **Python para Dados** na **Digital College Brasil**, sob orientação do professor **Daniel Teófilo**.

### 💡 Problemática (Solicitação da Área de Negócio)
A **Controladoria-Geral do Estado** solicitou ao time de dados um pipeline automatizado capaz de identificar, diariamente, contratos públicos com comportamento financeiro atípico. O critério não é uma regra fixa de valor — o objetivo é detectar padrões que fujam do comportamento histórico do conjunto (anomalias), permitindo priorizar auditorias humanas onde o risco estatístico é maior.

## 🏗️ Arquitetura do Pipeline

A solução foi desenhada para ser escalável e automatizada, integrando processamento local e sincronização em nuvem:

1.  **Extract:** Coleta de dados via API paginada do Portal da Transparência do Ceará.
2.  **Transform & ML:** Saneamento de dados e aplicação de modelo não supervisionado.
3.  **Load:** Persistência em banco local (PostgreSQL) e sincronização em nuvem (Supabase).
4.  **Visualize:** Dashboard interativo para análise dos auditores e envio de relatórios.

**Fluxo Geral:**
`API Transparência` → `PostgreSQL (Staging)` → `Python/Scikit-Learn (ML)` → `Supabase (Cloud)` → `Streamlit Dashboard`

## 🛠️ Stack Tecnológica

* **Linguagem:** Python 3.10+
* **Orquestração:** Apache Airflow
* **Banco de Dados:** PostgreSQL (Local) & Supabase (Cloud)
* **Machine Learning:** Scikit-Learn (Isolation Forest)
* **Visualização:** Streamlit
* **Infraestrutura:** Docker & Docker Compose
* **Bibliotecas:** Pandas, Requests, Psycopg2, Numpy, Scikit-learn

## 📝 Detalhamento das Etapas

O pipeline é orquestrado pela DAG `anomalias_contratos_ceara`, composta por 4 tasks principais:

* **`extrair_contratos`**: Coleta dados da [API do Ceará Transparente](https://api-dados-abertos.cearatransparente.ce.gov.br/), tratando paginação e inconsistências de volume.
* **`salvar_postgres`**: Persiste os contratos brutos no PostgreSQL para garantir a rastreabilidade e integridade (Staging).
* **`detectar_anomalias`**: Aplica o algoritmo **Isolation Forest** sobre features numéricas para classificar contratos com comportamento financeiro atípico (Risco Alto, Médio e Baixo).
* **`salvar_anomalias`**: Sincroniza os dados com o Supabase e dispara o envio diário de relatórios automatizados em `.xlsx` via e-mail.

## 📌 Pré-requisitos

* Docker e Docker Compose instalados.
* Chaves de API do Supabase configuradas.
* Acesso à internet para consumo da API pública do Ceará.

## 🚀 Como Executar

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/seu-usuario/projeto-anomalias-ceara.git](https://github.com/seu-usuario/projeto-anomalias-ceara.git)
    ```
2.  **Configuração de ambiente:**
    Renomeie o arquivo `.env.example` para `.env` e preencha com suas credenciais de banco e e-mail.
3.  **Suba os containers:**
    ```bash
    docker-compose up -d
    ```
4.  **Ative a DAG:** Acesse `localhost:8080` (Airflow) e ative o fluxo `anomalias_contratos_ceara`.

## 🎯 Resultados Alcançados

* **Auditoria Inteligente:** Transição de uma análise manual para uma abordagem baseada em dados e risco estatístico.
* **Pipeline End-to-End:** Consolidação de conhecimentos em ETL, Orquestração, Modelagem de Dados e Cloud.
* **Valor para o Negócio:** Entrega de um produto de dados pronto para consumo por áreas de controladoria e auditoria.

## 📚 Referência

Projeto desenvolvido como parte do aprendizado em Engenharia de Dados na **Digital College Brasil**, seguindo as orientações do professor **Daniel Teófilo**.

---

📧 **Contato:**
Conecte-se comigo no [LinkedIn](https://linkedin.com/in/eduardoofn)