# app.py (Versão 8.0 - O Portal Final com Status Automático e 3 APIs)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
from cachetools import TTLCache
import requests
import time
from datetime import datetime, timezone

# --- CONFIGURAÇÃO GLOBAL ---
CONCURRENCY_LIMIT = 50; MAX_RETRIES = 5; REQUEST_TIMEOUT = 20
# URLs das 3 APIs
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
AWESOMEAPI_URL = "https://cep.awesomeapi.com.br/json/{cep}"
# -----------------------------
cep_cache = TTLCache(maxsize=20_000, ttl=86400) # Nosso cache robusto para a consulta em lote

# --- FUNÇÕES DE BACKEND ---

# <<--- FUNÇÃO DE STATUS COM AUTO-ATUALIZAÇÃO DE 60 SEGUNDOS --->>
@st.cache_data(ttl=60, show_spinner=False)
def get_api_statuses():
    """Verifica a saúde de todas as APIs. O cache garante que isso só rode a cada 60s."""
    statuses = {}
    apis = {
        "BrasilAPI": BRASILAPI_V2_URL,
        "ViaCEP": VIACEP_URL,
        "AwesomeAPI": AWESOMEAPI_URL
    }
    for name, url in apis.items():
        try:
            start_time = time.monotonic()
            r = requests.get(url.format(cep="01001000"), timeout=5)
            end_time = time.monotonic()
            latency = int((end_time - start_time) * 1000)
            is_ok = r.ok and ("erro" not in r.text)
            statuses[name] = {"status": "Online" if is_ok else "Com Erros", "latency": latency}
        except requests.exceptions.RequestException:
            statuses[name] = {"status": "Offline", "latency": -1}
    st.session_state.last_check_time = datetime.now(timezone.utc)
    return statuses

def display_api_status_dashboard():
    """Componente que exibe um dashboard de status dinâmico e auto-atualizável."""
    statuses = get_api_statuses()
    if 'last_check_time' in st.session_state:
        time_diff = (datetime.now(timezone.utc) - st.session_state.last_check_time).total_seconds()
        st.caption(f"Status dos serviços (verificado há {int(time_diff)} segundos)")

    cols = st.columns(len(statuses))
    for col, (name, data) in zip(cols, statuses.items()):
        with col:
            with st.container(border=True):
                icon = "✅" if data['status'] == "Online" else "❌"
                st.markdown(f"**{name}** {icon}")
                latency_text = f"{data['latency']} ms" if data['latency'] >= 0 else "N/A"
                st.metric("Resposta", latency_text)
# <<----------------------------------------------------------------------->>

# (Funções de consulta e exibição atualizadas para incluir a AwesomeAPI)
@st.cache_data(ttl=3600) # Cache para consultas individuais
def consulta_cep_completa(cep):
    results = {}
    try: # BrasilAPI
        r = requests.get(BRASILAPI_V2_URL.format(cep=cep), timeout=5)
        if r.ok: results['BrasilAPI'] = {"data": r.json(), "status": "Sucesso"}
        else: results['BrasilAPI'] = {"data": None, "status": "Não encontrado"}
    except: results['BrasilAPI'] = {"data": None, "status": "Serviço indisponível"}
    try: # ViaCEP
        r = requests.get(VIACEP_URL.format(cep=cep), timeout=5)
        if r.ok and 'erro' not in r.text: results['ViaCEP'] = {"data": r.json(), "status": "Sucesso"}
        else: results['ViaCEP'] = {"data": None, "status": "Não encontrado"}
    except: results['ViaCEP'] = {"data": None, "status": "Serviço indisponível"}
    try: # AwesomeAPI
        r = requests.get(AWESOMEAPI_URL.format(cep=cep), timeout=5)
        if r.ok: results['AwesomeAPI'] = {"data": r.json(), "status": "Sucesso"}
        else: results['AwesomeAPI'] = {"data": None, "status": "Não encontrado"}
    except: results['AwesomeAPI'] = {"data": None, "status": "Serviço indisponível"}
    return results

def display_result_card(resultado: dict, api_name: str, status: str):
    with st.container(border=True):
        st.subheader(api_name, anchor=False)
        if status == "Sucesso":
            st.text(f"CEP: {resultado.get('cep') or resultado.get('code', 'N/A')}")
            st.text(f"Endereço: {resultado.get('street') or resultado.get('logradouro') or resultado.get('address', 'N/A')}")
            st.text(f"Bairro: {resultado.get('neighborhood') or resultado.get('bairro') or resultado.get('district', 'N/A')}")
            st.text(f"Cidade/UF: {resultado.get('city') or resultado.get('localidade', 'N/A')} / {resultado.get('state') or resultado.get('uf', 'N/A')}")
        else: st.error(status)

# (O robusto backend da consulta em lote permanece igual, focado na BrasilAPI por eficiência)
async def fetch_cep_data(cep: str) -> dict: #...
    if cep in cep_cache: return cep_cache[cep]
    if not cep or not cep.isdigit() or len(cep) != 8: return {"status_consulta": "Formato Inválido"}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(BRASILAPI_V2_URL.format(cep=cep), timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                data = response.json(); result = {"estado": data.get("state"), "cidade": data.get("city"), "bairro": data.get("neighborhood"), "logradouro": data.get("street"), "status_consulta": "Sucesso"}
                cep_cache[cep] = result; return result
        except: pass
    return {"status_consulta": f"Falha"}
async def processar_dataframe(df, cep_col):#...
    df['cep_padronizado'] = df[cep_col].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async def run_fetch(cep):
        async with semaphore: return await fetch_cep_data(cep)
    tasks = [run_fetch(cep) for cep in df['cep_padronizado'].tolist()]
    results = []; placeholder = st.empty()
    for i, f in enumerate(asyncio.as_completed(tasks)):
        results.append(await f); placeholder.text(f"Progresso: {i + 1} de {len(tasks)} consultados...")
    placeholder.success("Processamento concluído!")
    cep_to_result_map = dict(zip(df['cep_padronizado'], results))
    ordered_results = [cep_to_result_map[cep] for cep in df['cep_padronizado']]
    return pd.concat([df.drop(columns=['cep_padronizado']), pd.DataFrame(ordered_results)], axis=1)
def to_excel_bytes(df):#...
    output = BytesIO();
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados')
    return output.getvalue()


# --- INTERFACE GRÁFICA PRINCIPAL ---
st.set_page_config(page_title="Serviços CEP - Capital Consig", layout="wide")

with st.sidebar:
    st.image("logo.png", use_container_width=True)
    st.title("Capital Consig")
    st.info("Portal de Serviços de CEP.")

st.header("Portal de Serviços de CEP")
st.divider()

tab_individual, tab_lote = st.tabs(["🔍 Consulta Individual", "📦 Consulta em Lote"])

with tab_individual:
    display_api_status_dashboard()
    st.subheader("Consulta Rápida por CEP")
    cep_input = st.text_input("Digite o CEP (apenas números):", max_chars=8)
    if cep_input and len(cep_input) == 8 and cep_input.isdigit():
        with st.spinner("Buscando em todas as fontes..."):
            resultados = consulta_cep_completa(cep_input)
        
        st.divider()
        st.subheader("Resultados:")
        cols = st.columns(len(resultados))
        for col, (api_name, result_data) in zip(cols, resultados.items()):
            with col:
                display_result_card(result_data['data'], api_name, result_data['status'])
    elif len(cep_input) > 0:
        st.warning("Por favor, digite um CEP válido com 8 dígitos.")

with tab_lote:
    display_api_status_dashboard()
    st.subheader("Consulta de Múltiplos CEPs em Lote")
    uploaded_file = st.file_uploader("Selecione o arquivo para processamento", label_visibility="collapsed")
    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str) if uploaded_file.name.lower().endswith('.xlsx') else pd.read_csv(uploaded_file, dtype=str)
            cep_col = next((col for col in df.columns if 'cep' in str(col).lower()), None)
            if not cep_col: st.error("Nenhuma coluna com 'CEP' foi encontrada.")
            else:
                st.success(f"Arquivo '{uploaded_file.name}' carregado, com {len(df)} registros.")
                if st.button("Processar Planilha", use_container_width=True):
                    df_final = asyncio.run(processar_dataframe(df, cep_col))
                    st.subheader("Processamento Concluído")
                    st.dataframe(df_final, use_container_width=True)
                    st.download_button("Baixar Resultados", to_excel_bytes(df_final), f"{uploaded_file.name.split('.')[0]}_PROCESSADO.xlsx", use_container_width=True)
        except Exception as e: st.error(f"Erro ao processar: {e}")
