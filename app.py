# app.py (Versão 5.0 - Status "Live" e Interface Simplificada)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
from cachetools import TTLCache
import requests
from datetime import datetime, timezone

# --- CONFIGURAÇÃO GLOBAL ---
CONCURRENCY_LIMIT = 50; MAX_RETRIES = 5; REQUEST_TIMEOUT = 20
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
cep_cache = TTLCache(maxsize=20_000, ttl=86400)
# -----------------------------

# --- Inicialização do Estado da Sessão ---
# Guarda os resultados da verificação de status para que não se percam ao interagir com a página
if 'last_check_time' not in st.session_state:
    st.session_state.last_check_time = None
    st.session_state.status_results = None
# ---------------------------------------------

# --- FUNÇÕES AUXILIARES ---
def display_result_card(resultado: dict, api_name: str, status: str):
    with st.container(border=True):
        st.subheader(f"Resultado {api_name}", anchor=False)
        if status == "Sucesso":
            st.text(f"CEP: {resultado.get('cep', 'N/A')}")
            st.text(f"Logradouro: {resultado.get('street') or resultado.get('logradouro', 'N/A')}")
            st.text(f"Bairro: {resultado.get('neighborhood') or resultado.get('bairro', 'N/A')}")
            st.text(f"Cidade/UF: {resultado.get('city') or resultado.get('localidade', 'N/A')} - {resultado.get('state') or resultado.get('uf', 'N/A')}")
        else:
            st.error(status)

# --- FUNÇÕES DE BACKEND ---
def consulta_brasilapi(cep):
    try:
        response = requests.get(BRASILAPI_V2_URL.format(cep=cep), timeout=5)
        if response.status_code == 200: return response.json(), "Sucesso"
    except: pass
    return None, "Serviço indisponível ou CEP não encontrado"

def consulta_viacep(cep):
    try:
        response = requests.get(VIACEP_URL.format(cep=cep), timeout=5)
        if response.status_code == 200 and 'erro' not in response.json(): return response.json(), "Sucesso"
    except: pass
    return None, "Serviço indisponível ou CEP não encontrado"

def check_api_status(url_template):
    try:
        start_time = datetime.now(timezone.utc).timestamp()
        response = requests.get(url_template.format(cep="01001000"), timeout=5)
        end_time = datetime.now(timezone.utc).timestamp()
        latency = round((end_time - start_time) * 1000)
        return {"status": "Online" if response.ok else "Com Erros", "latency": latency}
    except requests.exceptions.RequestException:
        return {"status": "Offline", "latency": -1}

# O robusto backend da consulta em lote permanece igual.
async def fetch_cep_data(cep: str) -> dict: # (sem alterações)
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
async def processar_dataframe(df, cep_column_name): # ... (sem alterações)
    df['cep_padronizado'] = df[cep_column_name].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
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
def to_excel_bytes(df): # ... (sem alterações)
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

# A aba "Buscar por Endereço" foi removida.
tab_individual, tab_lote, tab_status = st.tabs(["🔍 Consulta Individual", "📦 Consulta em Lote", "🚦 Status dos Serviços"])

with tab_individual:
    st.subheader("Consulta Rápida por CEP")
    cep_input = st.text_input("Digite o CEP (apenas números):", max_chars=8)
    if st.button("Consultar", key="btn_consultar_cep"):
        if cep_input and len(cep_input) == 8 and cep_input.isdigit():
            with st.spinner("Buscando informações..."):
                res_br, status_br = consulta_brasilapi(cep_input)
                res_via, status_via = consulta_viacep(cep_input)
            st.divider()
            col1, col2 = st.columns(2)
            with col1: display_result_card(res_br, "BrasilAPI", status_br)
            with col2: display_result_card(res_via, "ViaCEP", status_via)
        else:
            st.warning("Por favor, digite um CEP válido com 8 dígitos.")

with tab_lote:
    st.subheader("Consulta de Múltiplos CEPs em Lote")
    st.markdown("Carregue sua planilha para processar todos os CEPs de uma vez.")
    uploaded_file = st.file_uploader("Selecione o arquivo", type=["xlsx", "csv"], label_visibility="collapsed")
    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str) if uploaded_file.name.lower().endswith('.xlsx') else pd.read_csv(uploaded_file, dtype=str)
            cep_col = next((col for col in df.columns if 'cep' in str(col).lower()), None)
            if not cep_col: st.error("ERRO: Nenhuma coluna com 'CEP' no nome foi encontrada.")
            else:
                st.success(f"Arquivo '{uploaded_file.name}' carregado, com {len(df)} registros para processar.")
                if st.button("Processar Planilha em Lote", use_container_width=True):
                    df_final = asyncio.run(processar_dataframe(df, cep_col))
                    st.subheader("Processamento Concluído")
                    st.dataframe(df_final, use_container_width=True)
                    st.download_button("Baixar Planilha Processada", to_excel_bytes(df_final), f"{uploaded_file.name.split('.')[0]}_PROCESSADO.xlsx", use_container_width=True)
        except Exception as e: st.error(f"Erro ao processar: {e}")

# <<--- ABA DE STATUS COMPLETAMENTE RECONSTRUÍDA --->>
with tab_status:
    st.subheader("Status dos Serviços de API")
    st.write("Verifique em tempo real a saúde e o tempo de resposta das APIs de CEP utilizadas pelo portal.")

    if st.button("Verificar Status Agora"):
        with st.spinner("Verificando..."):
            status_br_data = check_api_status(BRASILAPI_V2_URL)
            status_via_data = check_api_status(VIACEP_URL)
            st.session_state.status_results = {
                "BrasilAPI": status_br_data,
                "ViaCEP": status_via_data
            }
            st.session_state.last_check_time = datetime.now(timezone.utc)
    
    if st.session_state.status_results:
        last_check = st.session_state.last_check_time
        time_diff = (datetime.now(timezone.utc) - last_check).total_seconds()
        st.caption(f"Última verificação: {last_check.strftime('%H:%M:%S')} (há {int(time_diff)} segundos)")

        st.divider()

        results = st.session_state.status_results
        col1, col2 = st.columns(2)

        with col1:
            with st.container(border=True):
                api_name = "BrasilAPI"
                status = results[api_name]['status']
                latency = results[api_name]['latency']
                st.subheader(api_name, anchor=False)
                if status == 'Online':
                    st.success("✅ Online", icon="✅")
                    st.metric("Tempo de Resposta", f"{latency} ms")
                else:
                    st.error(f"❌ {status}", icon="❌")
        
        with col2:
            with st.container(border=True):
                api_name = "ViaCEP"
                status = results[api_name]['status']
                latency = results[api_name]['latency']
                st.subheader(api_name, anchor=False)
                if status == 'Online':
                    st.success("✅ Online", icon="✅")
                    st.metric("Tempo de Resposta", f"{latency} ms")
                else:
                    st.error(f"❌ {status}", icon="❌")
    else:
        st.info("Clique no botão acima para verificar o status atual dos serviços.")
