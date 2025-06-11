# app.py (Versão 7.0 - O Portal Final com Dashboard de Status "Live")
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
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
cep_cache = TTLCache(maxsize=20_000, ttl=86400)
# -----------------------------

# --- Inicialização do Estado da Sessão ---
# Essencial para "lembrar" dos dados do status entre as interações do usuário.
if 'last_status_check' not in st.session_state:
    st.session_state.last_status_check = None
    st.session_state.api_statuses = {}
# ---------------------------------------------


# --- FUNÇÕES DE BACKEND ---

def check_api_status(api_name: str, url_template: str) -> dict:
    """Função robusta para verificar a saúde de uma API."""
    try:
        start_time = time.monotonic()
        response = requests.get(url_template.format(cep="01001000"), timeout=5)
        end_time = time.monotonic()
        latency = round((end_time - start_time) * 1000)
        
        if api_name == "ViaCEP" and "erro" in response.text:
            return {"status": "Com Erros", "latency": latency}
        return {"status": "Online" if response.ok else "Com Erros", "latency": latency}
    except requests.exceptions.RequestException:
        return {"status": "Offline", "latency": -1}

# <<--- O NOVO PAINEL DE STATUS INTERATIVO --->>
def display_api_status_dashboard():
    """Componente que exibe um dashboard de status dinâmico e interativo."""
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.caption("STATUS DOS SERVIÇOS DE CONSULTA")
    with col2:
        # Botão para forçar a atualização do status
        if st.button("🔄 Atualizar Agora", use_container_width=True):
            with st.spinner('Verificando APIs...'):
                st.session_state.api_statuses['BrasilAPI'] = check_api_status("BrasilAPI", BRASILAPI_V2_URL)
                st.session_state.api_statuses['ViaCEP'] = check_api_status("ViaCEP", VIACEP_URL)
                st.session_state.last_status_check = datetime.now(timezone.utc)
            st.rerun() # Força o recarregamento da página para mostrar os novos dados imediatamente

    # Verifica se já temos dados de status para exibir
    if not st.session_state.api_statuses:
        st.info("Clique em 'Atualizar Agora' para verificar a saúde das APIs.")
        return # Não mostra nada se não houver dados

    # Se temos dados, mostra os cards de status
    api_col1, api_col2 = st.columns(2)
    
    with api_col1:
        with st.container(border=True):
            status_data = st.session_state.api_statuses.get('BrasilAPI', {"status": "Não verificado", "latency": ""})
            st.subheader("BrasilAPI", anchor=False)
            icon = "✅" if status_data['status'] == "Online" else "❌"
            st.markdown(f"**Status:** {icon} {status_data['status']}")
            st.metric("Tempo de Resposta", f"{status_data['latency']} ms" if isinstance(status_data['latency'], int) else "N/A")
            
    with api_col2:
        with st.container(border=True):
            status_data = st.session_state.api_statuses.get('ViaCEP', {"status": "Não verificado", "latency": ""})
            st.subheader("ViaCEP", anchor=False)
            icon = "✅" if status_data['status'] == "Online" else "❌"
            st.markdown(f"**Status:** {icon} {status_data['status']}")
            st.metric("Tempo de Resposta", f"{status_data['latency']} ms" if isinstance(status_data['latency'], int) else "N/A")

    # Mostra o tempo desde a última verificação
    if st.session_state.last_status_check:
        time_diff = (datetime.now(timezone.utc) - st.session_state.last_status_check).total_seconds()
        st.caption(f"Última verificação há {int(time_diff)} segundos.")

# <<-------------------------------------------->>

# (Todas as outras funções de backend e exibição, como display_result_card, etc., permanecem as mesmas)
def display_result_card(resultado, api_name, status):
    with st.container(border=True):
        st.subheader(f"Resultado {api_name}", anchor=False)
        if status == "Sucesso": st.text(f"CEP: {resultado.get('cep', 'N/A')}\nLogradouro: {resultado.get('street') or resultado.get('logradouro', 'N/A')}\nBairro: {resultado.get('neighborhood') or resultado.get('bairro', 'N/A')}\nCidade/UF: {resultado.get('city') or resultado.get('localidade', 'N/A')} - {resultado.get('state') or resultado.get('uf', 'N/A')}")
        else: st.error(status)
def consulta_brasilapi(cep):
    try:
        response = requests.get(BRASILAPI_V2_URL.format(cep=cep), timeout=5)
        if response.status_code == 200: return response.json(), "Sucesso"
    except: pass
    return None, "Serviço indisponível"
def consulta_viacep(cep):
    try:
        response = requests.get(VIACEP_URL.format(cep=cep), timeout=5)
        if response.status_code == 200 and 'erro' not in response.json(): return response.json(), "Sucesso"
    except: pass
    return None, "Serviço indisponível"
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
    return {"status_consulta": "Falha"}
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

tab_individual, tab_lote = st.tabs(["🔍 Consulta Individual", "📦 Consulta em Lote"])

with tab_individual:
    display_api_status_dashboard()
    st.divider()
    st.subheader("Consulta Rápida por CEP")
    cep_input = st.text_input("Digite o CEP (apenas números):", max_chars=8)
    if st.button("Consultar"):
        if cep_input and len(cep_input) == 8 and cep_input.isdigit():
            with st.spinner("Buscando informações..."):
                res_br, status_br = consulta_brasilapi(cep_input)
                res_via, status_via = consulta_viacep(cep_input)
            col1, col2 = st.columns(2)
            with col1: display_result_card(res_br, "BrasilAPI", status_br)
            with col2: display_result_card(res_via, "ViaCEP", status_via)
        else: st.warning("Digite um CEP válido com 8 dígitos.")

with tab_lote:
    display_api_status_dashboard()
    st.divider()
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
