# app.py (Versão 9.0 - A Versão Final com Consulta em Lote Multi-API)
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
CONCURRENCY_LIMIT = 30 # Reduzimos um pouco para não sobrecarregar as APIs com 3x mais tráfego
MAX_RETRIES = 3 # Retentativas por cada chamada individual de API
REQUEST_TIMEOUT = 15
# URLs das 3 APIs
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
AWESOMEAPI_URL = "https://cep.awesomeapi.com.br/json/{cep}"
# -----------------------------

# --- FUNÇÕES DE BACKEND ---

# Painel de Status Automático (sem alterações)
@st.cache_data(ttl=60, show_spinner=False)
def get_api_statuses(): #... (código idêntico à versão anterior)
    statuses = {}; apis = {"BrasilAPI": BRASILAPI_V2_URL, "ViaCEP": VIACEP_URL, "AwesomeAPI": AWESOMEAPI_URL}
    for name, url in apis.items():
        try:
            start_time = time.monotonic()
            r = requests.get(url.format(cep="01001000"), timeout=5)
            end_time = time.monotonic()
            latency = int((end_time - start_time) * 1000)
            is_ok = r.ok and ("erro" not in r.text)
            statuses[name] = {"status": "Online" if is_ok else "Com Erros", "latency": latency}
        except: statuses[name] = {"status": "Offline", "latency": -1}
    st.session_state.last_check_time = datetime.now(timezone.utc)
    return statuses
def display_api_status_dashboard(): #... (código idêntico à versão anterior)
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
                st.metric("Resposta", f"{data['latency']} ms" if data['latency'] >= 0 else "N/A")

# Funções de Consulta Individual (sem alterações)
@st.cache_data(ttl=3600)
def consulta_cep_completa(cep): #... (código idêntico à versão anterior)
    results = {}
    try: r = requests.get(BRASILAPI_V2_URL.format(cep=cep), timeout=5); results['BrasilAPI'] = {"data": r.json(), "status": "Sucesso"} if r.ok else {"data": None, "status": "Não encontrado"}
    except: results['BrasilAPI'] = {"data": None, "status": "Serviço indisponível"}
    try: r = requests.get(VIACEP_URL.format(cep=cep), timeout=5); results['ViaCEP'] = {"data": r.json(), "status": "Sucesso"} if r.ok and 'erro' not in r.text else {"data": None, "status": "Não encontrado"}
    except: results['ViaCEP'] = {"data": None, "status": "Serviço indisponível"}
    try: r = requests.get(AWESOMEAPI_URL.format(cep=cep), timeout=5); results['AwesomeAPI'] = {"data": r.json(), "status": "Sucesso"} if r.ok else {"data": None, "status": "Não encontrado"}
    except: results['AwesomeAPI'] = {"data": None, "status": "Serviço indisponível"}
    return results
def display_result_card(resultado, api_name, status): #... (código idêntico à versão anterior)
    with st.container(border=True):
        st.subheader(api_name, anchor=False)
        if status == "Sucesso":
            st.text(f"CEP: {resultado.get('cep') or resultado.get('code', 'N/A')}")
            st.text(f"Endereço: {resultado.get('street') or resultado.get('logradouro') or resultado.get('address', 'N/A')}")
            st.text(f"Bairro: {resultado.get('neighborhood') or resultado.get('bairro') or resultado.get('district', 'N/A')}")
            st.text(f"Cidade/UF: {resultado.get('city') or resultado.get('localidade', 'N/A')} / {resultado.get('state') or resultado.get('uf', 'N/A')}")
        else: st.error(status)

# <<--- O NOVO E ROBUSTO BACKEND DE CONSULTA EM LOTE ---
async def fetch_all_apis_for_cep_lote(cep: str, session: httpx.AsyncClient) -> dict:
    """Consulta um CEP em todas as 3 APIs de forma assíncrona e consolida os resultados."""
    if not cep or not cep.isdigit() or len(cep) != 8:
        return {'status_geral': 'Formato Inválido'}
    
    # Executa as 3 consultas em paralelo para o mesmo CEP
    tasks = [
        session.get(BRASILAPI_V2_URL.format(cep=cep)),
        session.get(VIACEP_URL.format(cep=cep)),
        session.get(AWESOMEAPI_URL.format(cep=cep))
    ]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Consolida os resultados em um único dicionário
    consolidated_result = {}
    
    # Processa BrasilAPI
    if not isinstance(responses[0], Exception) and responses[0].status_code == 200:
        data = responses[0].json()
        consolidated_result['status_brasilapi'] = 'Sucesso'
        consolidated_result['rua_brasilapi'] = data.get('street')
        consolidated_result['bairro_brasilapi'] = data.get('neighborhood')
        consolidated_result['cidade_brasilapi'] = data.get('city')
        consolidated_result['estado_brasilapi'] = data.get('state')
    else: consolidated_result['status_brasilapi'] = 'Falha'

    # Processa ViaCEP
    if not isinstance(responses[1], Exception) and responses[1].status_code == 200 and 'erro' not in responses[1].text:
        data = responses[1].json()
        consolidated_result['status_viacep'] = 'Sucesso'
        consolidated_result['rua_viacep'] = data.get('logradouro')
        consolidated_result['bairro_viacep'] = data.get('bairro')
        consolidated_result['cidade_viacep'] = data.get('localidade')
        consolidated_result['estado_viacep'] = data.get('uf')
    else: consolidated_result['status_viacep'] = 'Falha'
    
    # Processa AwesomeAPI
    if not isinstance(responses[2], Exception) and responses[2].status_code == 200:
        data = responses[2].json()
        consolidated_result['status_awesomeapi'] = 'Sucesso'
        consolidated_result['rua_awesomeapi'] = data.get('address')
        consolidated_result['bairro_awesomeapi'] = data.get('district')
        consolidated_result['cidade_awesomeapi'] = data.get('city')
        consolidated_result['estado_awesomeapi'] = data.get('state')
    else: consolidated_result['status_awesomeapi'] = 'Falha'
        
    return consolidated_result

async def processar_dataframe_multi_api(df: pd.DataFrame, cep_column_name: str) -> pd.DataFrame:
    df['cep_padronizado'] = df[cep_column_name].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async def run_fetch(cep, session):
        async with semaphore: return await fetch_all_apis_for_cep_lote(cep, session)

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as session:
        tasks = [run_fetch(cep, session) for cep in df['cep_padronizado'].tolist()]
        results = []
        placeholder = st.empty()
        for i, f in enumerate(asyncio.as_completed(tasks)):
            results.append(await f)
            placeholder.text(f"Progresso: {i + 1} de {len(tasks)} CEPs consultados em 3 fontes...")
        placeholder.success("Processamento multi-API concluído!")
    
    # Une os resultados ao DataFrame original
    df_results = pd.DataFrame(results)
    return pd.concat([df, df_results], axis=1).drop(columns=['cep_padronizado'])
# <<---------------------------------------------------->>

def to_excel_bytes(df: pd.DataFrame):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados_MultiAPI')
    return output.getvalue()


# --- INTERFACE GRÁFICA PRINCIPAL ---
st.set_page_config(page_title="Serviços CEP - Capital Consig", layout="wide")

with st.sidebar:
    st.image("logo.png", use_container_width=True)
    st.title("Capital Consig")
    st.info("Portal de Serviços de CEP.")

st.header("Portal de Serviços de CEP")
st.divider()

tab_individual, tab_lote = st.tabs(["🔍 Consulta Individual", "📦 Consulta em Lote (Multi-API)"])

with tab_individual: # ... (código da aba individual sem alterações)
    display_api_status_dashboard()
    st.subheader("Consulta Rápida por CEP")
    cep_input = st.text_input("Digite o CEP (apenas números):", max_chars=8)
    if cep_input and len(cep_input) == 8 and cep_input.isdigit():
        with st.spinner("Buscando em todas as fontes..."):
            resultados = consulta_cep_completa(cep_input)
        st.divider(); st.subheader("Resultados:"); cols = st.columns(len(resultados))
        for col, (api_name, result_data) in zip(cols, resultados.items()):
            with col: display_result_card(result_data['data'], api_name, result_data['status'])
    elif len(cep_input) > 0:
        st.warning("Por favor, digite um CEP válido com 8 dígitos.")

with tab_lote: # Aba de Lote agora usa o novo backend
    display_api_status_dashboard()
    st.subheader("Consulta de Múltiplos CEPs em Lote")
    st.warning("Atenção: Esta consulta é completa e pode levar mais tempo, pois verifica 3 fontes de dados para cada CEP.")
    
    uploaded_file = st.file_uploader("Selecione sua planilha para processamento", label_visibility="collapsed")
    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)
            cep_col = next((col for col in df.columns if 'cep' in str(col).lower()), None)
            
            if not cep_col:
                st.error("ERRO: Nenhuma coluna com 'CEP' no nome foi encontrada.")
            else:
                st.success(f"Arquivo '{uploaded_file.name}' carregado, com {len(df)} registros.")
                if st.button("Processar Lote Multi-API", use_container_width=True):
                    # CHAMA A NOVA FUNÇÃO DE PROCESSAMENTO
                    df_final = asyncio.run(processar_dataframe_multi_api(df, cep_col))
                    st.subheader("Processamento Concluído")
                    st.dataframe(df_final, use_container_width=True)
                    st.download_button("Baixar Resultados Completos", to_excel_bytes(df_final), f"{uploaded_file.name.split('.')[0]}_RESULTADO_MULTI_API.xlsx", use_container_width=True)
        except Exception as e:
            st.error(f"Erro ao processar o arquivo: {e}")
