# app.py (Versão 10.0 - O Portal Final com o Formato de Saída em Linhas)
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
CONCURRENCY_LIMIT = 40; MAX_RETRIES = 3; REQUEST_TIMEOUT = 15
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
AWESOMEAPI_URL = "https://cep.awesomeapi.com.br/json/{cep}"
# -----------------------------

# --- FUNÇÕES DE BACKEND ---

# (As funções de Status e Consulta Individual não precisam de alterações)
@st.cache_data(ttl=60, show_spinner=False)
def get_api_statuses(): #...
    statuses = {}; apis = {"BrasilAPI": BRASILAPI_V2_URL, "ViaCEP": VIACEP_URL, "AwesomeAPI": AWESOMEAPI_URL}
    for name, url in apis.items():
        try:
            start_time = time.monotonic(); r = requests.get(url.format(cep="01001000"), timeout=5); end_time = time.monotonic()
            latency = int((end_time - start_time) * 1000)
            is_ok = r.ok and ("erro" not in r.text)
            statuses[name] = {"status": "Online" if is_ok else "Com Erros", "latency": latency}
        except: statuses[name] = {"status": "Offline", "latency": -1}
    st.session_state.last_check_time = datetime.now(timezone.utc); return statuses
def display_api_status_dashboard(): #...
    statuses = get_api_statuses()
    if 'last_check_time' in st.session_state:
        time_diff = (datetime.now(timezone.utc) - st.session_state.last_check_time).total_seconds()
        st.caption(f"Status dos serviços (verificado há {int(time_diff)} segundos)")
    cols = st.columns(len(statuses))
    for col, (name, data) in zip(cols, statuses.items()):
        with col:
            with st.container(border=True):
                icon = "✅" if data['status'] == "Online" else "❌"
                st.markdown(f"**{name}** {icon}"); st.metric("Resposta", f"{data['latency']} ms" if data['latency'] >= 0 else "N/A")
@st.cache_data(ttl=3600)
def consulta_cep_completa(cep): #...
    results = {}
    try: r = requests.get(BRASILAPI_V2_URL.format(cep=cep), timeout=5); results['BrasilAPI'] = {"data": r.json(), "status": "Sucesso"} if r.ok else {"data": None, "status": "Não encontrado"}
    except: results['BrasilAPI'] = {"data": None, "status": "Serviço indisponível"}
    try: r = requests.get(VIACEP_URL.format(cep=cep), timeout=5); results['ViaCEP'] = {"data": r.json(), "status": "Sucesso"} if r.ok and 'erro' not in r.text else {"data": None, "status": "Não encontrado"}
    except: results['ViaCEP'] = {"data": None, "status": "Serviço indisponível"}
    try: r = requests.get(AWESOMEAPI_URL.format(cep=cep), timeout=5); results['AwesomeAPI'] = {"data": r.json(), "status": "Sucesso"} if r.ok else {"data": None, "status": "Não encontrado"}
    except: results['AwesomeAPI'] = {"data": None, "status": "Serviço indisponível"}
    return results
def display_result_card(resultado, api_name, status): #...
    with st.container(border=True):
        st.subheader(api_name, anchor=False)
        if status == "Sucesso":
            st.text(f"CEP: {resultado.get('cep') or resultado.get('code', 'N/A')}\nEndereço: {resultado.get('street') or resultado.get('logradouro') or resultado.get('address', 'N/A')}\nBairro: {resultado.get('neighborhood') or resultado.get('bairro') or resultado.get('district', 'N/A')}\nCidade/UF: {resultado.get('city') or resultado.get('localidade', 'N/A')} / {resultado.get('state') or resultado.get('uf', 'N/A')}")
        else: st.error(status)


# <<--- O NOVO CORAÇÃO DA CONSULTA EM LOTE ---
async def fetch_and_format_lote(original_row: dict, cep: str, session: httpx.AsyncClient) -> list[dict]:
    """Consulta um CEP em todas as APIs e formata a saída em múltiplas linhas, como solicitado."""
    if not cep or not cep.isdigit() or len(cep) != 8:
        # Retorna uma linha de erro para este CEP se o formato for inválido
        error_row = original_row.copy()
        error_row['STATUS'] = 'Formato de CEP Inválido'
        return [error_row]

    tasks = [
        session.get(BRASILAPI_V2_URL.format(cep=cep)),
        session.get(VIACEP_URL.format(cep=cep)),
        session.get(AWESOMEAPI_URL.format(cep=cep))
    ]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    
    output_rows = []

    # Processa BrasilAPI
    new_row_br = original_row.copy()
    if not isinstance(responses[0], Exception) and responses[0].status_code == 200:
        data = responses[0].json()
        new_row_br['ENDEREÇO'] = data.get('street')
        new_row_br['BAIRRO'] = data.get('neighborhood')
        new_row_br['CIDADE'] = data.get('city')
        new_row_br['ESTADO'] = data.get('state')
        new_row_br['STATUS'] = 'BRASILAPI: Sucesso'
    else:
        new_row_br['STATUS'] = 'BRASILAPI: Falha'
    output_rows.append(new_row_br)

    # Processa ViaCEP
    new_row_via = original_row.copy()
    if not isinstance(responses[1], Exception) and responses[1].status_code == 200 and 'erro' not in responses[1].text:
        data = responses[1].json()
        new_row_via['ENDEREÇO'] = data.get('logradouro')
        new_row_via['BAIRRO'] = data.get('bairro')
        new_row_via['CIDADE'] = data.get('localidade')
        new_row_via['ESTADO'] = data.get('uf')
        new_row_via['STATUS'] = 'VIACEP: Sucesso'
    else:
        new_row_via['STATUS'] = 'VIACEP: Falha'
    output_rows.append(new_row_via)
    
    # Processa AwesomeAPI
    new_row_awe = original_row.copy()
    if not isinstance(responses[2], Exception) and responses[2].status_code == 200:
        data = responses[2].json()
        new_row_awe['ENDEREÇO'] = data.get('address')
        new_row_awe['BAIRRO'] = data.get('district')
        new_row_awe['CIDADE'] = data.get('city')
        new_row_awe['ESTADO'] = data.get('state')
        new_row_awe['STATUS'] = 'AWESOMEAPI: Sucesso'
    else:
        new_row_awe['STATUS'] = 'AWESOMEAPI: Falha'
    output_rows.append(new_row_awe)
        
    return output_rows

async def processar_dataframe_em_linhas(df: pd.DataFrame, cep_column_name: str) -> pd.DataFrame:
    df['cep_padronizado'] = df[cep_column_name].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async def run_fetch(row, session):
        cep_p = row['cep_padronizado']
        original_data = row.drop('cep_padronizado').to_dict()
        async with semaphore:
            return await fetch_and_format_lote(original_data, cep_p, session)

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as session:
        # Cria tarefas para cada LINHA da planilha de entrada
        tasks = [run_fetch(row, session) for index, row in df.iterrows()]
        
        all_new_rows = []
        placeholder = st.empty()
        # Coleta os resultados (que são listas de linhas)
        for i, f in enumerate(asyncio.as_completed(tasks)):
            list_of_rows_for_cep = await f
            all_new_rows.extend(list_of_rows_for_cep)
            placeholder.text(f"Progresso: {i + 1} de {len(tasks)} propostas consultadas...")
        placeholder.success("Processamento concluído!")

    # Cria o DataFrame final a partir da lista achatada de todas as novas linhas
    final_df = pd.DataFrame(all_new_rows)
    # Garante a ordem das colunas como no exemplo
    cols = df.columns.drop(cep_column_name).tolist()
    final_cols = ['PROPOSTA', 'CEP', 'ENDEREÇO', 'BAIRRO', 'CIDADE', 'ESTADO', 'STATUS']
    # Reordena o dataframe final, mantendo apenas as colunas desejadas.
    return final_df[final_cols]
# <<---------------------------------------------------->>

def to_excel_bytes(df: pd.DataFrame): #...
    output = BytesIO();
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados_MultiAPI')
    return output.getvalue()


# --- INTERFACE GRÁFICA PRINCIPAL ---
st.set_page_config(page_title="Serviços CEP - Capital Consig", layout="wide")
with st.sidebar: st.image("logo.png", use_container_width=True); st.title("Capital Consig"); st.info("Portal de Serviços de CEP.")
st.header("Portal de Serviços de CEP"); st.divider()
tab_individual, tab_lote = st.tabs(["🔍 Consulta Individual", "📦 Consulta em Lote (Multi-API)"])

with tab_individual: # ... (código da aba individual sem alterações)
    display_api_status_dashboard()
    st.subheader("Consulta Rápida por CEP")
    cep_input = st.text_input("Digite o CEP (apenas números):", max_chars=8)
    if cep_input and len(cep_input) == 8 and cep_input.isdigit():
        with st.spinner("Buscando em todas as fontes..."): resultados = consulta_cep_completa(cep_input)
        st.divider(); st.subheader("Resultados:"); cols = st.columns(len(resultados))
        for col, (api_name, result_data) in zip(cols, resultados.items()):
            with col: display_result_card(result_data['data'], api_name, result_data['status'])
    elif len(cep_input) > 0: st.warning("Por favor, digite um CEP válido com 8 dígitos.")

with tab_lote: # Aba de Lote agora usa o novo backend em formato de linhas
    display_api_status_dashboard()
    st.subheader("Consulta de Múltiplos CEPs em Lote")
    st.info("Esta consulta verifica 3 fontes de dados para cada CEP e retorna o resultado em múltiplas linhas, como solicitado.")
    uploaded_file = st.file_uploader("Selecione sua planilha para processamento", label_visibility="collapsed")
    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)
            cep_col = next((col for col in df.columns if 'cep' in str(col).lower()), None)
            if not cep_col: st.error("ERRO: Nenhuma coluna com 'CEP' no nome foi encontrada.")
            else:
                st.success(f"Arquivo '{uploaded_file.name}' carregado, com {len(df)} propostas.")
                if st.button("Processar Lote no Formato de Linhas", use_container_width=True):
                    # CHAMA A NOVA FUNÇÃO DE PROCESSAMENTO
                    df_final = asyncio.run(processar_dataframe_em_linhas(df, cep_col))
                    st.subheader("Processamento Concluído")
                    st.dataframe(df_final, use_container_width=True)
                    st.download_button("Baixar Resultados em Linhas", to_excel_bytes(df_final), f"{uploaded_file.name.split('.')[0]}_RESULTADO_LINHAS.xlsx", use_container_width=True)
        except Exception as e: st.error(f"Erro ao processar o arquivo: {e}")
