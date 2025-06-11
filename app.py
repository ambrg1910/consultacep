# app.py (Versão 12.0 - A Versão Final com Experiência de Usuário Polida)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
from cachetools import TTLCache
import requests
import time
from datetime import datetime, timezone
from typing import Optional

# --- CONFIGURAÇÃO GLOBAL (sem alterações) ---
CONCURRENCY_LIMIT = 40; MAX_RETRIES = 3; REQUEST_TIMEOUT = 15
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
AWESOMEAPI_URL = "https://cep.awesomeapi.com.br/json/{cep}"
# -----------------------------------------------

# --- FUNÇÕES DE BACKEND (sem alterações lógicas, apenas o que é essencial para UI) ---
def find_column_by_keyword(df: pd.DataFrame, keyword: str) -> Optional[str]:
    for col in df.columns:
        if keyword.lower() in str(col).lower(): return str(col)
    return None

@st.cache_data(ttl=60, show_spinner="Verificando status das APIs...")
def get_api_statuses():
    statuses = {}; apis = {"BrasilAPI": BRASILAPI_V2_URL, "ViaCEP": VIACEP_URL, "AwesomeAPI": AWESOMEAPI_URL}
    for name, url in apis.items():
        try:
            r = requests.get(url.format(cep="01001000"), timeout=5)
            statuses[name] = {"status": "Online" if r.ok and "erro" not in r.text else "Com Erros", "latency": int(r.elapsed.total_seconds() * 1000)}
        except: statuses[name] = {"status": "Offline", "latency": -1}
    st.session_state.last_check_time = datetime.now(timezone.utc); return statuses

def display_api_status_dashboard():
    st.caption("Status dos Serviços de Consulta")
    statuses = get_api_statuses()
    cols = st.columns(len(statuses))
    for col, (name, data) in zip(cols, statuses.items()):
        with col:
            with st.container(border=True):
                icon = "✅" if data['status'] == "Online" else "❌"
                st.markdown(f"**{name}:** {icon} {data['status']}")
                st.metric("Resposta", f"{data['latency']} ms" if data['latency'] >= 0 else "N/A", label_visibility="collapsed")
    if 'last_check_time' in st.session_state:
        time_diff = (datetime.now(timezone.utc) - st.session_state.last_check_time).total_seconds()
        st.caption(f"*Verificado há {int(time_diff)} segundos.*")

@st.cache_data(ttl=3600, show_spinner="Consultando APIs...")
def consulta_cep_completa(cep):
    results = {}
    try: r = requests.get(BRASILAPI_V2_URL.format(cep=cep), timeout=5); results['BrasilAPI'] = {"data": r.json(), "status": "Sucesso"} if r.ok else {"data": None, "status": "Não encontrado"}
    except: results['BrasilAPI'] = {"data": None, "status": "Serviço indisponível"}
    try: r = requests.get(VIACEP_URL.format(cep=cep), timeout=5); results['ViaCEP'] = {"data": r.json(), "status": "Sucesso"} if r.ok and 'erro' not in r.text else {"data": None, "status": "Não encontrado"}
    except: results['ViaCEP'] = {"data": None, "status": "Serviço indisponível"}
    try: r = requests.get(AWESOMEAPI_URL.format(cep=cep), timeout=5); results['AwesomeAPI'] = {"data": r.json(), "status": "Sucesso"} if r.ok else {"data": None, "status": "Não encontrado"}
    except: results['AwesomeAPI'] = {"data": None, "status": "Serviço indisponível"}
    return results

def display_result_card(resultado, api_name, status):
    with st.container(border=True):
        st.subheader(api_name, anchor=False)
        if status == "Sucesso":
            st.text(f"CEP: {resultado.get('cep') or resultado.get('code', 'N/A')}\nEndereço: {resultado.get('street') or resultado.get('logradouro') or resultado.get('address', 'N/A')}\nBairro: {resultado.get('neighborhood') or resultado.get('bairro') or resultado.get('district', 'N/A')}\nCidade/UF: {resultado.get('city') or resultado.get('localidade', 'N/A')} / {resultado.get('state') or resultado.get('uf', 'N/A')}")
        else: st.error(status)

async def fetch_and_format_lote(original_row, cep, session): # (código idêntico)
    if not cep or not cep.isdigit() or len(cep) != 8:
        error_row = original_row.copy(); error_row['STATUS'] = 'Formato de CEP Inválido'
        return [error_row]
    tasks = [session.get(BRASILAPI_V2_URL.format(cep=cep)), session.get(VIACEP_URL.format(cep=cep)), session.get(AWESOMEAPI_URL.format(cep=cep))]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    output_rows = []
    # Processa BrasilAPI
    new_row_br = original_row.copy()
    if not isinstance(responses[0], Exception) and responses[0].status_code == 200: data = responses[0].json(); new_row_br.update({'ENDEREÇO': data.get('street'), 'BAIRRO': data.get('neighborhood'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state'), 'STATUS': 'BRASILAPI: Sucesso'})
    else: new_row_br['STATUS'] = 'BRASILAPI: Falha'
    output_rows.append(new_row_br)
    # Processa ViaCEP
    new_row_via = original_row.copy()
    if not isinstance(responses[1], Exception) and responses[1].status_code == 200 and 'erro' not in responses[1].text: data = responses[1].json(); new_row_via.update({'ENDEREÇO': data.get('logradouro'), 'BAIRRO': data.get('bairro'), 'CIDADE': data.get('localidade'), 'ESTADO': data.get('uf'), 'STATUS': 'VIACEP: Sucesso'})
    else: new_row_via['STATUS'] = 'VIACEP: Falha'
    output_rows.append(new_row_via)
    # Processa AwesomeAPI
    new_row_awe = original_row.copy()
    if not isinstance(responses[2], Exception) and responses[2].status_code == 200: data = responses[2].json(); new_row_awe.update({'ENDEREÇO': data.get('address'), 'BAIRRO': data.get('district'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state'), 'STATUS': 'AWESOMEAPI: Sucesso'})
    else: new_row_awe['STATUS'] = 'AWESOMEAPI: Falha'
    output_rows.append(new_row_awe)
    return output_rows

async def processar_dataframe_em_linhas(df, cep_col, prop_col):#...
    df['cep_padronizado'] = df[cep_col].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async def run_fetch(row, session):
        cep_p = row['cep_padronizado']; original_data = row.drop('cep_padronizado').to_dict()
        async with semaphore: return await fetch_and_format_lote(original_data, cep_p, session)
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as session:
        tasks = [run_fetch(row, session) for _, row in df.iterrows()]
        all_new_rows = []; placeholder = st.empty()
        for i, f in enumerate(asyncio.as_completed(tasks)):
            list_of_rows = await f; all_new_rows.extend(list_of_rows)
            placeholder.text(f"Progresso: {i + 1} de {len(tasks)} propostas consultadas...")
        placeholder.success("Processamento concluído!")
    final_df = pd.DataFrame(all_new_rows); final_cols = [prop_col, cep_col, 'ENDEREÇO', 'BAIRRO', 'CIDADE', 'ESTADO', 'STATUS']
    existing_cols = [c for c in final_cols if c in final_df.columns]
    return final_df[existing_cols]

def to_excel_bytes(df): # (código idêntico)
    output = BytesIO();
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados_MultiAPI')
    return output.getvalue()


# --- INTERFACE GRÁFICA PRINCIPAL ---
st.set_page_config(page_title="Serviços CEP - Capital Consig", layout="wide")

# <<--- MELHORIA 1: INJEÇÃO DE CSS PARA REMOVER O BOTÃO "FULLSCREEN" ---
st.markdown("<style> button[title='Fullscreen'] {display: none;} </style>", unsafe_allow_html=True)

with st.sidebar:
    st.image("logo.png", use_container_width=True)
    st.title("Capital Consig")
    st.info("Utilize as abas para selecionar a funcionalidade desejada.") # Texto refinado

st.header("Portal de Serviços de CEP")
st.divider()

tab_individual, tab_lote = st.tabs(["🔍 Consulta Individual", "📦 Consulta em Lote (Multi-API)"])

with tab_individual:
    display_api_status_dashboard()
    st.divider()
    
    st.subheader("Consulta Rápida por CEP")
    cep_input = st.text_input("Digite o CEP (apenas números):", max_chars=8)
    
    # <<--- MELHORIA 2: BOTÃO DEDICADO PARA INICIAR A CONSULTA ---
    if st.button("Consultar CEP", use_container_width=True):
        if cep_input and len(cep_input) == 8 and cep_input.isdigit():
            # A função de consulta agora tem o spinner dentro dela (via cache)
            resultados = consulta_cep_completa(cep_input)
            
            st.divider()
            st.subheader("Resultados:")
            
            # Mostra os resultados nos novos cards profissionais
            cols = st.columns(len(resultados))
            for col, (api_name, result_data) in zip(cols, resultados.items()):
                with col:
                    display_result_card(result_data['data'], api_name, result_data['status'])
        else:
            st.warning("Por favor, digite um CEP válido com 8 dígitos para iniciar a consulta.")

with tab_lote:
    display_api_status_dashboard()
    st.divider()

    st.subheader("Consulta de Múltiplos CEPs em Lote")
    st.info("Esta consulta verifica 3 fontes de dados para cada CEP e retorna o resultado em múltiplas linhas, como solicitado.")
    
    uploaded_file = st.file_uploader("Selecione sua planilha para processamento", label_visibility="collapsed")
    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)
            cep_col = find_column_by_keyword(df, "cep")
            prop_col = find_column_by_keyword(df, "proposta")

            if not cep_col or not prop_col:
                if not cep_col: st.error("ERRO: Nenhuma coluna com 'CEP' foi encontrada na planilha.")
                if not prop_col: st.error("ERRO: Nenhuma coluna com 'PROPOSTA' foi encontrada na planilha.")
            else:
                st.success(f"Arquivo '{uploaded_file.name}' carregado. Colunas encontradas: '{prop_col}' e '{cep_col}'.")
                st.info(f"{len(df)} registros prontos para processar.")
                if st.button("Processar Lote no Formato de Linhas", use_container_width=True):
                    df_final = asyncio.run(processar_dataframe_em_linhas(df, cep_col, prop_col))
                    st.subheader("Processamento Concluído")
                    st.dataframe(df_final, use_container_width=True)
                    st.download_button("Baixar Resultados em Linhas", to_excel_bytes(df_final), f"{uploaded_file.name.split('.')[0]}_RESULTADO_FINAL.xlsx", use_container_width=True)
        except Exception as e:
            st.error(f"Ocorreu um erro crítico ao processar o arquivo: {e}")
