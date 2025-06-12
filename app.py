# app.py (Versão 16.0 - O Portal Final com Cadência de Produção Industrial)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
from typing import Optional, List, Dict
from cachetools import TTLCache
import time
from datetime import datetime, timezone
import requests

# --- CONFIGURAÇÃO DE PRODUÇÃO PARA ALTA ESCALA E RESILIÊNCIA ---
BATCH_SIZE = 100                # Manteremos lotes de 100 propostas
PAUSE_BETWEEN_BATCHES = 8       # Aumentamos a pausa para ser mais "gentil"
CONCURRENCY_LIMIT = 10          # Esta é a mudança mais crítica: muito menos requisições simultâneas
REQUEST_TIMEOUT = 30
# -------------------------------------------------------------------

# URLs das APIs
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
AWESOMEAPI_URL = "https://cep.awesomeapi.com.br/json/{cep}"

# --- FUNÇÕES (Sem alterações lógicas, apenas herdam a nova configuração) ---

def find_column_by_keyword(df: pd.DataFrame, keyword: str) -> Optional[str]: #...
    for col in df.columns:
        if keyword.lower() in str(col).lower(): return str(col)
    return None

@st.cache_data(ttl=60, show_spinner="Verificando status das APIs...")
def get_api_statuses(): #...
    statuses = {}; apis = {"BrasilAPI": BRASILAPI_V2_URL, "ViaCEP": VIACEP_URL, "AwesomeAPI": AWESOMEAPI_URL}
    for name, url in apis.items():
        try:
            r = requests.get(url.format(cep="01001000"), timeout=5);
            statuses[name] = {"status": "Online" if r.ok and "erro" not in r.text else "Com Erros", "latency": int(r.elapsed.total_seconds() * 1000)}
        except: statuses[name] = {"status": "Offline", "latency": -1}
    st.session_state.last_check_time = datetime.now(timezone.utc); return statuses

def display_api_status_dashboard(): #...
    st.caption("Status dos Serviços Externos"); statuses = get_api_statuses()
    cols = st.columns(len(statuses))
    for col, (name, data) in zip(cols, statuses.items()):
        with col:
            delta_color = "off" if data['status'] == 'Online' else "inverse"; st.metric(label=name, value=data['status'], delta=f"{data['latency']} ms" if data['latency'] >= 0 else "N/A", delta_color=delta_color, help="Tempo de Resposta.")
    if 'last_check_time' in st.session_state: st.caption(f"*Verificado há {int((datetime.now(timezone.utc) - st.session_state.last_check_time).total_seconds())} segundos.*")

@st.cache_data(ttl=3600, show_spinner="Consultando APIs...")
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
            st.text_area("Resultado",f"CEP:        {resultado.get('cep') or resultado.get('code', 'N/A')}\n"f"Endereço:   {resultado.get('street') or resultado.get('logradouro') or resultado.get('address', 'N/A')}\n"f"Bairro:     {resultado.get('neighborhood') or resultado.get('bairro') or resultado.get('district', 'N/A')}\n"f"Cidade/UF:  {resultado.get('city') or resultado.get('localidade', 'N/A')} / {resultado.get('state') or resultado.get('uf', 'N/A')}",
                height=130, disabled=True, label_visibility="collapsed", key=f"resultado_{api_name}")
        else: st.error(status)


async def fetch_all_apis_for_cep_resilient(original_row: dict, cep: str, session: httpx.AsyncClient) -> List[Dict]:
    if not cep or not cep.isdigit() or len(cep) != 8:
        error_row = original_row.copy(); error_row['STATUS'] = 'Formato de CEP Inválido'
        return [error_row]
    tasks = {
        "BRASILAPI": session.get(BRASILAPI_V2_URL.format(cep=cep)), "VIACEP": session.get(VIACEP_URL.format(cep=cep)), "AWESOMEAPI": session.get(AWESOMEAPI_URL.format(cep=cep))}
    responses = await asyncio.gather(*tasks.values(), return_exceptions=True)
    results_map = dict(zip(tasks.keys(), responses)); output_rows = []
    # BrasilAPI
    res_br = results_map.get("BRASILAPI"); row_br = original_row.copy()
    if isinstance(res_br, httpx.Response) and res_br.status_code == 200:
        data = res_br.json(); row_br.update({'ENDEREÇO': data.get('street'), 'BAIRRO': data.get('neighborhood'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state'), 'STATUS': 'BRASILAPI: Sucesso'})
    else: row_br['STATUS'] = 'BRASILAPI: Falha'
    output_rows.append(row_br)
    # ViaCEP
    res_via = results_map.get("VIACEP"); row_via = original_row.copy()
    if isinstance(res_via, httpx.Response) and res_via.status_code == 200 and 'erro' not in res_via.text:
        data = res_via.json(); row_via.update({'ENDEREÇO': data.get('logradouro'), 'BAIRRO': data.get('bairro'), 'CIDADE': data.get('localidade'), 'ESTADO': data.get('uf'), 'STATUS': 'VIACEP: Sucesso'})
    else: row_via['STATUS'] = 'VIACEP: Falha'
    output_rows.append(row_via)
    # AwesomeAPI
    res_awe = results_map.get("AWESOMEAPI"); row_awe = original_row.copy()
    if isinstance(res_awe, httpx.Response) and res_awe.status_code == 200:
        data = res_awe.json(); row_awe.update({'ENDEREÇO': data.get('address'), 'BAIRRO': data.get('district'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state'), 'STATUS': 'AWESOMEAPI: Sucesso'})
    else: row_awe['STATUS'] = 'AWESOMEAPI: Falha'
    output_rows.append(row_awe)
    return output_rows

async def processar_lote_industrial(df: pd.DataFrame, cep_col: str, prop_col: str) -> pd.DataFrame:
    df['cep_padronizado'] = df[cep_col].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
    lista_de_lotes = [df.iloc[i:i + BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]
    all_final_rows = []
    progress_bar = st.progress(0, text="Aguardando início do processamento...")
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as session:
        total_propostas_processadas = 0
        for batch_num, lote_df in enumerate(lista_de_lotes):
            text_progress = f"Processando Lote {batch_num + 1} de {len(lista_de_lotes)}..."
            progress_bar.progress(total_propostas_processadas / len(df), text=text_progress)
            
            semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
            
            async def run_fetch(row):
                async with semaphore: return await fetch_all_apis_for_cep_resilient(row.drop('cep_padronizado').to_dict(), row['cep_padronizado'], session)
            
            tasks = [run_fetch(row) for _, row in lote_df.iterrows()]
            results_do_lote = await asyncio.gather(*tasks)
            for cep_results in results_do_lote: all_final_rows.extend(cep_results)
            total_propostas_processadas += len(lote_df)

            if batch_num + 1 < len(lista_de_lotes):
                for i in range(PAUSE_BETWEEN_BATCHES, 0, -1):
                    progress_bar.progress(total_propostas_processadas / len(df), text=f"Pausa estratégica de {i}s antes do próximo lote..."); await asyncio.sleep(1)

    progress_bar.empty()
    final_df = pd.DataFrame(all_final_rows)
    final_df.rename(columns={prop_col: 'PROPOSTA', cep_col: 'CEP'}, inplace=True)
    cols_finais = ['PROPOSTA', 'CEP', 'ENDEREÇO', 'BAIRRO', 'CIDADE', 'ESTADO', 'STATUS']
    return final_df[[c for c in cols_finais if c in final_df.columns]]

def to_excel_bytes(df: pd.DataFrame):
    output = BytesIO();
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados_MultiAPI')
    return output.getvalue()


# --- INTERFACE GRÁFICA PROFISSIONAL ---
st.set_page_config(page_title="Serviços CEP - Capital Consig", layout="wide")
st.markdown("<style> button[title='Fullscreen'] {display: none;} </style>", unsafe_allow_html=True)
with st.sidebar:
    st.image("logo.png", use_container_width=True); st.title("Capital Consig"); st.info("Portal de Validação e Enriquecimento de Dados.")
st.header("Portal de Serviços de CEP"); st.divider()
tab_individual, tab_lote = st.tabs(["Consulta Individual", "Consulta em Lote"])

with tab_individual:
    display_api_status_dashboard(); st.divider(); st.subheader("Consulta Unitária por CEP")
    col1, col2 = st.columns([3, 1]); cep_input = col1.text_input("Digite o CEP:", max_chars=8, label_visibility="collapsed", placeholder="Digite o CEP (apenas números)")
    if col2.button("Consultar CEP"):
        if cep_input and len(cep_input) == 8 and cep_input.isdigit():
            resultados = consulta_cep_completa(cep_input); st.subheader("Resultados:"); cols = st.columns(len(resultados))
            for col, (api_name, result_data) in zip(cols, resultados.items()):
                with col: display_result_card(result_data['data'], api_name, result_data['status'])
        else: st.warning("Digite um CEP válido com 8 dígitos.")

with tab_lote:
    display_api_status_dashboard(); st.divider(); st.subheader("Processamento de Planilha em Lote")
    st.warning("Atenção: Esta funcionalidade é projetada para grandes volumes. O processamento será mais lento, mas mais confiável. Não feche esta aba durante a execução.", icon="⏳")
    
    uploaded_file = st.file_uploader("Selecione sua planilha (.xlsx ou .csv)", label_visibility="collapsed")
    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)
            cep_col, prop_col = find_column_by_keyword(df, "cep"), find_column_by_keyword(df, "proposta")
            if not cep_col or not prop_col:
                if not cep_col: st.error("Coluna 'CEP' não encontrada.");
                if not prop_col: st.error("Coluna 'PROPOSTA' não encontrada.")
            else:
                st.success(f"Arquivo '{uploaded_file.name}' carregado e colunas identificadas com sucesso.")
                if st.button("Iniciar Processamento Industrial", use_container_width=True):
                    df_final = asyncio.run(processar_lote_industrial(df, cep_col, prop_col))
                    st.subheader("Processamento Concluído")
                    st.dataframe(df_final, use_container_width=True)
                    st.download_button("Baixar Resultados (.xlsx)", to_excel_bytes(df_final), f"{uploaded_file.name.split('.')[0]}_RESULTADO_FINAL.xlsx", use_container_width=True)
        except Exception as e:
            st.error(f"Erro Crítico: {e}")
