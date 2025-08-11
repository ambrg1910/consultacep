# app.py (Versão 2.1 - Cloud-Ready Deployment)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
import os
import time
from datetime import datetime, timezone
from pathlib import Path

# Importar nossa nova lógica de banco de dados
import database as db

# --- CONFIGURAÇÃO DA APLICAÇÃO ---
BATCH_SIZE = 50
PAUSE_BETWEEN_BATCHES = 5
CONCURRENCY_LIMIT = 8
REQUEST_TIMEOUT = 30
DATA_DIR = Path("data")  # CHANGED: Agora usamos Path sem criar a pasta aqui diretamente.

# --- DEFINIÇÃO DAS APIS ---
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
AWESOMEAPI_URL = "https://cep.awesomeapi.com.br/json/{cep}"


# --- LÓGICA DE INICIALIZAÇÃO SEGURA ---
@st.cache_resource
def initialize_app():
    """
    NEW: Função para inicializar recursos essenciais (DB e pastas) de forma segura e apenas uma vez.
    O decorator @st.cache_resource garante que isso só roda na primeira vez que a app é carregada.
    """
    DATA_DIR.mkdir(exist_ok=True)
    db.init_db()
    st.success("Aplicação inicializada e banco de dados pronto.")
    time.sleep(2) # Pausa para o usuário ver a mensagem de sucesso
    st.experimental_rerun() # Recarrega a página para remover a mensagem de sucesso e mostrar a UI principal

# O resto das suas funções permanece o mesmo (vou omitir por brevidade, use as que você já tem)
# find_column_by_keyword, fetch_all_apis_for_cep, processar_job, to_excel_bytes

# (COLE SUAS FUNÇÕES JÁ EXISTENTES AQUI, DA `find_column_by_keyword` ATÉ A `to_excel_bytes`)
def find_column_by_keyword(df: pd.DataFrame, keyword: str) -> Optional[str]: #...
    for col in df.columns:
        if keyword.lower() in str(col).lower(): return str(col)
    return None
async def fetch_all_apis_for_cep(original_row: dict, cep: str, session: httpx.AsyncClient) -> List[Dict]: #...
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
    else: row_br['STATUS'] = f'BRASILAPI: Falha ({type(res_br).__name__})'
    output_rows.append(row_br)
    # ViaCEP
    res_via = results_map.get("VIACEP"); row_via = original_row.copy()
    if isinstance(res_via, httpx.Response) and res_via.status_code == 200 and 'erro' not in res_via.text:
        data = res_via.json(); row_via.update({'ENDEREÇO': data.get('logradouro'), 'BAIRRO': data.get('bairro'), 'CIDADE': data.get('localidade'), 'ESTADO': data.get('uf'), 'STATUS': 'VIACEP: Sucesso'})
    else: row_via['STATUS'] = f'VIACEP: Falha ({type(res_via).__name__})'
    output_rows.append(row_via)
    # AwesomeAPI
    res_awe = results_map.get("AWESOMEAPI"); row_awe = original_row.copy()
    if isinstance(res_awe, httpx.Response) and res_awe.status_code == 200:
        data = res_awe.json(); row_awe.update({'ENDEREÇO': data.get('address'), 'BAIRRO': data.get('district'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state'), 'STATUS': 'AWESOMEAPI: Sucesso'})
    else: row_awe['STATUS'] = f'AWESOMEAPI: Falha ({type(res_awe).__name__})'
    output_rows.append(row_awe)
    return output_rows
async def processar_job(job_id: int, progress_bar_placeholder): #...
    job_info = db.get_job_by_id(job_id)
    if not job_info:
        st.error(f"Job com ID {job_id} não encontrado.")
        return
    try:
        db.update_job_status(job_id, 'PROCESSANDO')
        df = pd.read_excel(job_info['saved_filepath'], engine='openpyxl', dtype=str)
        df['cep_padronizado'] = df[job_info['cep_col']].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
        lista_de_lotes = [df.iloc[i:i + BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]
        total_propostas_processadas = 0
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as session:
            for batch_num, lote_df in enumerate(lista_de_lotes):
                text_progress = f"Job {job_id}: Processando Lote {batch_num + 1} de {len(lista_de_lotes)}..."
                progress_bar_placeholder.progress(total_propostas_processadas / len(df), text=text_progress)
                semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
                async def run_fetch(row):
                    async with semaphore:
                        base_row = {'PROPOSTA': row[job_info['prop_col']], 'CEP': row[job_info['cep_col']]}
                        return await fetch_all_apis_for_cep(base_row, row['cep_padronizado'], session)
                tasks = [run_fetch(row) for _, row in lote_df.iterrows()]
                results_do_lote_nested = await asyncio.gather(*tasks)
                final_results_lote = [item for sublist in results_do_lote_nested for item in sublist]
                db.save_results_to_db(job_id, final_results_lote)
                total_propostas_processadas += len(lote_df)
                db.update_job_status(job_id, 'PROCESSANDO', processed_ceps=total_propostas_processadas)
                if batch_num + 1 < len(lista_de_lotes):
                    for i in range(PAUSE_BETWEEN_BATCHES, 0, -1):
                        progress_bar_placeholder.progress(total_propostas_processadas / len(df), text=f"Pausa estratégica de {i}s...");
                        await asyncio.sleep(1)
        db.update_job_status(job_id, 'CONCLUIDO')
        progress_bar_placeholder.empty()
    except Exception as e:
        db.update_job_status(job_id, 'FALHOU')
        st.error(f"Ocorreu um erro crítico durante o processamento do Job {job_id}: {e}")
        st.rerun()
def to_excel_bytes(df: pd.DataFrame): #...
    output = BytesIO();
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados_MultiAPI')
    return output.getvalue()


# --- INTERFACE GRÁFICA ---
# NEW: Verifica se o DB já existe. Se não, mostra a tela de inicialização.
db_file = Path("data/jobs.db")
if not db_file.exists():
    initialize_app()
else:
    st.set_page_config(page_title="Serviços CEP - Capital Consig", layout="wide")
    with st.sidebar:
        st.image("logo.png", use_container_width=True)
        st.title("Capital Consig")
        st.info("Portal de Validação e Enriquecimento de Dados. Versão Cloud-Ready.")

    st.header("Portal de Serviços de CEP")
    st.divider()

    tab_lote, tab_individual = st.tabs(["**Processamento em Lote**", "Consulta Individual"])
    
    # (O código das abas (tab_lote, tab_individual) pode continuar exatamente o mesmo que você já tem)
    with tab_lote:
        # ... seu código da aba de lote aqui
        st.subheader("1. Criar um Novo Job de Processamento")
        uploaded_file = st.file_uploader("Selecione sua planilha (.xlsx ou .csv)")
        if uploaded_file:
            try:
                df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)
                st.dataframe(df.head(), use_container_width=True)
                cep_col = find_column_by_keyword(df, "cep")
                prop_col = find_column_by_keyword(df, "proposta")
                if not cep_col: st.error("Coluna contendo 'cep' não encontrada!")
                if not prop_col: st.error("Coluna contendo 'proposta' não encontrada!")
                if cep_col and prop_col:
                    st.success(f"Arquivo válido. Coluna de CEP: '{cep_col}'. Coluna de Proposta: '{prop_col}'. Total de {len(df)} registros.")
                    if st.button("➕ Criar e Adicionar Job à Fila"):
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        saved_path = DATA_DIR / f"{timestamp}_{uploaded_file.name}"
                        with open(saved_path, "wb") as f:
                            f.write(uploaded_file.getbuffer())
                        db.create_job(uploaded_file.name, str(saved_path), cep_col, prop_col, len(df))
                        st.success(f"Job para '{uploaded_file.name}' criado com sucesso! Veja na fila abaixo.")
                        st.rerun()
            except Exception as e:
                st.error(f"Erro ao ler a planilha: {e}")
        st.divider()
        st.subheader("2. Fila de Processamento")
        progress_bar_placeholder = st.empty()
        all_jobs = db.get_all_jobs()
        if not all_jobs:
            st.info("Nenhum job na fila. Crie um novo acima.")
        else:
            for job in all_jobs:
                status_color = {'PENDENTE': 'blue', 'PROCESSANDO': 'orange', 'CONCLUIDO': 'green', 'FALHOU': 'red'}.get(job['status'], 'gray')
                with st.container(border=True):
                    col1, col2, col3, col4 = st.columns([4, 2, 2, 2])
                    col1.write(f"**Job #{job['id']}**: {job['original_filename']}")
                    col2.write(f"Status: :{status_color}[{job['status']}]")
                    col2.write(f"Registros: {job['processed_ceps']} / {job['total_ceps']}")
                    if job['status'] == 'PENDENTE':
                        if col3.button("▶️ Processar", key=f"run_{job['id']}", use_container_width=True):
                            asyncio.run(processar_job(job['id'], progress_bar_placeholder))
                            st.success(f"Job {job['id']} concluído!")
                            st.rerun()
                    elif job['status'] == 'CONCLUIDO':
                        df_results = db.get_job_results_as_df(job['id'])
                        excel_bytes = to_excel_bytes(df_results)
                        col3.download_button(label="⬇️ Baixar Resultados (.xlsx)", data=excel_bytes, file_name=f"RESULTADO_JOB_{job['id']}.xlsx", key=f"download_{job['id']}", use_container_width=True)
                        with col4.expander("Ver amostra dos resultados"):
                            st.dataframe(df_results.head(10))
                    elif job['status'] == 'FALHOU':
                        col3.error("Job Falhou. Verifique os logs.")

    with tab_individual:
        st.subheader("Consulta Unitária por CEP")
        cep_input = st.text_input("Digite o CEP:", max_chars=8)
        if st.button("Consultar CEP Individualmente"):
            st.info("Funcionalidade de consulta individual a ser implementada ou mantida.")
