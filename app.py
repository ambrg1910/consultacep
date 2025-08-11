# app.py (Versão 3.1 - Correção do NameError)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
# AQUI ESTÁ A CORREÇÃO CRUCIAL:
from typing import Optional, List, Dict 
from pathlib import Path
from datetime import datetime
import database as db

# --- CONFIGURAÇÕES E CONSTANTES ---
BATCH_SIZE = 50
PAUSE_BETWEEN_BATCHES = 5
CONCURRENCY_LIMIT = 8
REQUEST_TIMEOUT = 30
DATA_DIR = Path("data")

VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
AWESOMEAPI_URL = "https://cep.awesomeapi.com.br/json/{cep}"


# --- FUNÇÕES CORE ---
def find_column_by_keyword(df: pd.DataFrame, keyword: str) -> Optional[str]:
    for col in df.columns:
        if keyword.lower() in str(col).lower():
            return str(col)
    return None

async def fetch_all_apis_for_cep(original_row: dict, cep: str, session: httpx.AsyncClient) -> List[Dict]:
    if not cep or not cep.isdigit() or len(cep) != 8:
        error_row = original_row.copy(); error_row['STATUS'] = 'Formato de CEP Inválido'
        return [error_row]
    tasks = {"BRASILAPI": session.get(BRASILAPI_V2_URL.format(cep=cep)),"VIACEP": session.get(VIACEP_URL.format(cep=cep)),"AWESOMEAPI": session.get(AWESOMEAPI_URL.format(cep=cep))}
    responses = await asyncio.gather(*tasks.values(), return_exceptions=True)
    results_map = dict(zip(tasks.keys(), responses)); output_rows = []
    def process_response(api_name, response, parser_func):
        row = original_row.copy()
        if isinstance(response, httpx.Response) and response.status_code == 200:
            data = response.json(); 
            if 'erro' not in data or api_name != 'VIACEP':
                row.update(parser_func(data))
                row['STATUS'] = f'{api_name}: Sucesso'
            else:
                row['STATUS'] = f'{api_name}: Não encontrado'
        else: row['STATUS'] = f'{api_name}: Falha'
        output_rows.append(row)
    process_response('BRASILAPI', results_map['BRASILAPI'], lambda d: {'ENDEREÇO': d.get('street'), 'BAIRRO': d.get('neighborhood'), 'CIDADE': d.get('city'), 'ESTADO': d.get('state')})
    process_response('VIACEP', results_map['VIACEP'], lambda d: {'ENDEREÇO': d.get('logradouro'), 'BAIRRO': d.get('bairro'), 'CIDADE': d.get('localidade'), 'ESTADO': d.get('uf')})
    process_response('AWESOMEAPI', results_map['AWESOMEAPI'], lambda d: {'ENDEREÇO': d.get('address'), 'BAIRRO': d.get('district'), 'CIDADE': d.get('city'), 'ESTADO': d.get('state')})
    return output_rows

async def processar_job(job_id: int):
    job_info = db.get_job_by_id(job_id)
    if not job_info: st.error(f"Job {job_id} não encontrado."); return
    progress_placeholder = st.empty()
    try:
        db.update_job_status(job_id, 'PROCESSANDO'); df = pd.read_excel(job_info['saved_filepath'], dtype=str)
        df['cep_padronizado'] = df[job_info['cep_col']].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
        lista_de_lotes = [df.iloc[i:i + BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]
        total_propostas_processadas = 0
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as session:
            for batch_num, lote_df in enumerate(lista_de_lotes):
                progress_placeholder.progress(total_propostas_processadas / len(df), text=f"Job {job_id}: Processando Lote {batch_num + 1}/{len(lista_de_lotes)}...")
                
                async def fetch_with_semaphore(row):
                    async with semaphore:
                        return await fetch_all_apis_for_cep(row, row['cep_padronizado'], session)
                
                # Reformulando o 'row' a ser passado para incluir PROPOSTA e CEP original
                tasks = [fetch_with_semaphore({'PROPOSTA': row[job_info['prop_col']],'CEP': row[job_info['cep_col']], 'cep_padronizado': row['cep_padronizado']}) for index, row in lote_df.iterrows()]
                results_do_lote_nested = await asyncio.gather(*tasks, return_exceptions=True)

                db.save_results_to_db(job_id, [item for sublist in results_do_lote_nested if isinstance(sublist, list) for item in sublist])
                total_propostas_processadas += len(lote_df)
                db.update_job_status(job_id, 'PROCESSANDO', processed_ceps=total_propostas_processadas)
                
                if batch_num + 1 < len(lista_de_lotes): await asyncio.sleep(PAUSE_BETWEEN_BATCHES)
        
        db.update_job_status(job_id, 'CONCLUIDO')

    except Exception as e: 
        db.update_job_status(job_id, 'FALHOU'); 
        st.error(f"Erro no Job {job_id}: {e}")
    finally: 
        progress_placeholder.empty()

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False, sheet_name='Resultados_CEP')
    return output.getvalue()

def render_ui():
    st.set_page_config(page_title="Serviços CEP", layout="wide")
    with st.sidebar: st.image("logo.png", use_container_width=True); st.title("Portal de Validação")
    st.header("Processamento de CEP em Lote")
    with st.expander("1. Criar Novo Job de Processamento", expanded=True):
        uploaded_file = st.file_uploader("Selecione sua planilha (.xlsx ou .csv)")
        if uploaded_file:
            try:
                df = pd.read_excel(uploaded_file, dtype=str)
                cep_col = find_column_by_keyword(df, "cep"); prop_col = find_column_by_keyword(df, "proposta")
                if cep_col and prop_col:
                    st.success(f"Arquivo OK. CEP: '{cep_col}', Proposta: '{prop_col}'. {len(df)} registros.")
                    if st.button("➕ Adicionar Job à Fila", use_container_width=True):
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        saved_path = DATA_DIR / f"{timestamp}_{uploaded_file.name}"
                        with open(saved_path, "wb") as f: f.write(uploaded_file.getbuffer())
                        db.create_job(uploaded_file.name, str(saved_path), cep_col, prop_col, len(df))
                        st.rerun()
                else: st.error("Colunas 'CEP' e/ou 'Proposta' não encontradas.")
            except Exception as e: st.error(f"Erro ao ler planilha: {e}")
    st.subheader("2. Fila de Processamento")
    all_jobs = db.get_all_jobs()
    if not all_jobs: st.info("Nenhum job na fila.")
    else:
        for job in all_jobs:
            status_color = {'PENDENTE': 'blue', 'PROCESSANDO': 'orange', 'CONCLUIDO': 'green', 'FALHOU': 'red'}.get(job['status'], 'gray')
            with st.container(border=True):
                c1, c2, c3 = st.columns([2,1,1])
                c1.write(f"**Job #{job['id']}**: {job['original_filename']} ({job['processed_ceps']}/{job['total_ceps']})")
                c2.write(f"Status: :{status_color}[{job['status']}]")
                if job['status'] == 'PENDENTE':
                    if c3.button("▶️ Processar", key=f"run_{job['id']}", use_container_width=True):
                        asyncio.run(processar_job(job['id'])); st.rerun()
                elif job['status'] == 'CONCLUIDO':
                    c3.download_button("⬇️ Baixar", to_excel_bytes(db.get_job_results_as_df(job['id'])), f"RESULTADO_{job['id']}.xlsx", use_container_width=True)

def main():
    db.init_db() 
    render_ui() 

if __name__ == "__main__":
    main()