# app.py (Versão 7.0 - Industrial Queue Engine)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime
import database as db
import time

# --- CONFIGURAÇÕES DE PERFORMANCE ---
CONCURRENCY_LIMIT = 75
REQUEST_TIMEOUT = 20
DATA_DIR = Path("data")
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
AWESOMEAPI_URL = "https://cep.awesomeapi.com.br/json/{cep}"

# --- FUNÇÕES CORE (Motor de Alta Performance) ---
def find_column_by_keyword(df: pd.DataFrame, keyword: str) -> Optional[str]:
    for col in df.columns:
        if keyword.lower() in str(col).lower(): return str(col)
    return None

def parse_brasilapi(data: Dict[str, Any]) -> Dict[str, Any]:
    return {'ENDEREÇO': data.get('street'), 'BAIRRO': data.get('neighborhood'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state')}

def parse_viacep(data: Dict[str, Any]) -> Dict[str, Any]:
    return {'ENDEREÇO': data.get('logradouro'), 'BAIRRO': data.get('bairro'), 'CIDADE': data.get('localidade'), 'ESTADO': data.get('uf')}

def parse_awesomeapi(data: Dict[str, Any]) -> Dict[str, Any]:
    return {'ENDEREÇO': data.get('address'), 'BAIRRO': data.get('district'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state')}

async def processar_job_completo(job_info: Dict[str, Any], progress_placeholder):
    try:
        df = pd.read_excel(job_info['saved_filepath'], dtype=str)
        df['cep_padronizado'] = df[job_info['cep_col']].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
        cep_cache = {}
        ceps_para_processar = df[['cep_padronizado', job_info['prop_col'], job_info['cep_col']]].drop_duplicates(subset=['cep_padronizado'])
        
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as session:
            semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
            async def fetch_brasilapi(cep, prop_info):
                async with semaphore:
                    if cep in cep_cache: return None
                    try:
                        resp = await session.get(BRASILAPI_V2_URL.format(cep=cep))
                        if resp.status_code == 200: cep_cache[cep] = {**prop_info, **parse_brasilapi(resp.json()), 'STATUS': 'BRASILAPI: Sucesso'}
                        else: return (cep, prop_info)
                    except Exception: return (cep, prop_info)
                    return None
            
            tasks_brasilapi = [fetch_brasilapi(row['cep_padronizado'], {'PROPOSTA': row[job_info['prop_col']], 'CEP': row[job_info['cep_col']]}) for _, row in ceps_para_processar.iterrows()]
            progress_placeholder.progress(0.1, text=f"Job #{job_info['id']}: 1/2 - Consultando {len(tasks_brasilapi)} CEPs na BrasilAPI...")
            ceps_falharam_brasilapi = [r for r in await asyncio.gather(*tasks_brasilapi) if r is not None]

            if ceps_falharam_brasilapi:
                async def fetch_fallbacks(cep, prop_info):
                    async with semaphore:
                        if cep in cep_cache: return
                        try:
                            tasks = {'viacep': session.get(VIACEP_URL.format(cep=cep)), 'awesome': session.get(AWESOMEAPI_URL.format(cep=cep))}
                            res = await asyncio.gather(*tasks.values())
                            if res[0].status_code == 200 and 'erro' not in res[0].text: cep_cache[cep] = {**prop_info, **parse_viacep(res[0].json()), 'STATUS': 'VIACEP: Sucesso'}
                            elif res[1].status_code == 200: cep_cache[cep] = {**prop_info, **parse_awesomeapi(res[1].json()), 'STATUS': 'AWESOMEAPI: Sucesso'}
                            else: cep_cache[cep] = {**prop_info, 'STATUS': 'FALHA TOTAL'}
                        except Exception: cep_cache[cep] = {**prop_info, 'STATUS': 'ERRO CRÍTICO'}
                
                progress_placeholder.progress(0.8, text=f"Job #{job_info['id']}: 2/2 - Consultando {len(ceps_falharam_brasilapi)} CEPs nas APIs de fallback...")
                await asyncio.gather(*[fetch_fallbacks(cep, p_info) for cep, p_info in ceps_falharam_brasilapi])
        
        final_results = [cep_cache.get(row['cep_padronizado'], {'PROPOSTA': row[job_info['prop_col']], 'CEP': row[job_info['cep_col']], 'STATUS': 'CEP Inválido'}) for _, row in df.iterrows()]
        db.save_results_to_db(job_info['id'], final_results)
        db.update_job_status(job_info['id'], 'CONCLUIDO', processed_ceps=len(df))
    except Exception as e:
        db.update_job_status(job_info['id'], 'FALHOU')
        st.error(f"Erro Crítico no Job {job_info['id']}: {e}")

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO();
    with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False, sheet_name='Resultados_CEP')
    return output.getvalue()

# --- MOTOR DA INTERFACE E GERENCIADOR DE ESTADO ---
def main():
    st.set_page_config(page_title="Capital Consig - Validador de CEP", layout="wide")
    db.init_db()

    # --- Sidebar ---
    with st.sidebar:
        st.image("logo.png", use_container_width=True); st.title("Portal de Validação")
        st.info("Industrial Queue Engine v7.0")

    # --- Corpo Principal ---
    st.header("Processamento de CEP em Lote")
    
    # Verifica se já existe um job "PROCESSANDO". Isso define o estado global.
    is_worker_active = any(job['status'] == 'PROCESSANDO' for job in db.get_all_jobs())

    with st.expander("1. Adicionar Novo Job à Fila", expanded=True):
        uploaded_file = st.file_uploader("Selecione sua planilha (.xlsx)", disabled=is_worker_active)
        if uploaded_file:
            try:
                df = pd.read_excel(uploaded_file, dtype=str)
                cep_col = find_column_by_keyword(df, "cep"); prop_col = find_column_by_keyword(df, "proposta")
                if cep_col and prop_col:
                    st.success(f"Arquivo OK. Total de {len(df)} registros.")
                    if st.button("➕ Adicionar à Fila de Processamento", use_container_width=True):
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        saved_path = DATA_DIR / f"{timestamp}_{uploaded_file.name}"
                        with open(saved_path, "wb") as f: f.write(uploaded_file.getbuffer())
                        db.create_job(uploaded_file.name, str(saved_path), cep_col, prop_col, len(df))
                        st.rerun()
                else: st.error("Colunas 'CEP' e/ou 'Proposta' não encontradas.")
            except Exception as e: st.error(f"Erro ao ler planilha: {e}")

    st.subheader("2. Fila de Processamento Global")
    
    progress_placeholder = st.empty()
    
    # Botão para INICIAR o processamento da fila, se nada estiver rodando
    next_job = db.get_next_pending_job()
    if not is_worker_active and next_job:
        if st.button(f"▶️ Iniciar Processamento da Fila (Próximo: Job #{next_job['id']})", use_container_width=True):
            st.session_state.run_worker = True # Aciona o trabalhador
            st.rerun()
    elif is_worker_active:
        st.info("Um trabalhador já está processando a fila. A tela será atualizada automaticamente.")

    # Exibição da lista de jobs
    all_jobs = db.get_all_jobs()
    if not all_jobs: st.info("Nenhum job na fila.")
    else:
        for job in all_jobs:
            status_color = {'PENDENTE': 'blue', 'PROCESSANDO': 'orange', 'CONCLUIDO': 'green', 'FALHOU': 'red'}.get(job['status'], 'gray')
            with st.container(border=True):
                c1, c2, c3 = st.columns([3, 1.5, 1])
                c1.write(f"**Job #{job['id']}**: {job['original_filename']}")
                c1.write(f"Registros: {job['processed_ceps']}/{job['total_ceps']}")
                c2.write(f"Status: **:{status_color}[{job['status']}]**")
                if job['status'] == 'CONCLUIDO':
                    c3.download_button("⬇️ Exportar", to_excel_bytes(db.get_job_results_as_df(job['id'])), f"RESULTADO_JOB_{job['id']}.xlsx", use_container_width=True)

    # --- LÓGICA DO TRABALHADOR (Worker) ---
    if st.session_state.get('run_worker', False):
        st.session_state.run_worker = False # Reseta o gatilho
        
        job_a_processar = db.get_next_pending_job()
        if job_a_processar:
            # Marca o job como PROCESSANDO para bloquear a UI para todos os outros usuários
            db.update_job_status(job_a_processar['id'], 'PROCESSANDO')
            st.rerun() # Atualiza a UI para refletir o bloqueio

    # A sessão que está com o status "PROCESSANDO" se torna o worker
    job_em_processamento = next((job for job in all_jobs if job['status'] == 'PROCESSANDO'), None)
    if job_em_processamento:
        asyncio.run(processar_job_completo(job_em_processamento, progress_placeholder))
        st.rerun() # Ao terminar um job, recarrega para pegar o próximo ou liberar a fila

if __name__ == "__main__":
    main()