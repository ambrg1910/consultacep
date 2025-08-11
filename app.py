# app.py (Versão 10.0 - Production-Grade Stability)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime, timedelta
import database as db
import time

# --- CONFIGURAÇÕES DE PERFORMANCE RESILIENTE ---
BATCH_SIZE = 200
CONCURRENCY_LIMIT = 20
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 1
DATA_DIR = Path("data")
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"

# --- FUNÇÕES CORE (Motor de Processamento) ---
def find_column_by_keyword(df: pd.DataFrame, keyword: str) -> Optional[str]:
    for col in df.columns:
        if keyword.lower() in str(col).lower(): return str(col)
    return None

async def fetch_with_retries(session: httpx.AsyncClient, url: str) -> Optional[httpx.Response]:
    for attempt in range(MAX_RETRIES):
        try:
            resp = await session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status() # Lança erro para status como 500, 403, etc.
            return resp
        except (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPStatusError):
            if attempt + 1 == MAX_RETRIES: return None
            await asyncio.sleep(RETRY_DELAY_SECONDS * (attempt + 1))
    return None

async def processar_lote_ceps(lote_df: pd.DataFrame, job_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    ceps_unicos_lote = lote_df[['cep_padronizado', job_info['prop_col'], job_info['cep_col']]].drop_duplicates(subset=['cep_padronizado'])
    result_map = {}
    
    async with httpx.AsyncClient() as session:
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        async def get_cep_data(cep: str, prop_info: Dict[str, Any]):
            async with semaphore:
                brasilapi_resp = await fetch_with_retries(session, BRASILAPI_V2_URL.format(cep=cep))
                if brasilapi_resp:
                    data = brasilapi_resp.json()
                    result_map[prop_info['CEP']] = {**prop_info, 'ENDEREÇO': data.get('street'), 'BAIRRO': data.get('neighborhood'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state'), 'STATUS': 'BRASILAPI: Sucesso'}
                    return

                viacep_resp = await fetch_with_retries(session, VIACEP_URL.format(cep=cep))
                if viacep_resp and 'erro' not in viacep_resp.text:
                    data = viacep_resp.json()
                    result_map[prop_info['CEP']] = {**prop_info, 'ENDEREÇO': data.get('logradouro'), 'BAIRRO': data.get('bairro'), 'CIDADE': data.get('localidade'), 'ESTADO': data.get('uf'), 'STATUS': 'VIACEP: Sucesso'}
                    return
                
                result_map[prop_info['CEP']] = {**prop_info, 'STATUS': 'FALHA TOTAL'}

        tasks = [get_cep_data(row['cep_padronizado'], {'PROPOSTA': row[job_info['prop_col']], 'CEP': row[job_info['cep_col']]}) for _, row in ceps_unicos_lote.iterrows()]
        await asyncio.gather(*tasks)

    return [result_map.get(row[job_info['cep_col']], {'PROPOSTA': row[job_info['prop_col']], 'CEP': row[job_info['cep_col']], 'STATUS': 'CEP Inválido'}) for _, row in lote_df.iterrows()]

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False, sheet_name='Resultados_CEP')
    return output.getvalue()

def main():
    st.set_page_config(page_title="Capital Consig - Validador de CEP", layout="wide")
    db.init_db()

    # --- Gerenciamento de Estado ---
    if 'active_job_id' not in st.session_state: st.session_state.active_job_id = None
    if 'start_time' not in st.session_state: st.session_state.start_time = None
    
    active_job = db.get_active_job()
    st.session_state.active_job_id = active_job['id'] if active_job else None

    with st.sidebar:
        st.image("logo.png", use_container_width=True)
        st.title("Portal de Validação")
        st.info("Production Stability Engine v10.0")

    st.header("Processamento de CEP em Lote")
    is_worker_active = st.session_state.active_job_id is not None
    
    with st.expander("1. Adicionar Novo Job à Fila", expanded=True):
        uploaded_file = st.file_uploader("Selecione sua planilha (.xlsx)", disabled=is_worker_active, key="file_uploader")
        if uploaded_file:
            try:
                df = pd.read_excel(uploaded_file, dtype=str)
                cep_col = find_column_by_keyword(df, "cep")
                prop_col = find_column_by_keyword(df, "proposta")
                if cep_col and prop_col:
                    st.success(f"Arquivo válido! Total de {len(df)} registros. CEP em '{cep_col}', Proposta em '{prop_col}'.")
                    if st.button("➕ Adicionar à Fila", use_container_width=True):
                        saved_path = DATA_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uploaded_file.name}"
                        with open(saved_path, "wb") as f: f.write(uploaded_file.getbuffer())
                        db.create_job(uploaded_file.name, str(saved_path), cep_col, prop_col, len(df))
                        st.rerun()
                else:
                    st.error("ERRO: Colunas 'CEP' e/ou 'Proposta' não encontradas na planilha. Verifique o nome das colunas.")
            except Exception as e: st.error(f"Erro ao ler planilha: {e}")

    # --- PAINEL DE CONTROLE DO WORKER ---
    worker_panel = st.container()
    if is_worker_active:
        job_info = db.get_job_by_id(st.session_state.active_job_id)
        with worker_panel:
            st.info(f"Trabalhador Ativo - Processando Job #{job_info['id']}: {job_info['original_filename']}")
            progress_bar = st.progress(0, text="Calculando...")
            metrics_cols = st.columns(3)
            speed_metric = metrics_cols[0].empty()
            etc_metric = metrics_cols[1].empty()
            
            # --- Lógica do Worker ---
            if st.session_state.start_time is None: st.session_state.start_time = time.time()
            start_index = job_info['processed_ceps']
            lote_df = pd.read_excel(job_info['saved_filepath'], dtype=str).iloc[start_index : start_index + BATCH_SIZE]
            df_full_len = job_info['total_ceps']
            
            if not lote_df.empty:
                lote_df['cep_padronizado'] = lote_df[job_info['cep_col']].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
                results = asyncio.run(processar_lote_ceps(lote_df, job_info))
                db.save_results_to_db(job_info['id'], results)
                new_processed = start_index + len(lote_df)
                db.update_job_status(job_info['id'], 'PROCESSANDO', new_processed)

                elapsed = time.time() - st.session_state.start_time
                speed = new_processed / elapsed if elapsed > 1 else 0
                etc_seconds = (df_full_len - new_processed) / speed if speed > 0 else 0

                progress_bar.progress(new_processed / df_full_len, text=f"Progresso: {new_processed} de {df_full_len}")
                speed_metric.metric("Velocidade", f"{speed:.1f} reg/s")
                etc_metric.metric("Tempo Restante", f"{timedelta(seconds=int(etc_seconds))}")
                
                time.sleep(0.5); st.rerun()
            else: # Concluído
                db.update_job_status(job_info['id'], 'CONCLUIDO')
                worker_panel.success(f"Job #{job_info['id']} concluído!")
                st.session_state.active_job_id = None; st.session_state.start_time = None
                time.sleep(2); st.rerun()
    else: # Worker não ativo
        next_job = db.get_next_pending_job()
        if next_job:
            st.button(f"▶️ INICIAR PROCESSAMENTO DA FILA (Próximo: Job #{next_job['id']})", use_container_width=True, type="primary", on_click=lambda: db.update_job_status(next_job['id'], 'PROCESSANDO'))

    # --- Fila de Processamento Global ---
    st.subheader("2. Fila de Processamento")
    for job in db.get_all_jobs():
        if job['id'] == st.session_state.active_job_id: continue # Não mostra o job ativo na lista, pois ele já está no painel
        status_color = {'PENDENTE': 'blue', 'CONCLUIDO': 'green', 'FALHOU': 'red'}.get(job['status'], 'gray')
        with st.container(border=True):
            c1, c2, c3 = st.columns([3, 1.5, 1])
            c1.write(f"**Job #{job['id']}**: {job['original_filename']} ({job['processed_ceps']}/{job['total_ceps']})")
            c2.write(f"Status: **:{status_color}[{job['status']}]**")
            if job['status'] == 'CONCLUIDO':
                c3.download_button("⬇️ Exportar", to_excel_bytes(db.get_job_results_as_df(job['id'])), f"RESULTADO_JOB_{job['id']}.xlsx", use_container_width=True, key=f"dl_{job['id']}")

if __name__ == "__main__":
    main()