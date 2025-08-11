# app.py (Versão 11.0 - The Direct-Feedback Engine)
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

# --- CONFIGURAÇÕES ---
BATCH_SIZE = 200
CONCURRENCY_LIMIT = 20
REQUEST_TIMEOUT = 10
MAX_RETRIES = 2
RETRY_DELAY = 1
DATA_DIR = Path("data")
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"

# --- FUNÇÕES CORE ---
def find_column_by_keyword(df: pd.DataFrame, keyword: str) -> Optional[str]:
    for col in df.columns:
        if keyword.lower() in str(col).lower(): return str(col)
    return None

async def fetch_with_retries(session: httpx.AsyncClient, url: str) -> Optional[httpx.Response]:
    for attempt in range(MAX_RETRIES):
        try:
            resp = await session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200: return resp
        except (httpx.TimeoutException, httpx.ConnectError):
            if attempt + 1 < MAX_RETRIES: await asyncio.sleep(RETRY_DELAY)
    return None

async def process_batch_async(lote_df: pd.DataFrame, job_info: Dict, session: httpx.AsyncClient) -> List[Dict]:
    cep_unicos = lote_df.drop_duplicates(subset=['cep_padronizado'])
    result_map = {}
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async def get_cep_data(cep: str, prop_info: Dict):
        async with semaphore:
            brasil_resp = await fetch_with_retries(session, BRASILAPI_V2_URL.format(cep=cep))
            if brasil_resp and brasil_resp.status_code == 200:
                data = brasil_resp.json()
                result_map[cep] = {**prop_info, 'ENDEREÇO': data.get('street'), 'BAIRRO': data.get('neighborhood'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state'), 'STATUS': 'BRASILAPI: Sucesso'}
                return
            
            viacep_resp = await fetch_with_retries(session, VIACEP_URL.format(cep=cep))
            if viacep_resp and viacep_resp.status_code == 200 and 'erro' not in viacep_resp.text:
                data = viacep_resp.json()
                result_map[cep] = {**prop_info, 'ENDEREÇO': data.get('logradouro'), 'BAIRRO': data.get('bairro'), 'CIDADE': data.get('localidade'), 'ESTADO': data.get('uf'), 'STATUS': 'VIACEP: Sucesso'}
                return
            
            result_map[cep] = {**prop_info, 'STATUS': 'FALHA TOTAL'}

    tasks = [get_cep_data(row['cep_padronizado'], {'PROPOSTA': row[job_info['prop_col']], 'CEP': row[job_info['cep_col']]}) for _, row in cep_unicos.iterrows()]
    await asyncio.gather(*tasks)

    return [result_map.get(row['cep_padronizado'], {'PROPOSTA': row[job_info['prop_col']], 'CEP': row[job_info['cep_col']], 'STATUS': 'CEP Inválido'}) for _, row in lote_df.iterrows()]

async def run_worker_job_async(job_id: int, ui_elements: Dict):
    job_info = db.get_job_by_id(job_id)
    start_time = time.time()
    try:
        df_full = pd.read_excel(job_info['saved_filepath'], dtype=str)
        df_full['cep_padronizado'] = df_full[job_info['cep_col']].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
        
        lotes = [df_full.iloc[i:i + BATCH_SIZE] for i in range(0, len(df_full), BATCH_SIZE)]
        processed_count = 0

        async with httpx.AsyncClient() as session:
            for i, lote_df in enumerate(lotes):
                results = await process_batch_async(lote_df, job_info, session)
                db.save_results_to_db(job_id, results)
                processed_count += len(lote_df)
                db.update_job_status(job_id, 'PROCESSANDO', processed_count)

                # Atualiza a UI em tempo real via callbacks
                elapsed = time.time() - start_time
                speed = processed_count / elapsed if elapsed > 0 else 0
                remaining = job_info['total_ceps'] - processed_count
                etc = timedelta(seconds=int(remaining / speed)) if speed > 0 else "Calculando..."
                
                ui_elements["progress_bar"].progress(processed_count / job_info['total_ceps'], text=f"Lote {i+1}/{len(lotes)} | Progresso: {processed_count} de {job_info['total_ceps']}")
                ui_elements["speed_metric"].metric("Velocidade", f"{speed:.1f} reg/s")
                ui_elements["etc_metric"].metric("Tempo Restante", f"{etc}")
        
        db.update_job_status(job_id, 'CONCLUIDO')
        ui_elements["main_panel"].success(f"Job #{job_id} concluído com sucesso!")

    except Exception as e:
        db.update_job_status(job_id, 'FALHOU')
        ui_elements["main_panel"].error(f"Erro crítico no Job #{job_id}: {e}")
    finally:
        time.sleep(3) # Pausa para ver a mensagem final
        st.rerun() # Força o recarregamento final para liberar a UI

def main():
    st.set_page_config(page_title="Capital Consig - Validador CEP", layout="wide")
    db.init_db()

    with st.sidebar:
        st.image("logo.png", use_container_width=True)
        st.title("Portal de Validação")
        st.info("Direct-Feedback Engine v11.0")
    st.header("Processamento de CEP em Lote")

    active_job = db.get_active_job()

    if active_job:
        # MODO DE PROCESSAMENTO - UI BLOQUEADA E COM FEEDBACK
        st.info(f"TRABALHADOR ATIVO - Processando Job #{active_job['id']}: {active_job['original_filename']}")
        st.markdown("---")
        progress_bar = st.progress(0, "Aguardando início do lote...")
        m_cols = st.columns(3)
        speed_metric = m_cols[0].empty()
        etc_metric = m_cols[1].empty()
        st.markdown("---")
        st.warning("A aplicação está processando. Por favor, não feche ou recarregue esta aba.")
        
        ui_elements = {"progress_bar": progress_bar, "speed_metric": speed_metric, "etc_metric": etc_metric, "main_panel": st.container()}
        asyncio.run(run_worker_job_async(active_job['id'], ui_elements))
    
    else:
        # MODO NORMAL - UI DESBLOQUEADA
        with st.expander("1. Adicionar Novo Job à Fila", expanded=True):
            uploaded_file = st.file_uploader("Selecione sua planilha (.xlsx)")
            if uploaded_file:
                try:
                    df = pd.read_excel(uploaded_file, dtype=str)
                    cep_col = find_column_by_keyword(df, "cep")
                    prop_col = find_column_by_keyword(df, "proposta")
                    if cep_col and prop_col:
                        st.success(f"Arquivo OK! {len(df)} registros.")
                        if st.button("➕ Adicionar à Fila", use_container_width=True):
                            filepath = DATA_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uploaded_file.name}"
                            with open(filepath, "wb") as f: f.write(uploaded_file.getbuffer())
                            db.create_job(uploaded_file.name, str(filepath), cep_col, prop_col, len(df))
                            st.rerun()
                    else: st.error("ERRO: Colunas 'CEP' e/ou 'Proposta' não encontradas.")
                except Exception as e: st.error(f"Erro ao ler planilha: {e}")

        st.subheader("2. Fila de Processamento Global")
        next_job = db.get_next_pending_job()
        if next_job:
            st.button(f"▶️ INICIAR PROCESSAMENTO (Próximo: Job #{next_job['id']})", use_container_width=True, type="primary", 
                      on_click=lambda: db.update_job_status(next_job['id'], 'PROCESSANDO'))
        
        all_jobs = db.get_all_jobs()
        if not all_jobs:
            st.info("Nenhum job na fila.")
        
        for job in all_jobs:
            status_color = {'PENDENTE': 'blue', 'PROCESSANDO': 'orange', 'CONCLUIDO': 'green', 'FALHOU': 'red'}.get(job['status'], 'gray')
            with st.container(border=True):
                c1, c2, c3 = st.columns([3, 1.5, 1])
                c1.write(f"**Job #{job['id']}**: {job['original_filename']} ({job['processed_ceps']}/{job['total_ceps']})")
                c2.write(f"Status: **:{status_color}[{job['status']}]**")
                if job['status'] == 'CONCLUIDO':
                    c3.download_button("⬇️ Exportar", to_excel_bytes(db.get_job_results_as_df(job['id'])), f"RESULTADO_JOB_{job['id']}.xlsx", use_container_width=True, key=f"dl_{job['id']}")
                elif job['status'] == 'FALHOU':
                     c3.error("FALHOU")

if __name__ == "__main__":
    main()