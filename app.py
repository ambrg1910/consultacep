# app.py (Versão 9.0 - Resilient Paced Engine)
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
CONCURRENCY_LIMIT = 20          # Limite mais gentil para evitar bloqueios de API
REQUEST_TIMEOUT = 15            
MAX_RETRIES = 3                 # Número de retentativas para cada API
RETRY_DELAY_SECONDS = 1         # Delay inicial entre retentativas
DATA_DIR = Path("data")

VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"

# --- FUNÇÕES CORE ---
def find_column_by_keyword(df: pd.DataFrame, keyword: str) -> Optional[str]:
    for col in df.columns:
        if keyword.lower() in str(col).lower(): return str(col)
    return None

async def fetch_with_retries(session: httpx.AsyncClient, url: str) -> Optional[httpx.Response]:
    """Tenta buscar uma URL com retentativas e recuo exponencial simples."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = await session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status() # Lança um erro para status como 500, 403, etc.
            return resp
        except (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPStatusError):
            if attempt + 1 == MAX_RETRIES: return None # Desiste após a última tentativa
            await asyncio.sleep(RETRY_DELAY_SECONDS * (attempt + 1)) # Delay de 1s, 2s, 3s...
    return None

async def processar_lote_ceps(lote_df: pd.DataFrame, job_info: Dict[str, Any], session: httpx.AsyncClient) -> List[Dict[str, Any]]:
    cep_cache = {}
    ceps_unicos_lote = lote_df[['cep_padronizado', job_info['prop_col'], job_info['cep_col']]].drop_duplicates(subset=['cep_padronizado'])
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async def get_cep_data(cep, prop_info):
        async with semaphore:
            # 1. Tenta BrasilAPI com retentativas
            brasilapi_resp = await fetch_with_retries(session, BRASILAPI_V2_URL.format(cep=cep))
            if brasilapi_resp:
                data = brasilapi_resp.json()
                return {**prop_info, 'ENDEREÇO': data.get('street'), 'BAIRRO': data.get('neighborhood'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state'), 'STATUS': 'BRASILAPI: Sucesso'}
            
            # 2. Se falhar, tenta ViaCEP com retentativas
            viacep_resp = await fetch_with_retries(session, VIACEP_URL.format(cep=cep))
            if viacep_resp and 'erro' not in viacep_resp.text:
                data = viacep_resp.json()
                return {**prop_info, 'ENDEREÇO': data.get('logradouro'), 'BAIRRO': data.get('bairro'), 'CIDADE': data.get('localidade'), 'ESTADO': data.get('uf'), 'STATUS': 'VIACEP: Sucesso'}

            # 3. Se tudo falhar
            return {**prop_info, 'STATUS': 'FALHA TOTAL'}

    tasks = [get_cep_data(row['cep_padronizado'], {'PROPOSTA': row[job_info['prop_col']], 'CEP': row[job_info['cep_col']]}) for _, row in ceps_unicos_lote.iterrows()]
    results_unicos = await asyncio.gather(*tasks)

    # Cria um mapa de resultados para preenchimento rápido
    result_map = {res['PROPOSTA']: res for res in results_unicos if 'PROPOSTA' in res}
    
    # Monta a lista final na ordem original do lote
    final_results = []
    for _, row in lote_df.iterrows():
        prop = row[job_info['prop_col']]
        if prop in result_map:
            final_results.append(result_map[prop])
        else: # Caso de CEP inválido ou erro no mapeamento
            final_results.append({'PROPOSTA': prop, 'CEP': row[job_info['cep_col']], 'STATUS': 'ERRO DE MAPEAMENTO'})

    return final_results

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO();
    with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False, sheet_name='Resultados_CEP')
    return output.getvalue()

def main():
    st.set_page_config(page_title="Capital Consig - Validador de CEP", layout="wide")
    db.init_db()

    # --- Gerenciamento de Estado ---
    if 'active_job_id' not in st.session_state: st.session_state.active_job_id = None
    if 'start_time' not in st.session_state: st.session_state.start_time = None
    
    active_job_info = db.get_active_job()
    if active_job_info:
        st.session_state.active_job_id = active_job_info['id']
    else:
        # Se nenhum job estiver 'PROCESSANDO', mas um ID ativo ainda estiver na sessão, limpa-o
        if st.session_state.active_job_id is not None:
             job_check = db.get_job_by_id(st.session_state.active_job_id)
             if not job_check or job_check['status'] != 'PROCESSANDO':
                st.session_state.active_job_id = None
                st.session_state.start_time = None

    # --- Interface Gráfica ---
    with st.sidebar:
        st.image("logo.png", use_container_width=True); st.title("Portal de Validação"); st.info("Resilient Paced Engine v9.0")
    st.header("Processamento de CEP em Lote")
    is_worker_active = st.session_state.active_job_id is not None
    
    with st.expander("1. Adicionar Novo Job à Fila", expanded=True):
        uploaded_file = st.file_uploader("Selecione sua planilha (.xlsx)", disabled=is_worker_active)
        if uploaded_file:
            # ... (código de upload sem alterações) ...
            try:
                df = pd.read_excel(uploaded_file, dtype=str)
                cep_col = find_column_by_keyword(df, "cep"); prop_col = find_column_by_keyword(df, "proposta")
                if cep_col and prop_col:
                    st.success(f"Arquivo OK. Total de {len(df)} registros.")
                    if st.button("➕ Adicionar à Fila de Processamento", use_container_width=True, disabled=is_worker_active):
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        saved_path = DATA_DIR / f"{timestamp}_{uploaded_file.name}";
                        with open(saved_path, "wb") as f: f.write(uploaded_file.getbuffer())
                        db.create_job(uploaded_file.name, str(saved_path), cep_col, prop_col, len(df)); st.rerun()
                else: st.error("Colunas 'CEP' e/ou 'Proposta' não encontradas.")
            except Exception as e: st.error(f"Erro ao ler planilha: {e}")

    worker_panel = st.container()

    st.subheader("2. Fila de Processamento Global")
    all_jobs = db.get_all_jobs()
    if not all_jobs: st.info("Nenhum job na fila.")
    else:
        for job in all_jobs:
            # ... (código de exibição da lista de jobs sem alterações) ...
            status_color = {'PENDENTE': 'blue', 'PROCESSANDO': 'orange', 'CONCLUIDO': 'green', 'FALHOU': 'red', 'CANCELADO': 'gray'}.get(job['status'], 'gray')
            with st.container(border=True):
                c1, c2, c3 = st.columns([3, 1.5, 1])
                c1.write(f"**Job #{job['id']}**: {job['original_filename']}")
                c1.write(f"Registros: {job['processed_ceps']}/{job['total_ceps']}")
                c2.write(f"Status: **:{status_color}[{job['status']}]**")
                if job['status'] == 'CONCLUIDO':
                    c3.download_button("⬇️ Exportar", to_excel_bytes(db.get_job_results_as_df(job['id'])), f"RESULTADO_JOB_{job['id']}.xlsx", use_container_width=True)


    # --- Lógica do Worker ---
    if is_worker_active:
        job_info = db.get_job_by_id(st.session_state.active_job_id)
        with worker_panel:
            # ... (painel de métricas sem alterações) ...
            st.info(f"Trabalhador Ativo - Processando Job #{job_info['id']}")
            progress_bar = st.progress(0, text="Iniciando...")
            metrics_cols = st.columns(3)
            speed_metric = metrics_cols[0].empty()
            etc_metric = metrics_cols[1].empty()
            errors_metric = metrics_cols[2].empty() # Placeholder para futuras contagens de erro
        
        try:
            df_full = pd.read_excel(job_info['saved_filepath'], dtype=str)
            df_full['cep_padronizado'] = df_full[job_info['cep_col']].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
            
            start_index = job_info['processed_ceps']
            if st.session_state.start_time is None: st.session_state.start_time = time.time() - 1 # Evita divisão por zero

            if start_index < job_info['total_ceps']:
                lote_df = df_full.iloc[start_index : start_index + BATCH_SIZE]
                results = asyncio.run(processar_lote_ceps(lote_df, job_info, httpx.AsyncClient()))
                
                new_processed_count = start_index + len(lote_df)
                db.save_results_to_db(job_info['id'], results)
                db.update_job_status(job_info['id'], 'PROCESSANDO', new_processed_count)

                # Atualiza métricas
                elapsed_time = time.time() - st.session_state.start_time
                speed = new_processed_count / elapsed_time
                remaining = job_info['total_ceps'] - new_processed_count
                etc_seconds = (remaining / speed) if speed > 0 else 0
                
                progress_bar.progress(new_processed_count / job_info['total_ceps'], text=f"Progresso: {new_processed_count} de {job_info['total_ceps']}")
                speed_metric.metric("Velocidade", f"{speed:.1f} reg/s")
                etc_metric.metric("Tempo Restante", f"{timedelta(seconds=int(etc_seconds))}")
                
                time.sleep(0.5); st.rerun()
            else: # Concluído
                db.update_job_status(job_info['id'], 'CONCLUIDO')
                worker_panel.success(f"Job #{job_info['id']} concluído!");
                st.session_state.active_job_id = None; st.session_state.start_time = None
                time.sleep(2); st.rerun()
        
        except Exception as e:
            db.update_job_status(job_info['id'], 'FALHOU');
            worker_panel.error(f"Erro Crítico: {e}")
            st.session_state.active_job_id = None; st.session_state.start_time = None
            time.sleep(5); st.rerun()

    elif db.get_next_pending_job(): # Worker não está ativo, mas há jobs pendentes
        next_job = db.get_next_pending_job()
        if st.button(f"▶️ INICIAR PROCESSAMENTO DA FILA (Próximo: Job #{next_job['id']})", use_container_width=True, type="primary"):
            st.session_state.active_job_id = next_job['id']
            db.update_job_status(next_job['id'], 'PROCESSANDO'); st.rerun()

if __name__ == "__main__":
    main()