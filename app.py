# app.py (Versão 12.0 - The Unbreakable Synchronous Engine)
import streamlit as st
import pandas as pd
import requests
from io import BytesIO
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime, timedelta
import database as db
import time
from concurrent.futures import ThreadPoolExecutor

# --- CONFIGURAÇÕES ---
BATCH_SIZE = 200
MAX_WORKERS = 20                # Equivalente ao CONCURRENCY_LIMIT para o modo síncrono
REQUEST_TIMEOUT = 10
DATA_DIR = Path("data")
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"

# --- FUNÇÕES CORE ---
def find_column_by_keyword(df: pd.DataFrame, keyword: str) -> Optional[str]:
    for col in df.columns:
        if keyword.lower() in str(col).lower(): return str(col)
    return None

def get_cep_data(cep: str, session: requests.Session) -> Dict[str, Any]:
    """Busca dados de um único CEP com a lógica de fallback."""
    try: # Tenta BrasilAPI
        resp = session.get(BRASILAPI_V2_URL.format(cep=cep), timeout=REQUEST_TIMEOUT)
        if resp.ok:
            data = resp.json()
            return {'ENDEREÇO': data.get('street'), 'BAIRRO': data.get('neighborhood'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state'), 'STATUS': 'BRASILAPI: Sucesso'}
    except requests.exceptions.RequestException: pass

    try: # Tenta ViaCEP
        resp = session.get(VIACEP_URL.format(cep=cep), timeout=REQUEST_TIMEOUT)
        if resp.ok and 'erro' not in resp.text:
            data = resp.json()
            return {'ENDEREÇO': data.get('logradouro'), 'BAIRRO': data.get('bairro'), 'CIDADE': data.get('localidade'), 'ESTADO': data.get('uf'), 'STATUS': 'VIACEP: Sucesso'}
    except requests.exceptions.RequestException: pass
    
    return {'STATUS': 'FALHA TOTAL'}

def process_batch(lote_df: pd.DataFrame, job_info: Dict) -> List[Dict]:
    """Processa um lote de CEPs em paralelo usando threads."""
    cep_unicos_map = {row['cep_padronizado']: {'PROPOSTA': row[job_info['prop_col']], 'CEP': row[job_info['cep_col']]} for _, row in lote_df.drop_duplicates(subset=['cep_padronizado']).iterrows()}
    result_map = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        with requests.Session() as session:
            futures = {executor.submit(get_cep_data, cep, session): cep for cep in cep_unicos_map.keys()}
            for future in futures:
                cep = futures[future]
                try:
                    result = future.result()
                    result_map[cep] = {**cep_unicos_map[cep], **result}
                except Exception:
                    result_map[cep] = {**cep_unicos_map[cep], 'STATUS': 'ERRO CRÍTICO'}
    
    return [result_map.get(row['cep_padronizado'], {**cep_unicos_map.get(row['cep_padronizado']), 'STATUS': 'CEP Inválido'}) for _, row in lote_df.iterrows()]

def run_worker_job(job_id: int):
    """Executa o processamento completo de um job, lote por lote, atualizando a UI."""
    job_info = db.get_job_by_id(job_id)
    ui = st.session_state.ui_elements

    try:
        df_full = pd.read_excel(job_info['saved_filepath'], dtype=str)
        df_full['cep_padronizado'] = df_full[job_info['cep_col']].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
        
        start_time = time.time()
        lotes = [df_full.iloc[i:i + BATCH_SIZE] for i in range(0, len(df_full), BATCH_SIZE)]
        processed_count = 0

        for i, lote_df in enumerate(lotes):
            results = process_batch(lote_df, job_info)
            db.save_results_to_db(job_id, results)
            processed_count += len(lote_df)
            db.update_job_status(job_id, 'PROCESSANDO', processed_count)

            # Atualiza UI em tempo real
            elapsed = time.time() - start_time
            speed = processed_count / elapsed if elapsed > 0 else 0
            remaining = job_info['total_ceps'] - processed_count
            etc = timedelta(seconds=int(remaining / speed)) if speed > 0 else "..."
            
            ui["progress_bar"].progress(processed_count / job_info['total_ceps'], text=f"Lote {i+1}/{len(lotes)} | {processed_count} de {job_info['total_ceps']}")
            ui["speed_metric"].metric("Velocidade", f"{speed:.1f} reg/s")
            ui["etc_metric"].metric("Tempo Restante", str(etc))
        
        db.update_job_status(job_id, 'CONCLUIDO')
        ui["main_panel"].success(f"Job #{job_id} concluído!")

    except Exception as e:
        db.update_job_status(job_id, 'FALHOU')
        ui["main_panel"].error(f"Erro crítico no Job #{job_id}: {e}")
    finally:
        st.session_state.active_job_id = None
        time.sleep(3); st.rerun()

def main():
    st.set_page_config(page_title="Capital Consig - Validador CEP", layout="wide")
    db.init_db()

    if 'active_job_id' not in st.session_state: st.session_state.active_job_id = None
    
    # --- UI Principal ---
    with st.sidebar:
        st.image("logo.png", use_container_width=True); st.title("Portal de Validação")
        st.info("Unbreakable Engine v12.0")
    st.header("Processamento de CEP em Lote")

    active_job = db.get_active_job()
    st.session_state.active_job_id = active_job['id'] if active_job else None

    if st.session_state.active_job_id:
        # MODO DE PROCESSAMENTO - UI BLOQUEADA E COM FEEDBACK
        job_info = db.get_job_by_id(st.session_state.active_job_id)
        st.info(f"TRABALHADOR ATIVO - Processando Job #{job_info['id']}: {job_info['original_filename']}")
        st.warning("A aplicação está processando. Não feche ou recarregue esta aba.")
        
        st.session_state.ui_elements = {
            "progress_bar": st.progress(0, "Aguardando início..."),
            "speed_metric": st.columns(3)[0].empty(),
            "etc_metric": st.columns(3)[1].empty(),
            "main_panel": st.container()
        }
        run_worker_job(st.session_state.active_job_id)

    else:
        # MODO NORMAL - UI DESBLOQUEADA
        with st.expander("1. Adicionar Novo Job à Fila", expanded=True):
            uploaded_file = st.file_uploader("Selecione sua planilha (.xlsx)")
            if uploaded_file:
                # ... (código de upload sem alterações) ...
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
            if st.button(f"▶️ INICIAR PROCESSAMENTO (Próximo: Job #{next_job['id']})", use_container_width=True, type="primary"):
                db.update_job_status(next_job['id'], 'PROCESSANDO')
                st.rerun()

        for job in db.get_all_jobs():
            # ... (código de exibição da lista de jobs sem alterações) ...
            status_color = {'PENDENTE': 'blue', 'PROCESSANDO': 'orange', 'CONCLUIDO': 'green', 'FALHOU': 'red'}.get(job['status'], 'gray')
            with st.container(border=True):
                c1, c2, c3 = st.columns([3, 1.5, 1])
                c1.write(f"**Job #{job['id']}**: {job['original_filename']} ({job['processed_ceps']}/{job['total_ceps']})")
                c2.write(f"Status: **:{status_color}[{job['status']}]**")
                if job['status'] == 'CONCLUIDO':
                    c3.download_button("⬇️ Exportar", to_exce