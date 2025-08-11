# app.py (Versão 5.0 - High-Performance Engine)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime
import database as db

# --- CONFIGURAÇÕES DE ALTA PERFORMANCE ---
CONCURRENCY_LIMIT = 50          # Aumentamos drasticamente a concorrência
REQUEST_TIMEOUT = 20            # Timeout de 20 segundos por requisição
# PAUSE_BETWEEN_BATCHES foi removida. Não precisamos mais dela.

DATA_DIR = Path("data")
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
AWESOMEAPI_URL = "https://cep.awesomeapi.com.br/json/{cep}"

# --- FUNÇÕES CORE (REESCRITAS PARA PERFORMANCE) ---

def find_column_by_keyword(df: pd.DataFrame, keyword: str) -> Optional[str]:
    for col in df.columns:
        if keyword.lower() in str(col).lower():
            return str(col)
    return None

def parse_brasilapi(data: Dict[str, Any]) -> Dict[str, Any]:
    return {'ENDEREÇO': data.get('street'), 'BAIRRO': data.get('neighborhood'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state')}

def parse_viacep(data: Dict[str, Any]) -> Dict[str, Any]:
    return {'ENDEREÇO': data.get('logradouro'), 'BAIRRO': data.get('bairro'), 'CIDADE': data.get('localidade'), 'ESTADO': data.get('uf')}

def parse_awesomeapi(data: Dict[str, Any]) -> Dict[str, Any]:
    return {'ENDEREÇO': data.get('address'), 'BAIRRO': data.get('district'), 'CIDADE': data.get('city'), 'ESTADO': data.get('state')}

async def processar_job(job_id: int):
    st.session_state['is_processing'] = True
    job_info = db.get_job_by_id(job_id)
    if not job_info: st.error(f"Job {job_id} não encontrado."); st.session_state['is_processing'] = False; return

    progress_placeholder = st.session_state.get('progress_placeholder')
    
    try:
        db.update_job_status(job_id, 'PROCESSANDO'); st.rerun()
        df = pd.read_excel(job_info['saved_filepath'], dtype=str)
        df['cep_padronizado'] = df[job_info['cep_col']].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)

        # Cache em memória para evitar requisições duplicadas DENTRO do mesmo job
        cep_cache = {}
        # Lista final de resultados
        final_results = []
        
        # Lista de CEPs únicos para processar
        ceps_para_processar = df[['cep_padronizado', job_info['prop_col'], job_info['cep_col']]].drop_duplicates(subset=['cep_padronizado'])
        
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as session:
            semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
            
            # --- PASSO 1: ATACAR COM A BRASILAPI (A MAIS RÁPIDA) ---
            async def fetch_brasilapi(cep, prop_info):
                async with semaphore:
                    if cep in cep_cache: return None # Já processado
                    try:
                        resp = await session.get(BRASILAPI_V2_URL.format(cep=cep))
                        if resp.status_code == 200:
                            data = resp.json()
                            cep_cache[cep] = {**prop_info, **parse_brasilapi(data), 'STATUS': 'BRASILAPI: Sucesso'}
                        else: return (cep, prop_info) # Falhou, precisa de fallback
                    except Exception: return (cep, prop_info) # Falhou, precisa de fallback
                    return None
            
            tasks_brasilapi = [fetch_brasilapi(row['cep_padronizado'], {'PROPOSTA': row[job_info['prop_col']], 'CEP': row[job_info['cep_col']]}) for _, row in ceps_para_processar.iterrows()]
            
            if progress_placeholder: progress_placeholder.progress(0.1, text=f"Job #{job_id}: Consultando {len(tasks_brasilapi)} CEPs na BrasilAPI...")
            
            ceps_falharam_brasilapi = [r for r in await asyncio.gather(*tasks_brasilapi) if r is not None]

            # --- PASSO 2: FALLBACK PARA VIACEP E AWESOMEAPI (SÓ PARA AS FALHAS) ---
            if ceps_falharam_brasilapi:
                async def fetch_fallbacks(cep, prop_info):
                    async with semaphore:
                        if cep in cep_cache: return None
                        try:
                            fallback_tasks = {
                                'viacep': session.get(VIACEP_URL.format(cep=cep)),
                                'awesome': session.get(AWESOMEAPI_URL.format(cep=cep))
                            }
                            responses = await asyncio.gather(*fallback_tasks.values())
                            # Tenta ViaCEP primeiro
                            if responses[0].status_code == 200 and 'erro' not in responses[0].text:
                                cep_cache[cep] = {**prop_info, **parse_viacep(responses[0].json()), 'STATUS': 'VIACEP: Sucesso'}
                            # Se não, tenta AwesomeAPI
                            elif responses[1].status_code == 200:
                                cep_cache[cep] = {**prop_info, **parse_awesomeapi(responses[1].json()), 'STATUS': 'AWESOMEAPI: Sucesso'}
                            # Se ambos falharem
                            else: cep_cache[cep] = {**prop_info, 'STATUS': 'TODAS AS APIS: Falha'}
                        except Exception: cep_cache[cep] = {**prop_info, 'STATUS': 'TODAS AS APIS: Erro Crítico'}

                tasks_fallback = [fetch_fallbacks(cep, prop_info) for cep, prop_info in ceps_falharam_brasilapi]
                if progress_placeholder: progress_placeholder.progress(0.8, text=f"Job #{job_id}: Consultando {len(tasks_fallback)} CEPs nas APIs de fallback...")
                await asyncio.gather(*tasks_fallback)
        
        # Mapeia os resultados do cache de volta para o DataFrame original
        final_results = [cep_cache.get(row['cep_padronizado'], {
            'PROPOSTA': row[job_info['prop_col']], 
            'CEP': row[job_info['cep_col']], 
            'STATUS': 'CEP Inválido'
        }) for _, row in df.iterrows()]
        
        db.save_results_to_db(job_id, final_results)
        db.update_job_status(job_id, 'CONCLUIDO', processed_ceps=len(df))

    except Exception as e:
        db.update_job_status(job_id, 'FALHOU')
        st.error(f"Erro Crítico no Job {job_id}: {e}")
    finally:
        st.session_state['is_processing'] = False
        if progress_placeholder: progress_placeholder.empty()

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados_CEP')
    return output.getvalue()

def render_ui():
    st.set_page_config(page_title="Serviços CEP", layout="wide")
    with st.sidebar:
        st.image("logo.png", use_container_width=True)
        st.title("Portal de Validação")
        st.info("High-Performance Engine v5.0")

    st.header("Processamento de CEP em Lote")
    
    if 'is_processing' not in st.session_state:
        st.session_state['is_processing'] = False

    with st.expander("1. Criar Novo Job de Processamento", expanded=True):
        uploaded_file = st.file_uploader("Selecione sua planilha (.xlsx ou .csv)", disabled=st.session_state.is_processing)
        if uploaded_file:
            try:
                df = pd.read_excel(uploaded_file, dtype=str)
                cep_col = find_column_by_keyword(df, "cep"); prop_col = find_column_by_keyword(df, "proposta")
                if cep_col and prop_col:
                    st.success(f"Arquivo OK. CEP: '{cep_col}', Proposta: '{prop_col}'. {len(df)} registros.")
                    if st.button("➕ Adicionar Job à Fila", use_container_width=True, disabled=st.session_state.is_processing):
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        saved_path = DATA_DIR / f"{timestamp}_{uploaded_file.name}"
                        with open(saved_path, "wb") as f: f.write(uploaded_file.getbuffer())
                        db.create_job(uploaded_file.name, str(saved_path), cep_col, prop_col, len(df))
                        st.rerun()
                else: st.error("Colunas 'CEP' e/ou 'Proposta' não encontradas.")
            except Exception as e: st.error(f"Erro ao ler planilha: {e}")

    st.subheader("2. Fila de Processamento")

    st.session_state['progress_placeholder'] = st.empty()
    if st.session_state.is_processing:
        st.session_state.progress_placeholder.info("Um job está em processamento. Por favor, aguarde...")
    
    all_jobs = db.get_all_jobs()
    if not all_jobs:
        st.info("Nenhum job na fila.")
    else:
        for job in all_jobs:
            status_color = {'PENDENTE': 'blue', 'PROCESSANDO': 'orange', 'CONCLUIDO': 'green', 'FALHOU': 'red'}.get(job['status'], 'gray')
            with st.container(border=True):
                c1, c2, c3 = st.columns([3, 1.5, 1])
                c1.write(f"**Job #{job['id']}**: {job['original_filename']}")
                if job['status'] == 'CONCLUIDO':
                    c1.write(f"Registros processados: {job['total_ceps']}")
                else:
                    c1.write(f"Total de registros: {job['total_ceps']}")
                c2.write(f"Status: **:{status_color}[{job['status']}]**")

                if job['status'] == 'PENDENTE':
                    if c3.button("▶️ Processar", key=f"run_{job['id']}", use_container_width=True, disabled=st.session_state.is_processing):
                        asyncio.run(processar_job(job['id']))
                        st.rerun()
                elif job['status'] == 'CONCLUIDO':
                    c3.download_button("⬇️ Baixar", to_excel_bytes(db.get_job_results_as_df(job['id'])), f"RESULTADO_{job['id']}.xlsx", use_container_width=True)
                elif job['status'] == 'FALHOU':
                    c3.error("Falhou")

def main():
    db.init_db() 
    render_ui() 

if __name__ == "__main__":
    main()