# app.py (Versão Industrial 2.0 - com Fila de Jobs Persistente)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
from typing import Optional, List, Dict
import os
import time
from datetime import datetime, timezone
import requests
from pathlib import Path

# Importar nossa nova lógica de banco de dados
import database as db

# --- CONFIGURAÇÃO DA APLICAÇÃO ---
BATCH_SIZE = 50                 # Lotes menores para salvar progresso mais frequentemente
PAUSE_BETWEEN_BATCHES = 5       # Pausa para ser gentil com as APIs
CONCURRENCY_LIMIT = 8           # Limite de requisições simultâneas
REQUEST_TIMEOUT = 30            # Timeout para cada requisição
DATA_DIR = Path("data")         # Pasta para salvar as planilhas enviadas

# Garante que o diretório de dados e o DB existam
DATA_DIR.mkdir(exist_ok=True)
db.init_db()

# --- DEFINIÇÃO DAS APIS (Sem alteração) ---
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
AWESOMEAPI_URL = "https://cep.awesomeapi.com.br/json/{cep}"


# --- FUNÇÕES DE LÓGICA E PROCESSAMENTO (Refatoradas para o sistema de Jobs) ---

def find_column_by_keyword(df: pd.DataFrame, keyword: str) -> Optional[str]:
    for col in df.columns:
        if keyword.lower() in str(col).lower():
            return str(col)
    return None

async def fetch_all_apis_for_cep(original_row: dict, cep: str, session: httpx.AsyncClient) -> List[Dict]:
    """Consulta todas as APIs para um único CEP de forma resiliente."""
    # ... (Esta função é IDÊNTICA à sua `fetch_all_apis_for_cep_resilient`) ...
    # ... por brevidade, vamos reusar a lógica. Vou colar a versão compacta dela ...
    if not cep or not cep.isdigit() or len(cep) != 8:
        error_row = original_row.copy(); error_row['STATUS'] = 'Formato de CEP Inválido'
        return [error_row]

    tasks = {
        "BRASILAPI": session.get(BRASILAPI_V2_URL.format(cep=cep)),
        "VIACEP": session.get(VIACEP_URL.format(cep=cep)),
        "AWESOMEAPI": session.get(AWESOMEAPI_URL.format(cep=cep))
    }
    responses = await asyncio.gather(*tasks.values(), return_exceptions=True)
    results_map = dict(zip(tasks.keys(), responses))
    output_rows = []

    # BrasilAPI
    res_br = results_map.get("BRASILAPI")
    data_br = res_br.json() if isinstance(res_br, httpx.Response) and res_br.status_code == 200 else {}
    output_rows.append({
        **original_row,
        'ENDEREÇO': data_br.get('street'), 'BAIRRO': data_br.get('neighborhood'),
        'CIDADE': data_br.get('city'), 'ESTADO': data_br.get('state'),
        'STATUS': 'BRASILAPI: Sucesso' if data_br else f'BRASILAPI: Falha ({type(res_br).__name__})'
    })

    # ViaCEP
    res_via = results_map.get("VIACEP")
    data_via = res_via.json() if isinstance(res_via, httpx.Response) and res_via.status_code == 200 and 'erro' not in res_via.text else {}
    output_rows.append({
        **original_row,
        'ENDEREÇO': data_via.get('logradouro'), 'BAIRRO': data_via.get('bairro'),
        'CIDADE': data_via.get('localidade'), 'ESTADO': data_via.get('uf'),
        'STATUS': 'VIACEP: Sucesso' if data_via else f'VIACEP: Falha ({type(res_via).__name__})'
    })

    # AwesomeAPI
    res_awe = results_map.get("AWESOMEAPI")
    data_awe = res_awe.json() if isinstance(res_awe, httpx.Response) and res_awe.status_code == 200 else {}
    output_rows.append({
        **original_row,
        'ENDEREÇO': data_awe.get('address'), 'BAIRRO': data_awe.get('district'),
        'CIDADE': data_awe.get('city'), 'ESTADO': data_awe.get('state'),
        'STATUS': 'AWESOMEAPI: Sucesso' if data_awe else f'AWESOMEAPI: Falha ({type(res_awe).__name__})'
    })

    return output_rows


async def processar_job(job_id: int, progress_bar_placeholder):
    """Função principal que executa um job de processamento."""
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
                        # Precisamos renomear as colunas para o formato final antes de passar
                        base_row = {
                            'PROPOSTA': row[job_info['prop_col']],
                            'CEP': row[job_info['cep_col']]
                        }
                        return await fetch_all_apis_for_cep(base_row, row['cep_padronizado'], session)

                tasks = [run_fetch(row) for _, row in lote_df.iterrows()]
                results_do_lote_nested = await asyncio.gather(*tasks)
                
                # Aplaina a lista de listas de resultados e salva no DB
                final_results_lote = [item for sublist in results_do_lote_nested for item in sublist]
                db.save_results_to_db(job_id, final_results_lote)

                total_propostas_processadas += len(lote_df)
                db.update_job_status(job_id, 'PROCESSANDO', processed_ceps=total_propostas_processadas)
                
                # Pausa estratégica entre lotes
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


# --- FUNÇÕES AUXILIARES DA INTERFACE ---

def to_excel_bytes(df: pd.DataFrame):
    """Converte um DataFrame para bytes de um arquivo Excel."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados_MultiAPI')
    return output.getvalue()


# --- INTERFACE GRÁFICA ---
st.set_page_config(page_title="Serviços CEP - Capital Consig", layout="wide")
with st.sidebar:
    st.image("logo.png", use_container_width=True)
    st.title("Capital Consig")
    st.info("Portal de Validação e Enriquecimento de Dados. Versão Industrial 2.0 com Fila de Jobs.")

st.header("Portal de Serviços de CEP")
st.divider()

tab_lote, tab_individual = st.tabs(["**Processamento em Lote (RECOMENDADO)**", "Consulta Individual (Rápida)"])

# --- ABA DE PROCESSAMENTO EM LOTE (TOTALMENTE REFEITA) ---
with tab_lote:
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
                    # Salva o arquivo fisicamente com um nome único
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    saved_path = DATA_DIR / f"{timestamp}_{uploaded_file.name}"
                    with open(saved_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    
                    # Cria o job no banco de dados
                    db.create_job(uploaded_file.name, str(saved_path), cep_col, prop_col, len(df))
                    st.success(f"Job para '{uploaded_file.name}' criado com sucesso! Veja na fila abaixo.")
                    st.rerun() # Atualiza a UI para mostrar o novo job na lista

        except Exception as e:
            st.error(f"Erro ao ler a planilha: {e}")

    st.divider()
    st.subheader("2. Fila de Processamento")
    
    # Placeholder para a barra de progresso que será usada por qualquer job
    progress_bar_placeholder = st.empty()

    all_jobs = db.get_all_jobs()
    if not all_jobs:
        st.info("Nenhum job na fila. Crie um novo acima.")
    else:
        for job in all_jobs:
            status_color = {
                'PENDENTE': 'blue', 'PROCESSANDO': 'orange',
                'CONCLUIDO': 'green', 'FALHOU': 'red'
            }.get(job['status'], 'gray')
            
            with st.container(border=True):
                col1, col2, col3, col4 = st.columns([4, 2, 2, 2])
                col1.write(f"**Job #{job['id']}**: {job['original_filename']}")
                col2.write(f"Status: :{status_color}[{job['status']}]")
                col2.write(f"Registros: {job['processed_ceps']} / {job['total_ceps']}")
                
                # Lógica dos botões de ação
                if job['status'] == 'PENDENTE':
                    if col3.button("▶️ Processar", key=f"run_{job['id']}", use_container_width=True):
                        asyncio.run(processar_job(job['id'], progress_bar_placeholder))
                        st.success(f"Job {job['id']} concluído!")
                        st.rerun() # Atualiza a tela para mostrar o novo status

                elif job['status'] == 'CONCLUIDO':
                    df_results = db.get_job_results_as_df(job['id'])
                    excel_bytes = to_excel_bytes(df_results)
                    col3.download_button(
                        label="⬇️ Baixar Resultados (.xlsx)",
                        data=excel_bytes,
                        file_name=f"RESULTADO_JOB_{job['id']}.xlsx",
                        key=f"download_{job['id']}",
                        use_container_width=True
                    )
                    with col4.expander("Ver amostra dos resultados"):
                        st.dataframe(df_results.head(10))
                
                elif job['status'] == 'FALHOU':
                    col3.error("Job Falhou. Verifique os logs.")

# --- ABA DE CONSULTA INDIVIDUAL (Sem grandes alterações, só para manter a funcionalidade) ---
with tab_individual:
    st.subheader("Consulta Unitária por CEP")
    # ... O código desta aba pode permanecer o mesmo que você já tinha ...
    # ... Apenas garanta que ele não interfira com o resto ...
    cep_input = st.text_input("Digite o CEP:", max_chars=8)
    if st.button("Consultar CEP"):
        # Sua lógica de consulta individual aqui
        st.info("Funcionalidade de consulta individual mantida.")
