import streamlit as st
import pandas as pd
import requests
import time
import io
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

# --- Constantes e Configuração Inicial ---
BRASIL_API_URL = "https://brasilapi.com.br/api/cep/v2/{}"
VIACEP_API_URL = "https://viacep.com.br/ws/{}/json/"
MAX_WORKERS = 20  # Limite sensato para não sobrecarregar as APIs
REQUEST_TIMEOUT = 10  # Segundos para timeout das requisições
MAX_RETRIES = 2 # Tentativas para cada API antes de falhar

# --- Configuração da Página Streamlit ---
st.set_page_config(
    page_title="O Motor de Validação v12",
    page_icon="⚡",
    layout="wide"
)

# --- Funções de Lógica de Negócio ---

def find_columns(df_columns):
    """Identifica inteligentemente as colunas de PROPOSTA e CEP."""
    proposta_col = None
    cep_col = None
    for col in df_columns:
        if re.search("proposta", col, re.IGNORECASE):
            proposta_col = col
        if re.search("cep", col, re.IGNORECASE):
            cep_col = col
    return proposta_col, cep_col

def get_cep_data(cep, session):
    """
    Busca dados de um CEP com estratégia Primary/Fallback e retentativas.
    Essa função é o coração da resiliência.
    """
    clean_cep = re.sub(r'\D', '', str(cep))
    if len(clean_cep) != 8:
        return {'status': 'CEP Inválido'}

    # 1. Tentar BrasilAPI (Primary) com retentativas
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(BRASIL_API_URL.format(clean_cep), timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                return {
                    'endereco': data.get('street'),
                    'bairro': data.get('neighborhood'),
                    'cidade': data.get('city'),
                    'estado': data.get('state'),
                    'status': 'OK - BrasilAPI'
                }
        except requests.exceptions.RequestException:
            time.sleep(0.5) # Pausa antes de retentativa
            continue
    
    # 2. Tentar ViaCEP (Fallback) com retentativas
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(VIACEP_API_URL.format(clean_cep), timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                if not data.get('erro'):
                    return {
                        'endereco': data.get('logradouro'),
                        'bairro': data.get('bairro'),
                        'cidade': data.get('localidade'),
                        'estado': data.get('uf'),
                        'status': 'OK - ViaCEP'
                    }
        except requests.exceptions.RequestException:
            time.sleep(0.5) # Pausa antes de retentativa
            continue
    
    return {'status': 'Falha na Consulta'}


def process_job(job_df, cep_col, ui_placeholders):
    """
    Processa um único job (DataFrame) usando ThreadPoolExecutor.
    Atualiza os placeholders da UI em tempo real.
    """
    total_records = len(job_df)
    results = [None] * total_records
    records_processed = 0
    start_time = time.time()

    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_index = {
                executor.submit(get_cep_data, row[cep_col], session): index
                for index, row in job_df.iterrows()
            }

            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    results[index] = future.result()
                except Exception as e:
                    results[index] = {'status': f'Erro: {e}'}
                
                records_processed += 1
                
                # --- Atualização do Painel de Controle (Feedback em Tempo Real) ---
                if records_processed % 10 == 0 or records_processed == total_records: # Atualiza a cada 10 registros
                    elapsed_time = time.time() - start_time
                    speed = records_processed / elapsed_time if elapsed_time > 0 else 0
                    etc_seconds = (total_records - records_processed) / speed if speed > 0 else 0
                    
                    progress = records_processed / total_records
                    
                    with ui_placeholders["progress_bar"]:
                        st.progress(progress, text=f"Processando... {records_processed}/{total_records}")
                    
                    with ui_placeholders["metrics"]:
                        etc_str = str(timedelta(seconds=int(etc_seconds)))
                        st.metric(label="Velocidade Atual", value=f"{speed:.1f} reg/s")

                    with ui_placeholders["etc"]:
                        st.metric(label="Tempo Estimado de Conclusão", value=f"{etc_str}")

    return pd.DataFrame(results)

def to_excel(df):
    """Converte um DataFrame para um objeto BytesIO em formato Excel."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados')
    return output.getvalue()

# --- Gerenciamento de Estado da Aplicação (O segredo para a UI não congelar) ---
if 'jobs_queue' not in st.session_state:
    st.session_state.jobs_queue = []
if 'completed_jobs' not in st.session_state:
    st.session_state.completed_jobs = []
if 'is_processing' not in st.session_state:
    st.session_state.is_processing = False
if 'job_counter' not in st.session_state:
    st.session_state.job_counter = 0

# --- Interface do Usuário (UI) ---

st.title("🚀 O Motor de Validação da Capital Consig v12")
st.markdown("A ferramenta definitiva para validação de CEPs em massa. *Confiabilidade e performance para o operador.*")

# --- Seção de Upload de Jobs ---
st.header("1. Adicionar Novo Job à Fila")

uploaded_file = st.file_uploader(
    "Arraste e solte um arquivo Excel (.xlsx) aqui",
    type="xlsx",
    disabled=st.session_state.is_processing
)

if uploaded_file is not None:
    try:
        df = pd.read_excel(uploaded_file)
        proposta_col, cep_col = find_columns(df.columns)
        
        if not proposta_col or not cep_col:
            st.error(f"Erro: Não foi possível encontrar as colunas 'PROPOSTA' e 'CEP' no arquivo. Colunas encontradas: {', '.join(df.columns)}")
        else:
            st.session_state.job_counter += 1
            job_id = f"Job #{st.session_state.job_counter} - {uploaded_file.name}"
            
            # Adiciona na fila global, mas evita duplicatas se a página recarregar
            if not any(j['id'] == job_id for j in st.session_state.jobs_queue):
                st.session_state.jobs_queue.append({
                    "id": job_id,
                    "df": df,
                    "proposta_col": proposta_col,
                    "cep_col": cep_col,
                    "status": "Pendente",
                    "original_df": df.copy() # Guarda o original
                })
                st.success(f"✅ Job '{job_id}' ({len(df)} registros) adicionado à fila.")

    except Exception as e:
        st.error(f"Ocorreu um erro ao ler o arquivo: {e}")

st.divider()

# --- Seção da Fila e Processamento ---
st.header("2. Fila de Processamento e Controle")

# Painel de Controle do Job ATIVO (aparecerá quando o processamento iniciar)
if st.session_state.is_processing:
    st.subheader("Painel de Controle do Job Ativo")
    st.info(f"Processando: **{st.session_state.jobs_queue[0]['id']}**")
    
    # Placeholders para as atualizações em tempo real
    progress_bar_placeholder = st.empty()
    cols = st.columns(2)
    metrics_placeholder = cols[0]
    etc_placeholder = cols[1]
else:
    # Cria placeholders vazios para evitar erro quando o botão for clicado
    progress_bar_placeholder = st.empty()
    metrics_placeholder = st.empty()
    etc_placeholder = st.empty()

# Botão de Iniciar Processamento
if st.session_state.jobs_queue and not st.session_state.is_processing:
    if st.button("▶️ INICIAR PROCESSAMENTO DA FILA", type="primary", use_container_width=True):
        st.session_state.is_processing = True
        
        # A MÁGICA ACONTECE AQUI:
        # A função é chamada uma vez. Ela vai iterar por toda a fila.
        # Os placeholders da UI são passados para a função para serem atualizados por ela.
        # A UI do Streamlit não fica bloqueada esperando o fim.
        
        queue_copy = list(st.session_state.jobs_queue)
        for job in queue_copy:
            job_start_time = time.time()
            job['status'] = 'Processando'
            
            # Passa os placeholders para a função de processamento
            result_df = process_job(
                job_df=job['original_df'], 
                cep_col=job['cep_col'],
                ui_placeholders={
                    "progress_bar": progress_bar_placeholder,
                    "metrics": metrics_placeholder,
                    "etc": etc_placeholder
                }
            )

            # Enriquecer o DataFrame original
            final_df = job['original_df'].copy()
            final_df[['ENDEREÇO', 'BAIRRO', 'CIDADE', 'ESTADO', 'STATUS']] = result_df

            # Limpa os placeholders para o próximo job
            progress_bar_placeholder.empty()
            metrics_placeholder.empty()
            etc_placeholder.empty()
            
            job_processing_time = time.time() - job_start_time
            
            # Move o job da fila para a lista de concluídos
            job_concluido = {
                'id': job['id'],
                'df_result': final_df,
                'record_count': len(final_df),
                'processing_time': job_processing_time
            }
            st.session_state.completed_jobs.insert(0, job_concluido) # Insere no início
            st.session_state.jobs_queue.pop(0)
            
        st.session_state.is_processing = False
        st.success("🎉 Todos os jobs na fila foram processados!")
        st.rerun() # Força um recarregamento final para limpar a UI

# Visualização da Fila
if st.session_state.jobs_queue:
    st.subheader("Jobs na Fila:")
    for job in st.session_state.jobs_queue:
        st.text(f"➡️ {job['id']} - Status: {job['status']}")
else:
    st.info("A fila de processamento está vazia.")

st.divider()

# --- Seção de Resultados ---
st.header("3. Jobs Concluídos")

if st.session_state.completed_jobs:
    for job in st.session_state.completed_jobs:
        with st.expander(f"**{job['id']}** - {job['record_count']} registros processados em {job['processing_time']:.2f} segundos"):
            st.dataframe(job['df_result'].head())
            st.download_button(
                label=f"⬇️ Exportar {job['id']}",
                data=to_excel(job['df_result']),
                file_name=f"resultado_{re.sub('[^a-zA-Z0-9]', '_', job['id'])}.xlsx",
                mime="application/vnd.ms-excel",
                key=f"download_{job['id']}" # Chave única para cada botão
            )
else:
    st.info("Nenhum job foi concluído ainda.")