# app.py (versão com comunicação profissional e corporativa)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
from cachetools import TTLCache

# --- CONFIGURAÇÃO DA APLICAÇÃO (sem alterações) ---
CONCURRENCY_LIMIT = 50
MAX_RETRIES = 5
RETRY_DELAY = 1
HTTP_429_DELAY = 3
REQUEST_TIMEOUT = 20
BRASILAPI_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
cep_cache = TTLCache(maxsize=10_000, ttl=86400)
# ----------------------------------------------------

async def fetch_cep_data(cep: str) -> dict: # (sem alterações)
    if cep in cep_cache: return cep_cache[cep]
    if not cep or not cep.isdigit() or len(cep) != 8: return {"status_consulta": "CEP Inválido"}
    last_error_message = f"Falha em {MAX_RETRIES} tentativas."
    async with httpx.AsyncClient() as client:
        for attempt in range(MAX_RETRIES):
            try:
                response = await client.get(BRASILAPI_URL.format(cep=cep), timeout=REQUEST_TIMEOUT)
                if response.status_code == 200:
                    data = response.json(); result = {"estado": data.get("state"), "cidade": data.get("city"), "bairro": data.get("neighborhood"), "logradouro": data.get("street"), "status_consulta": "Sucesso"}
                    cep_cache[cep] = result; return result
                elif response.status_code == 404: return {"status_consulta": "Não Encontrado"}
                elif response.status_code == 429: last_error_message = "API Rate Limit (429)"
                else: last_error_message = f"Erro HTTP {response.status_code}"
                if attempt < MAX_RETRIES - 1: await asyncio.sleep(HTTP_429_DELAY if response.status_code == 429 else RETRY_DELAY)
            except (httpx.RequestError, httpx.TimeoutException) as e: last_error_message = str(e.__class__.__name__)
    return {"status_consulta": f"Falha: {last_error_message}"}

async def processar_dataframe(df: pd.DataFrame, cep_column_name: str) -> pd.DataFrame: # (sem alterações)
    df['cep_padronizado'] = df[cep_column_name].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async def run_fetch(cep):
        async with semaphore: return await fetch_cep_data(cep)
    tasks = [run_fetch(cep) for cep in df['cep_padronizado'].tolist()]
    progress_bar = st.progress(0, text="Processando... Por favor, aguarde.")
    results = []
    for i, f in enumerate(asyncio.as_completed(tasks)):
        results.append(await f)
        progress_bar.progress((i + 1) / len(tasks), text=f"Processando... {i+1} de {len(tasks)}")
    progress_bar.empty()
    cep_to_result_map = dict(zip(df['cep_padronizado'], results))
    ordered_results = [cep_to_result_map[cep] for cep in df['cep_padronizado']]
    return pd.concat([df.drop(columns=['cep_padronizado']), pd.DataFrame(ordered_results)], axis=1)

def to_excel_bytes(df: pd.DataFrame) -> bytes: # (sem alterações)
    output = BytesIO();
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados')
    return output.getvalue()

# --- INTERFACE GRÁFICA PROFISSIONAL ---
st.set_page_config(page_title="Consulta de CEPs em Lote", layout="wide") # 'wide' para layout mais espaçoso
st.title("Sistema de Consulta de CEPs em Lote")
st.markdown("Carregue sua planilha (Excel ou CSV) para validar e enriquecer os dados de CEP.")
st.markdown("---")

uploaded_file = st.file_uploader(
    "Selecione o Arquivo de CEPs",
    type=["xlsx", "csv"]
)

if uploaded_file is not None:
    try:
        df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str) if uploaded_file.name.lower().endswith('.xlsx') else pd.read_csv(uploaded_file, dtype=str)
        cep_col_name = next((col for col in df.columns if 'cep' in str(col).lower()), None)
        
        if cep_col_name is None:
            st.error("ERRO: Nenhuma coluna contendo 'CEP' foi encontrada na planilha. Verifique o cabeçalho do arquivo e tente novamente.")
        else:
            st.success(f"Arquivo '{uploaded_file.name}' carregado. Coluna de CEP identificada: '{cep_col_name}'.")
            st.info(f"Total de registros encontrados na planilha: {len(df)}.")
            
            if st.button("Validar e Processar CEPs"):
                with st.spinner('Executando consultas...'): # Usando um spinner para uma animação mais sóbria
                    df_final = asyncio.run(processar_dataframe(df, cep_col_name))

                st.header("Resultados do Processamento")
                
                total = len(df_final); sucessos = (df_final['status_consulta'] == 'Sucesso').sum()
                nao_encontrados = (df_final['status_consulta'] == 'Não Encontrado').sum()
                erros = total - sucessos - nao_encontrados
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Total de Registros", total, help="Número total de linhas na planilha enviada.")
                col2.metric("Consultas com Sucesso", sucessos, help="CEPs válidos e encontrados.")
                col3.metric("Falhas de Consulta", erros, help="CEPs que não puderam ser processados após múltiplas tentativas.")

                st.dataframe(df_final)

                excel_data = to_excel_bytes(df_final)

                st.download_button(
                    label="Baixar Planilha Processada",
                    data=excel_data,
                    file_name=f"{uploaded_file.name.split('.')[0]}_PROCESSADO.xlsx", # Nome do arquivo de saída alterado
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    except Exception as e:
        st.error(f"Ocorreu um erro inesperado ao processar o arquivo. Detalhe: {e}")
