# app.py (versão com a correção final de leitura)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
from cachetools import TTLCache

# --- CONFIGURAÇÃO DA APLICAÇÃO ---
CONCURRENCY_LIMIT = 50
MAX_RETRIES = 5
RETRY_DELAY = 1
HTTP_429_DELAY = 3
REQUEST_TIMEOUT = 20
BRASILAPI_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
cep_cache = TTLCache(maxsize=10_000, ttl=86400)
# -----------------------------------

async def fetch_cep_data(cep: str) -> dict:
    if cep in cep_cache: return cep_cache[cep]
    if not cep or not cep.isdigit() or len(cep) != 8: return {"status_consulta": "CEP_INVALIDO_FORMATO"}
    last_error_message = f"Falha após {MAX_RETRIES} tentativas."
    async with httpx.AsyncClient() as client:
        for attempt in range(MAX_RETRIES):
            try:
                response = await client.get(BRASILAPI_URL.format(cep=cep), timeout=REQUEST_TIMEOUT)
                if response.status_code == 200:
                    data = response.json()
                    result = {"estado": data.get("state"), "cidade": data.get("city"), "bairro": data.get("neighborhood"), "logradouro": data.get("street"), "status_consulta": "VALIDADO"}
                    cep_cache[cep] = result; return result
                elif response.status_code == 404: return {"status_consulta": "NAO_ENCONTRADO"}
                elif response.status_code == 429:
                    last_error_message = "API Rate Limit (429)"
                    if attempt < MAX_RETRIES - 1: await asyncio.sleep(HTTP_429_DELAY)
                else:
                    last_error_message = f"Erro HTTP {response.status_code}"
                    if attempt < MAX_RETRIES - 1: await asyncio.sleep(RETRY_DELAY)
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_error_message = str(e.__class__.__name__)
                if attempt < MAX_RETRIES - 1: await asyncio.sleep(RETRY_DELAY)
    return {"status_consulta": f"ERRO: {last_error_message}"}

async def processar_dataframe(df: pd.DataFrame, cep_column_name: str) -> pd.DataFrame:
    df['cep_padronizado'] = df[cep_column_name].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async def run_fetch(cep):
        async with semaphore: return await fetch_cep_data(cep)
    tasks = [run_fetch(cep) for cep in df['cep_padronizado'].tolist()]
    progress_bar = st.progress(0, text="Consultando CEPs... Por favor, aguarde.")
    results = []
    for i, f in enumerate(asyncio.as_completed(tasks)):
        results.append(await f)
        progress_bar.progress((i + 1) / len(tasks), text=f"Consultando CEPs... {i+1}/{len(tasks)}")
    progress_bar.empty()
    cep_to_result_map = dict(zip(df['cep_padronizado'], results))
    ordered_results = [cep_to_result_map[cep] for cep in df['cep_padronizado']]
    df_results = pd.DataFrame(ordered_results)
    return pd.concat([df.drop(columns=['cep_padronizado']), df_results], axis=1)

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados')
    return output.getvalue()

# --- Interface Gráfica da Aplicação ---
st.set_page_config(page_title="Consulta de CEPs em Lote", layout="centered")
st.title("🚀 Ferramenta Profissional de Consulta de CEPs")
st.write("Suba sua planilha, e nós a devolveremos com os dados de endereço preenchidos.")
st.markdown("---")
uploaded_file = st.file_uploader("1. Selecione sua planilha (Excel ou CSV)", type=["xlsx", "csv"])
if uploaded_file is not None:
    try:
        # <<--- LÓGICA DE LEITURA CORRIGIDA AQUI --->>
        if uploaded_file.name.lower().endswith('.csv'):
            # Manual de instruções para ler a "lista de compras" (CSV)
            df = pd.read_csv(uploaded_file, dtype=str)
        else:
            # Manual de instruções para ler o "relatório complexo" (Excel)
            df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str)
        # <<-------------------------------------->>

        cep_col_name = next((col for col in df.columns if 'cep' in str(col).lower()), None)
        if cep_col_name is None:
            st.error("Erro: Nenhuma coluna com 'CEP' no nome foi encontrada.")
        else:
            st.success(f"Planilha '{uploaded_file.name}' lida! Coluna de CEP: '{cep_col_name}'.")
            st.info(f"A planilha contém {len(df)} linhas para processar.")
            if st.button("2. ✨ Iniciar Processamento"):
                df_final = asyncio.run(processar_dataframe(df, cep_col_name))
                st.balloons()
                st.header("Resultados")
                total = len(df_final); sucessos = (df_final['status_consulta'] == 'VALIDADO').sum()
                erros = total - sucessos - (df_final['status_consulta'] == 'NAO_ENCONTRADO').sum()
                col1, col2, col3 = st.columns(3)
                col1.metric("Total de CEPs", total); col2.metric("✅ Sucessos", sucessos); col3.metric("❌ Erros", erros)
                st.dataframe(df_final)
                excel_data = to_excel_bytes(df_final)
                st.download_button(
                    label="📥 Baixar Planilha com Resultados", data=excel_data,
                    file_name=f"{uploaded_file.name.split('.')[0]}_RESULTADO.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    except Exception as e:
        st.error(f"Ocorreu um erro ao ler ou processar o arquivo: {e}")
