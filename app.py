# app.py (versão final com correções de robustez e exibição)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
from cachetools import TTLCache
import requests
import unicodedata # <<-- Importado para remover acentos

# --- CONFIGURAÇÃO GLOBAL (sem alterações) ---
CONCURRENCY_LIMIT = 50; MAX_RETRIES = 5; REQUEST_TIMEOUT = 20
BRASILAPI_CEP_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
BRASILAPI_CEP_V1_URL = "https://brasilapi.com.br/api/cep/v1/{uf}/{cidade}/{logradouro}"
cep_cache = TTLCache(maxsize=20_000, ttl=86400)

# --- FUNÇÕES DE BACKEND ---

def remover_acentos(texto: str) -> str:
    """Normaliza o texto, removendo acentuações para ser compatível com a API."""
    nfkd_form = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

# <<--- CORREÇÃO 1: Função de busca por endereço agora sanitiza os inputs --->>
def busca_por_endereco(uf: str, cidade: str, logradouro: str):
    if len(logradouro) < 3: return "LOGRADOURO_CURTO"
    try:
        # Sanitiza os inputs para remover acentos e espaços extras
        cidade_sanitizada = remover_acentos(cidade.strip())
        logradouro_sanitizado = remover_acentos(logradouro.strip())
        url = BRASILAPI_CEP_V1_URL.format(uf=uf.upper(), cidade=cidade_sanitizada, logradouro=logradouro_sanitizado)
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        return response.json() if response.status_code == 200 else []
    except requests.exceptions.RequestException: return []

# (Todas as outras funções de backend, como consulta_cep_individual e as de lote, permanecem as mesmas)
def consulta_cep_individual(cep: str): # ...
    try:
        response = requests.get(BRASILAPI_CEP_V2_URL.format(cep=cep), timeout=REQUEST_TIMEOUT)
        return response.json() if response.status_code == 200 else None
    except: return None
async def fetch_cep_data(cep: str) -> dict: # ...
    if cep in cep_cache: return cep_cache[cep]
    if not cep or not cep.isdigit() or len(cep) != 8: return {"status_consulta": "Formato Inválido"}
    last_error_message = f"Falha em {MAX_RETRIES} tentativas."
    async with httpx.AsyncClient() as client:
        for attempt in range(MAX_RETRIES):
            try:
                response = await client.get(BRASILAPI_CEP_V2_URL.format(cep=cep), timeout=REQUEST_TIMEOUT)
                if response.status_code == 200:
                    data = response.json(); result = {"estado": data.get("state"), "cidade": data.get("city"), "bairro": data.get("neighborhood"), "logradouro": data.get("street"), "status_consulta": "Sucesso"}
                    cep_cache[cep] = result; return result
                elif response.status_code == 404: return {"status_consulta": "Não Encontrado"}
                else: last_error_message = f"Erro HTTP {response.status_code}"
                if attempt < MAX_RETRIES - 1: await asyncio.sleep(1)
            except (httpx.RequestError, httpx.TimeoutException) as e: last_error_message = e.__class__.__name__
    return {"status_consulta": f"Falha ({last_error_message})"}
async def processar_dataframe(df, cep_column_name): # ...
    df['cep_padronizado'] = df[cep_column_name].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT); tasks=[]
    async def run_fetch(cep):
        async with semaphore: return await fetch_cep_data(cep)
    tasks = [run_fetch(cep) for cep in df['cep_padronizado'].tolist()]
    results = []
    placeholder = st.empty()
    for i, f in enumerate(asyncio.as_completed(tasks)):
        results.append(await f)
        placeholder.text(f"Progresso: {i + 1} de {len(tasks)} CEPs consultados...")
    placeholder.success(f"Processamento concluído!")
    cep_to_result_map = dict(zip(df['cep_padronizado'], results))
    ordered_results = [cep_to_result_map[cep] for cep in df['cep_padronizado']]
    return pd.concat([df.drop(columns=['cep_padronizado']), pd.DataFrame(ordered_results)], axis=1)
def to_excel_bytes(df): # ...
    output = BytesIO();
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados')
    return output.getvalue()

# --- INTERFACE GRÁFICA UNIFICADA ---
st.set_page_config(page_title="Serviços CEP - Capital Consig", layout="wide")

with st.sidebar:
    st.image("logo.png", use_container_width=True)
    st.title("Capital Consig")
    st.info("Portal de Serviços de CEP. Selecione uma das abas para iniciar.")

st.header("Portal de Serviços de CEP")

tab1, tab2, tab3 = st.tabs(["Consulta por CEP", "Buscar CEP por Endereço", "Consulta em Lote"])

# --- LÓGICA DA ABA 1: CONSULTA INDIVIDUAL ---
with tab1:
    st.subheader("Consulta Rápida por CEP")
    cep_input = st.text_input("Digite o CEP (apenas números):", max_chars=8, key="cep_individual")
    if st.button("Consultar CEP"):
        if cep_input and len(cep_input) == 8 and cep_input.isdigit():
            with st.spinner("Buscando..."):
                resultado = consulta_cep_individual(cep_input)
                # <<--- CORREÇÃO 2: Exibição profissional do resultado --->>
                if resultado:
                    st.success("CEP Encontrado!")
                    st.markdown(f"""
                        - **CEP:** `{resultado.get('cep', 'N/A')}`
                        - **Logradouro:** {resultado.get('street', 'N/A')}
                        - **Bairro:** {resultado.get('neighborhood', 'N/A')}
                        - **Cidade:** {resultado.get('city', 'N/A')}
                        - **Estado:** {resultado.get('state', 'N/A')}
                    """)
                else:
                    st.error("CEP não encontrado ou inválido.")
        else:
            st.warning("Por favor, digite um CEP válido com 8 dígitos.")

# --- LÓGICA DA ABA 2: BUSCAR CEP POR ENDEREÇO ---
with tab2:
    st.subheader("Buscar CEP por Endereço")
    st.write("Preencha os campos para encontrar o CEP (rua deve ter no mínimo 3 caracteres).")
    estados = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
    col1, col2, col3 = st.columns(3)
    with col1: uf_input = st.selectbox("Estado (UF):", estados, key="uf_busca")
    with col2: cidade_input = st.text_input("Cidade:", key="cidade_busca")
    with col3: logradouro_input = st.text_input("Nome da Rua (Logradouro):", key="rua_busca")
    if st.button("Buscar Endereço"):
        if uf_input and cidade_input and logradouro_input:
            with st.spinner("Buscando endereços..."):
                resultados_busca = busca_por_endereco(uf_input, cidade_input, logradouro_input)
                # <<--- CORREÇÃO 2: Exibição profissional dos resultados --->>
                if resultados_busca == "LOGRADOURO_CURTO":
                    st.warning("O nome da rua deve ter pelo menos 3 caracteres.")
                elif resultados_busca:
                    st.success(f"Encontrados {len(resultados_busca)} resultados para '{logradouro_input}':")
                    for end in resultados_busca:
                        with st.container():
                            st.markdown(f"""
                                **CEP:** `{end.get('cep', 'N/A')}` | **Logradouro:** {end.get('street', 'N/A')} | **Bairro:** {end.get('neighborhood', 'N/A')}
                            """)
                            st.divider()
                else:
                    st.error("Nenhum endereço encontrado para os dados informados.")
        else:
            st.warning("Por favor, preencha todos os campos.")

# --- LÓGICA DA ABA 3: CONSULTA EM LOTE ---
with tab3: # (Nenhuma mudança de lógica necessária aqui, já era robusto)
    st.subheader("Consulta de Múltiplos CEPs em Lote")
    st.markdown("Carregue sua planilha (Excel ou CSV) para processar todos os CEPs de uma só vez.")
    uploaded_file = st.file_uploader(
        "Selecione o arquivo", type=["xlsx", "csv"], label_visibility="collapsed", key="lote_uploader"
    )
    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str) if uploaded_file.name.lower().endswith('.xlsx') else pd.read_csv(uploaded_file, dtype=str)
            cep_col_name = next((col for col in df.columns if 'cep' in str(col).lower()), None)
            if cep_col_name is None: st.error("ERRO: Nenhuma coluna com 'CEP' encontrada.")
            else:
                st.success(f"Arquivo '{uploaded_file.name}' carregado. {len(df)} registros para processar.")
                if st.button(f"Processar {len(df)} Registros", use_container_width=True):
                    with st.spinner("Executando consultas..."): df_final = asyncio.run(processar_dataframe(df, cep_col_name))
                    st.subheader("Processamento Concluído")
                    st.dataframe(df_final, use_container_width=True)
                    excel_data = to_excel_bytes(df_final)
                    st.download_button(
                        "Baixar Planilha Processada", excel_data, f"{uploaded_file.name.split('.')[0]}_PROCESSADO.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        except Exception as e: st.error(f"Ocorreu um erro inesperado: {e}")
