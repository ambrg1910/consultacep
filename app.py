# app.py (Versão Portal de Serviços v3.0 - Definitiva)
import streamlit as st
import pandas as pd
import asyncio
import httpx
from io import BytesIO
from cachetools import TTLCache
import requests
import unicodedata

# --- CONFIGURAÇÃO GLOBAL ---
CONCURRENCY_LIMIT = 50; MAX_RETRIES = 5; REQUEST_TIMEOUT = 20
# URLs das APIs que vamos integrar
BRASILAPI_V2_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
BRASILAPI_V1_SEARCH_URL = "https://brasilapi.com.br/api/cep/v1/{uf}/{cidade}/{logradouro}"
cep_cache = TTLCache(maxsize=20_000, ttl=86400)
# -----------------------------

# --- FUNÇÕES AUXILIARES ---
def remover_acentos(texto: str) -> str:
    nfkd_form = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def display_result_card(resultado: dict, api_name: str, status: str = "Sucesso"):
    if status == "Sucesso":
        with st.container(border=True):
            st.subheader(f"Resultado {api_name}", anchor=False)
            col1, col2 = st.columns(2)
            col1.metric("CEP", resultado.get("cep", "N/A"))
            col2.metric("Logradouro", resultado.get("street") or resultado.get("logradouro", "N/A"))
            col1.metric("Bairro", resultado.get("neighborhood") or resultado.get("bairro", "N/A"))
            col2.metric("Cidade / UF", f"{resultado.get('city') or resultado.get('localidade', 'N/A')} - {resultado.get('state') or resultado.get('uf', 'N/A')}")
    else:
        with st.container(border=True):
            st.subheader(f"Resultado {api_name}", anchor=False)
            st.error(status)

# --- FUNÇÕES DE BACKEND (PARA CADA API) ---
def consulta_brasilapi(cep):
    try:
        response = requests.get(BRASILAPI_V2_URL.format(cep=cep), timeout=5)
        if response.status_code == 200: return response.json(), "Sucesso"
    except: pass
    return None, "Serviço indisponível ou CEP não encontrado"

def consulta_viacep(cep):
    try:
        response = requests.get(VIACEP_URL.format(cep=cep), timeout=5)
        if response.status_code == 200 and not response.json().get('erro'):
            return response.json(), "Sucesso"
    except: pass
    return None, "Serviço indisponível ou CEP não encontrado"

def busca_por_endereco_robusta(uf, cidade, logradouro):
    if len(logradouro) < 3: return "LOGRADOURO_CURTO"
    try:
        cidade_s = remover_acentos(cidade.strip())
        logradouro_s = remover_acentos(logradouro.strip())
        url = BRASILAPI_V1_SEARCH_URL.format(uf=uf, cidade=cidade_s, logradouro=logradouro_s)
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        return response.json() if response.status_code == 200 else []
    except: return []

# Backend para verificação de status das APIs
def check_api_status(api_name, url):
    try:
        start_time = asyncio.get_event_loop().time()
        response = requests.get(url.format(cep="01001000"), timeout=5)
        end_time = asyncio.get_event_loop().time()
        tempo = round((end_time - start_time) * 1000)
        return "Online", f"{tempo} ms" if response.status_code == 200 else "Offline", "N/A"
    except:
        return "Offline", "N/A"

# Backend robusto para consulta em lote (já validado)
async def fetch_cep_data(cep: str) -> dict: # (sem alterações de lógica interna)
    if cep in cep_cache: return cep_cache[cep]
    if not cep or not cep.isdigit() or len(cep) != 8: return {"status_consulta": "Formato Inválido"}
    last_error_message = f"Falha em {MAX_RETRIES} tentativas."
    async with httpx.AsyncClient() as client:
        for attempt in range(MAX_RETRIES):
            try:
                # Dando prioridade à BrasilAPI por ser mais completa
                response = await client.get(BRASILAPI_V2_URL.format(cep=cep), timeout=REQUEST_TIMEOUT)
                if response.status_code == 200:
                    data = response.json(); result = {"estado": data.get("state"), "cidade": data.get("city"), "bairro": data.get("neighborhood"), "logradouro": data.get("street"), "status_consulta": "Sucesso"}
                    cep_cache[cep] = result; return result
            except (httpx.RequestError, httpx.TimeoutException): pass
    return {"status_consulta": f"Falha"}
async def processar_dataframe(df: pd.DataFrame, cep_column_name: str): # ... (sem alterações)
    df['cep_padronizado'] = df[cep_column_name].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async def run_fetch(cep):
        async with semaphore: return await fetch_cep_data(cep)
    tasks = [run_fetch(cep) for cep in df['cep_padronizado'].tolist()]
    results = []; placeholder = st.empty()
    for i, f in enumerate(asyncio.as_completed(tasks)):
        results.append(await f); placeholder.text(f"Progresso: {i + 1} de {len(tasks)} consultados...")
    placeholder.success("Processamento concluído!")
    cep_to_result_map = dict(zip(df['cep_padronizado'], results))
    ordered_results = [cep_to_result_map[cep] for cep in df['cep_padronizado']]
    return pd.concat([df.drop(columns=['cep_padronizado']), pd.DataFrame(ordered_results)], axis=1)
def to_excel_bytes(df: pd.DataFrame): # ... (sem alterações)
    output = BytesIO();
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados')
    return output.getvalue()


# --- INTERFACE GRÁFICA PRINCIPAL ---
st.set_page_config(page_title="Serviços CEP - Capital Consig", layout="wide")

with st.sidebar:
    st.image("logo.png", use_container_width=True)
    st.title("Capital Consig")
    st.info("Portal de Serviços de CEP. Selecione uma funcionalidade nas abas ao lado.")

st.header("Portal de Serviços de CEP")

tab_individual, tab_busca_endereco, tab_lote, tab_status = st.tabs(["🔍 Consulta Individual", "🗺️ Buscar por Endereço", "📦 Consulta em Lote", "🚦 Status dos Serviços"])

with tab_individual:
    st.subheader("Consulta Rápida por CEP")
    cep_input = st.text_input("Digite o CEP (apenas números):", max_chars=8, key="cep_ind_input")
    if st.button("Consultar CEP", key="btn_cep_ind"):
        if cep_input and len(cep_input) == 8 and cep_input.isdigit():
            with st.spinner("Buscando..."):
                res_br, status_br = consulta_brasilapi(cep_input)
                res_via, status_via = consulta_viacep(cep_input)
            
            st.divider()
            st.subheader("Resultados:")
            col_br, col_via = st.columns(2)
            with col_br:
                display_result_card(res_br, "BrasilAPI", status_br)
            with col_via:
                display_result_card(res_via, "ViaCEP", status_via)
        else: st.warning("Por favor, digite um CEP válido com 8 dígitos.")

with tab_busca_endereco:
    st.subheader("Buscar CEP por Endereço")
    st.write("Preencha os campos para encontrar o CEP. A rua deve ter no mínimo 3 caracteres.")
    estados = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]
    c1, c2, c3 = st.columns(3)
    uf_sel = c1.selectbox("UF", estados)
    cidade_sel = c2.text_input("Cidade")
    rua_sel = c3.text_input("Logradouro")

    if st.button("Buscar Endereço", key="btn_busca_end"):
        if uf_sel and cidade_sel and rua_sel:
            with st.spinner("Buscando..."):
                resultados = busca_por_endereco_robusta(uf_sel, cidade_sel, rua_sel)
                if resultados == "LOGRADOURO_CURTO": st.warning("O nome da rua deve ter pelo menos 3 caracteres.")
                elif resultados:
                    st.success(f"{len(resultados)} resultado(s) encontrado(s).")
                    st.dataframe(pd.DataFrame(resultados), use_container_width=True)
                else: st.error("Nenhum endereço encontrado para os dados informados.")
        else: st.warning("Todos os campos são obrigatórios.")

with tab_lote: # Interface da Consulta em Lote
    st.subheader("Consulta de Múltiplos CEPs em Lote")
    st.markdown("Carregue sua planilha (Excel ou CSV). O sistema irá processar e devolver a planilha com os dados de endereço preenchidos.")
    uploaded_file = st.file_uploader("Selecione o arquivo", type=["xlsx", "csv"], label_visibility="collapsed", key="lote_up")
    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file, engine='openpyxl', dtype=str) if uploaded_file.name.lower().endswith('.xlsx') else pd.read_csv(uploaded_file, dtype=str)
            cep_col = next((col for col in df.columns if 'cep' in str(col).lower()), None)
            if not cep_col: st.error("ERRO: Nenhuma coluna com 'CEP' no nome foi encontrada.")
            else:
                st.success(f"Arquivo '{uploaded_file.name}' carregado: {len(df)} registros.")
                if st.button("Processar Planilha em Lote", use_container_width=True):
                    df_final = asyncio.run(processar_dataframe(df, cep_col))
                    st.subheader("Processamento Concluído")
                    st.dataframe(df_final, use_container_width=True)
                    st.download_button("Baixar Planilha Processada", to_excel_bytes(df_final), f"{uploaded_file.name.split('.')[0]}_PROCESSADO.xlsx", use_container_width=True)
        except Exception as e: st.error(f"Erro ao processar: {e}")

with tab_status:
    st.subheader("Status dos Serviços de Consulta")
    if st.button("Verificar Status Agora"):
        with st.spinner("Verificando..."):
            status_br, tempo_br = check_api_status("BrasilAPI", BRASILAPI_V2_URL)
            status_via, tempo_via = check_api_status("ViaCEP", VIACEP_URL)
        
        c1_stat, c2_stat = st.columns(2)
        with c1_stat:
            with st.container(border=True):
                st.metric(label="BrasilAPI", value=status_br, delta=tempo_br, delta_color="inverse")
        with c2_stat:
            with st.container(border=True):
                st.metric(label="ViaCEP", value=status_via, delta=tempo_via, delta_color="inverse")
