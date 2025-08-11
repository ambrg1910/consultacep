# database.py (Versão final e robusta)
import sqlite3
import pandas as pd
from pathlib import Path

# Mantemos a definição do caminho aqui
DB_PATH = Path("data/jobs.db")

def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    # Esta função agora assume que a pasta já existe.
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Inicializa o banco de dados e cria as tabelas se não existirem."""
    # <<< ESTA É A CORREÇÃO CRUCIAL >>>
    # Garante que o diretório pai ('data') do nosso arquivo de banco de dados exista.
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # O resto da função continua normalmente, agora com a certeza de que a pasta existe.
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_filename TEXT NOT NULL,
            saved_filepath TEXT NOT NULL,
            cep_col TEXT NOT NULL,
            prop_col TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDENTE',
            total_ceps INTEGER DEFAULT 0,
            processed_ceps INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            proposta TEXT,
            cep_original TEXT,
            endereco TEXT,
            bairro TEXT,
            cidade TEXT,
            estado TEXT,
            status_api TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        )
    ''')
    conn.commit()
    conn.close()

# Nenhuma outra função abaixo precisa ser alterada.
# Você pode copiar e colar esta função init_db() para dentro do seu
# arquivo existente, ou substituir o arquivo inteiro por este conteúdo
# para ter certeza.

def create_job(original_filename, saved_filepath, cep_col, prop_col, total_ceps):
    """Cria um novo job no banco de dados."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO jobs (original_filename, saved_filepath, cep_col, prop_col, total_ceps) VALUES (?, ?, ?, ?, ?)',
        (original_filename, saved_filepath, cep_col, prop_col, total_ceps)
    )
    job_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return job_id

def get_all_jobs():
    """Retorna todos os jobs do banco de dados."""
    conn = get_db_connection()
    jobs = conn.execute('SELECT * FROM jobs ORDER BY created_at DESC').fetchall()
    conn.close()
    return jobs

def get_job_by_id(job_id):
    """Busca um job específico pelo seu ID."""
    conn = get_db_connection()
    job = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
    conn.close()
    return job

def update_job_status(job_id, status, processed_ceps=None):
    """Atualiza o status e/ou o progresso de um job."""
    conn = get_db_connection()
    cursor = conn.cursor()
    if processed_ceps is not None:
        cursor.execute('UPDATE jobs SET status = ?, processed_ceps = ? WHERE id = ?', (status, processed_ceps, job_id))
    else:
        cursor.execute('UPDATE jobs SET status = ? WHERE id = ?', (status, job_id))
    
    if status in ['CONCLUIDO', 'FALHOU']:
        cursor.execute('UPDATE jobs SET finished_at = CURRENT_TIMESTAMP WHERE id = ?', (job_id,))
    
    conn.commit()
    conn.close()

def save_results_to_db(job_id, results_list):
    """Salva uma lista de resultados no banco de dados de forma eficiente."""
    conn = get_db_connection()
    cursor = conn.cursor()
    rows_to_insert = [
        (job_id, r.get('PROPOSTA'), r.get('CEP'), r.get('ENDEREÇO'), r.get('BAIRRO'), r.get('CIDADE'), r.get('ESTADO'), r.get('STATUS'))
        for r in results_list
    ]
    cursor.executemany('INSERT INTO results (job_id, proposta, cep_original, endereco, bairro, cidade, estado, status_api) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', rows_to_insert)
    conn.commit()
    conn.close()

def get_job_results_as_df(job_id):
    """Busca todos os resultados de um job e retorna como um DataFrame do Pandas."""
    conn = get_db_connection()
    df = pd.read_sql_query(f"SELECT proposta AS PROPOSTA, cep_original as CEP, endereco as ENDEREÇO, bairro as BAIRRO, cidade as CIDADE, estado as ESTADO, status_api as STATUS FROM results WHERE job_id = {job_id}", conn)
    conn.close()
    return df