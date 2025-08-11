# database.py
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any

DB_PATH = Path("data/jobs.db")

def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    """Garante que a pasta 'data' existe e inicializa o schema do DB."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, original_filename TEXT, saved_filepath TEXT, cep_col TEXT, prop_col TEXT, status TEXT DEFAULT "PENDENTE", total_ceps INTEGER DEFAULT 0, processed_ceps INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, finished_at TIMESTAMP)')
        cursor.execute('CREATE TABLE IF NOT EXISTS results (id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL, proposta TEXT, cep_original TEXT, endereco TEXT, bairro TEXT, cidade TEXT, estado TEXT, status_api TEXT, FOREIGN KEY(job_id) REFERENCES jobs(id))')
        conn.commit()

def create_job(filename: str, filepath: str, cep_col: str, prop_col: str, total: int) -> int:
    """Adiciona um novo job na fila."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT INTO jobs (original_filename, saved_filepath, cep_col, prop_col, total_ceps) VALUES (?, ?, ?, ?, ?)', (filename, filepath, cep_col, prop_col, total))
        return cursor.lastrowid

def get_all_jobs() -> List[Dict[str, Any]]:
    """Busca todos os jobs, do mais recente para o mais antigo."""
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute('SELECT * FROM jobs ORDER BY id DESC').fetchall()]

def get_job_by_id(job_id: int) -> Optional[Dict[str, Any]]:
    """Busca um job pelo seu ID."""
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        job = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
        return dict(job) if job else None

def get_active_job() -> Optional[Dict[str, Any]]:
    """Verifica se existe um job com o status 'PROCESSANDO'."""
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        job = conn.execute("SELECT * FROM jobs WHERE status = 'PROCESSANDO' LIMIT 1").fetchone()
        return dict(job) if job else None

def get_next_pending_job() -> Optional[Dict[str, Any]]:
    """Pega o próximo job da fila que está com status 'PENDENTE'."""
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        job = conn.execute("SELECT * FROM jobs WHERE status = 'PENDENTE' ORDER BY id ASC LIMIT 1").fetchone()
        return dict(job) if job else None

def update_job_status(job_id: int, status: str, processed: Optional[int] = None):
    """Atualiza o status e, opcionalmente, o número de CEPs processados."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        query = 'UPDATE jobs SET status = ?'
        params = [status]
        if processed is not None:
            query += ', processed_ceps = ?'; params.append(processed)
        if status in ['CONCLUIDO', 'FALHOU']:
            query += ', finished_at = CURRENT_TIMESTAMP'
        query += ' WHERE id = ?'; params.append(job_id)
        cursor.execute(query, tuple(params))
        conn.commit()

def save_results_to_db(job_id: int, results: List[Dict[str, Any]]):
    """Salva um lote de resultados no banco de dados."""
    rows = [(job_id, r.get('PROPOSTA'), r.get('CEP'), r.get('ENDEREÇO'), r.get('BAIRRO'), r.get('CIDADE'), r.get('ESTADO'), r.get('STATUS')) for r in results]
    with get_db_connection() as conn:
        conn.executemany('INSERT INTO results (job_id, proposta, cep_original, endereco, bairro, cidade, estado, status_api) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', rows)
        conn.commit()

def get_job_results_as_df(job_id: int) -> pd.DataFrame:
    """Exporta os resultados de um job para um DataFrame do Pandas."""
    with get_db_connection() as conn:
        return pd.read_sql_query(f"SELECT proposta AS PROPOSTA, cep_original as CEP, endereco as ENDEREÇO, bairro as BAIRRO, cidade as CIDADE, estado as ESTADO, status_api as STATUS FROM results WHERE job_id = {job_id}", conn)