# database.py
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any

DB_PATH = Path("data/jobs.db")

def get_db_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, original_filename TEXT, saved_filepath TEXT, cep_col TEXT, prop_col TEXT, status TEXT DEFAULT "PENDENTE", total_ceps INTEGER DEFAULT 0, processed_ceps INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, finished_at TIMESTAMP)')
        cursor.execute('CREATE TABLE IF NOT EXISTS results (id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL, proposta TEXT, cep_original TEXT, endereco TEXT, bairro TEXT, cidade TEXT, estado TEXT, status_api TEXT, FOREIGN KEY(job_id) REFERENCES jobs(id))')
        conn.commit()

def create_job(filename: str, filepath: str, cep_col: str, prop_col: str, total: int) -> int:
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT INTO jobs (original_filename, saved_filepath, cep_col, prop_col, total_ceps) VALUES (?, ?, ?, ?, ?)', (filename, filepath, cep_col, prop_col, total))
        return cursor.lastrowid

def get_all_jobs() -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute('SELECT * FROM jobs ORDER BY id DESC').fetchall()]

def get_job_by_id(job_id: int) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        job = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
        return dict(job) if job else None

def get_active_job() -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        job = conn.execute("SELECT * FROM jobs WHERE status = 'PROCESSANDO' LIMIT 1").fetchone()
        return dict(job) if job else None

def get_next_pending_job() -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        job = conn.execute("SELECT * FROM jobs WHERE status = 'PENDENTE' ORDER BY id ASC LIMIT 1").fetchone()
        return dict(job) if job else None

def update_job_status(job_id: int, status: str, processed: Optional[int] = None):
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

def save_results_to_db(job_id: int, results: List[Dict[str, Any]]):
    rows = [(job_id, r.get('PROPOSTA'), r.get('CEP'), r.get('ENDEREÇO'), r.get('BAIRRO'), r.get('CIDADE'), r.get('ESTADO'), r.get('STATUS')) for r in results]
    with get_db_connection() as conn:
        conn.executemany('INSERT INTO results (job_id, proposta, cep_original, endereco, bairro, cidade, estado, status_api) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', rows)

def get_job_results_as_df(job_id: int) -> pd.DataFrame:
    with get_db_connection() as conn:
        return pd.read_sql_query(f"SELECT proposta AS PROPOSTA, cep_original as CEP, endereco as ENDEREÇO, bairro as BAIRRO, cidade as CIDADE, estado as ESTADO, status_api as STATUS FROM results WHERE job_id = {job_id}", conn)