# database.py
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any

DB_PATH = Path("data/jobs.db")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False); conn.row_factory = sqlite3.Row
    return conn

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True); conn = get_db_connection(); cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, original_filename TEXT NOT NULL, saved_filepath TEXT NOT NULL, cep_col TEXT NOT NULL, prop_col TEXT NOT NULL, status TEXT DEFAULT "PENDENTE", total_ceps INTEGER DEFAULT 0, processed_ceps INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, finished_at TIMESTAMP)')
    cursor.execute('CREATE TABLE IF NOT EXISTS results (id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL, proposta TEXT, cep_original TEXT, endereco TEXT, bairro TEXT, cidade TEXT, estado TEXT, status_api TEXT, FOREIGN KEY(job_id) REFERENCES jobs(id))')
    conn.commit(); conn.close()

def create_job(filename, filepath, cep_col, prop_col, total):
    conn = get_db_connection(); cursor = conn.cursor()
    cursor.execute('INSERT INTO jobs (original_filename, saved_filepath, cep_col, prop_col, total_ceps) VALUES (?, ?, ?, ?, ?)', (filename, filepath, cep_col, prop_col, total))
    job_id = cursor.lastrowid; conn.commit(); conn.close()
    return job_id

def get_all_jobs():
    conn = get_db_connection(); jobs = conn.execute('SELECT * FROM jobs ORDER BY id DESC').fetchall()
    conn.close()
    return [dict(row) for row in jobs]

def get_job_by_id(job_id) -> Optional[Dict[str, Any]]:
    conn = get_db_connection(); job = conn.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
    conn.close()
    return dict(job) if job else None

def get_active_job() -> Optional[Dict[str, Any]]:
    conn = get_db_connection(); job = conn.execute("SELECT * FROM jobs WHERE status = 'PROCESSANDO' LIMIT 1").fetchone()
    conn.close()
    return dict(job) if job else None

def get_next_pending_job() -> Optional[Dict[str, Any]]:
    conn = get_db_connection(); job = conn.execute("SELECT * FROM jobs WHERE status = 'PENDENTE' ORDER BY id ASC LIMIT 1").fetchone()
    conn.close()
    return dict(job) if job else None

def update_job_status(job_id, status, processed=None):
    conn = get_db_connection(); cursor = conn.cursor()
    query = 'UPDATE jobs SET status = ?'
    params = [status]
    if processed is not None:
        query += ', processed_ceps = ?'; params.append(processed)
    if status in ['CONCLUIDO', 'FALHOU', 'CANCELADO']:
        query += ', finished_at = CURRENT_TIMESTAMP'
    query += ' WHERE id = ?'; params.append(job_id)
    cursor.execute(query, tuple(params)); conn.commit(); conn.close()

def save_results_to_db(job_id, results):
    conn = get_db_connection(); cursor = conn.cursor()
    rows = [(job_id, r.get('PROPOSTA'), r.get('CEP'), r.get('ENDEREÇO'), r.get('BAIRRO'), r.get('CIDADE'), r.get('ESTADO'), r.get('STATUS')) for r in results]
    cursor.executemany('INSERT INTO results (job_id, proposta, cep_original, endereco, bairro, cidade, estado, status_api) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', rows)
    conn.commit(); conn.close()

def get_job_results_as_df(job_id):
    conn = get_db_connection(); df = pd.read_sql_query(f"SELECT proposta AS PROPOSTA, cep_original as CEP, endereco as ENDEREÇO, bairro as BAIRRO, cidade as CIDADE, estado as ESTADO, status_api as STATUS FROM results WHERE job_id = {job_id}", conn)
    conn.close()
    return df