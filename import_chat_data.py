import json
import psycopg2
from psycopg2.extras import execute_values
import os

# Get the SQL connection string from the environment variable
SQL_DB_CONNECTION_STRING = "postgresql://gtahidi_admin:Ambu9265!@jsondata.postgres.database.azure.com:5432/postgres"

if not SQL_DB_CONNECTION_STRING:
    raise ValueError("SQL_DB_CONNECTION_STRING environment variable is not set")

def connect_to_db():
    return psycopg2.connect(SQL_DB_CONNECTION_STRING)
def create_tables_if_not_exist(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        id SERIAL PRIMARY KEY,
        session_id TEXT UNIQUE,
        source TEXT,
        memory_type TEXT,
        email TEXT
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY,
        session_id TEXT REFERENCES sessions(session_id),
        content TEXT,
        role TEXT,
        time TIMESTAMP,
        used_tools JSONB,
        file_annotations JSONB
    )
    """)
def insert_sessions(cur, sessions):
    execute_values(cur, """
        INSERT INTO sessions (session_id, source, memory_type, email)
        VALUES %s
        ON CONFLICT (session_id) DO NOTHING
    """, sessions)

def insert_messages(cur, messages):
    execute_values(cur, """
        INSERT INTO messages (session_id, content, role, time, used_tools, file_annotations)
        VALUES %s
    """, messages)

def import_data(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)

    sessions = []
    messages = []

    for item in data:
        sessions.append((
            item['sessionId'],
            item['source'],
            item.get('memoryType'),
            item.get('email', '')
        ))

        for msg in item['messages']:
            messages.append((
                item['sessionId'],
                msg['content'],
                msg['role'],
                msg['time'],
                json.dumps(msg.get('usedTools', [])),
                json.dumps(msg.get('fileAnnotations', []))
            ))

    conn = connect_to_db()
    cur = conn.cursor()

    try:
        create_tables_if_not_exist(cur)
        insert_sessions(cur, sessions)
        insert_messages(cur, messages)
        conn.commit()
        print(f"Imported {len(sessions)} sessions and {len(messages)} messages successfully.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    import_data('ab912ece-da19-4721-ba72-6acd787adead-Message (2).json')