
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from flask import Flask, request, jsonify
from flask_compress import Compress

app = Flask(__name__)
Compress(app)

load_dotenv()
app = Flask(__name__)

# Verbindung zur PostgreSQL-Datenbank herstellen
def get_db_connection():
    load_dotenv()
    ENGINE = create_engine(os.getenv("DB_CONNECT"))
    conn=ENGINE.connect()  
    conn.execute(text("SET search_path TO projekt_katinka;"))
    return conn

@app.route('/data', methods=['GET'])
def get_data():
    table = request.args.get('table')
    limit = request.args.get('limit', type=int)
    offset = request.args.get('offset', default=0, type=int)
    if not table:
        return jsonify({"error": "Table name is required"}), 400
    return jsonify(table,limit,offset)

def get_table_by_name(table:str, limit:int=None, offset:int=0):
    conn = get_db_connection()

    try:
        query = f"SELECT * FROM {table}"
        if limit is not None:
            query += f" LIMIT {limit} OFFSET {offset}"
        query+=";"
        result=conn.execute(text(query))
        columns = result.keys()
        rows = result.fetchall()
        data = [dict(zip(columns, row)) for row in rows]
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    return data

@app.route('/tables', methods=['GET'])
def get_tables():
    return jsonify(alltables)

def alltables():
    conn = get_db_connection()
    try:
        result=conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'projekt_katinka'"))
        tables = result.fetchall()
        table_list = [table[0] for table in tables]
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()
    return table_list

@app.route('/data/all')
def get_all_data():
    all_tables = alltables()  
    result = {}

    for table in all_tables:
        if table=="users" or table=="vocable_learning" or table=="lexeme":
            continue
        result[table] = get_table_by_name(table)

    return jsonify(result)


@app.route("/")
def home():
    return "Hello from Katinka!"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000)) # Render gibt dir den Port
    app.run(host="0.0.0.0", port=port)