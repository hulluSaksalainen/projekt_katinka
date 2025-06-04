
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from flask import Flask, request, jsonify
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
    limit = request.args.get('limit', default=1000, type=int)
    offset = request.args.get('offset', default=0, type=int)
    if not table:
        return jsonify({"error": "Table name is required"}), 400

    conn = get_db_connection()

    try:
        result=conn.execute(text(f'SELECT * FROM {table} LIMIT {limit} OFFSET {offset};'))
        columns = result.keys()
        rows = result.fetchall()
        data = [dict(zip(columns, row)) for row in rows]
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    return jsonify(data)

@app.route('/tables', methods=['GET'])
def get_tables():
    conn = get_db_connection()
    try:
        result=conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'projekt_katinka'"))
        tables = result.fetchall()
        table_list = [table[0] for table in tables]
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    return jsonify(table_list)


@app.route("/")
def home():
    return "Hello from Katinka!"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000)) # Render gibt dir den Port
    app.run(host="0.0.0.0", port=port)