
from dotenv import load_dotenv
import os
import psycopg2
from flask import Flask, request, jsonify

app = Flask(__name__)

# Verbindung zur PostgreSQL-Datenbank herstellen
def get_db_connection():
    load_dotenv()
    conn = psycopg2.connect(os.getenv('DB_PORT'))
    return conn

@app.route('/data', methods=['GET'])
def get_data():
    table = request.args.get('table')
    limit = request.args.get('limit', default=1000, type=int)
    offset = request.args.get('offset', default=0, type=int)

    if not table:
        return jsonify({"error": "Table name is required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(f'SELECT * FROM {table} LIMIT %s OFFSET %s', (limit, offset))
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        data = [dict(zip(colnames, row)) for row in rows]
    except psycopg2.DatabaseError as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

    return jsonify(data)

@app.route('/tables', methods=['GET'])
def get_tables():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'projekt_katinka'")
        tables = cur.fetchall()
        table_list = [table[0] for table in tables]
    except psycopg2.DatabaseError as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

    return jsonify(table_list)


@app.route("/")
def home():
    return "Hello from Katinka!"

if __name__ == "__main__":
    app.run()

#if __name__ == '__main__':
 #   app.run(host='0.0.0.0', port=5000)
