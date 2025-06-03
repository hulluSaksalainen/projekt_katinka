#erstelle die trigger für meine Views
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

#Verbindung zur Datenbank
load_dotenv()
C = create_engine(os.getenv("DB_CONNECT"))
CONN = C.connect()
CONN.execute(text("SET search_path TO projekt_katinka;"))
try:
    test_query = pd.read_sql("SELECT 1;", CONN)
except SQLAlchemyError as e:
    print(f"Fehler bei der Verbindung: {e}")

#zentrale function
function_fuer_trigger = """CREATE OR REPLACE FUNCTION refresh_materialized_views() RETURNS void AS $$
DECLARE
BEGIN
    RAISE NOTICE 'Refreshing materialized views...';
    if TG_TABLE_NAME = 'vocable_learning' then
		REFRESH MATERIALIZED VIEW CONCURRENTLY
    elsif TG_TABLE_NAME = 'lexeme' then
		REFRESH MATERIALIZED VIEW CONCURRENTLY
	elsif TG_TABLE_NAME = 'languages' then
		REFRESH MATERIALIZED VIEW CONCURRENTLY
    elsif TG_TABLE_NAME = 'wordtype' then
		REFRESH MATERIALIZED VIEW CONCURRENTLY
    elsif TG_TABLE_NAME = 'users' then
		REFRESH MATERIALIZED VIEW CONCURRENTLY
    elsif TG_TABLE_NAME = 'users_languages' then
		REFRESH MATERIALIZED VIEW CONCURRENTLY
	end if;
    REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englisch_formenreichtum_mat;
    REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_sprachenfolge_mat;
    REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these3_wordtype_error_mat;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;"""

