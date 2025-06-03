import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from dotenv import load_dotenv
import os
load_dotenv()
ENGINE = create_engine(os.getenv("DB_CONNECT"))
# Logging einrichten
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

durchlauf_zaehler = 6 #start Zähler
START_TEMP_TABLE="CREATE TEMP TABLE temp_table AS "
#sql_strings
NOCH_WELCHE_ZU_BEFUELLEN_FUER_LEXEME_ID="select vl.lexeme_id from katinka_projekt.vocable_learning vl where vl.lexeme_id not in (select l.lexeme_id from lexeme l) limit 1"
TEMP_TABLE_FUER_LEXEME_ID=f"""{START_TEMP_TABLE}select distinct on (vl.lexeme_id) vl.lexeme_id 
	from projekt_katinka.vocalbe_learning order by lemexe_id limit 1000 offset {durchlauf_zaehler*1000}"""
INSERT_LEXEME_ID="insert into lexeme (lexeme_id) set lexeme_id = (select tt.lexeme_id from temp_table tt)"
CONTROLL_LEXEME_ID="select max(lid) as wert from projekt_katinka.lexeme"

NOCH_WELCHE_ZU_BEFUELLEN_FUER_WORDTYPE="SELECT lid FROM projekt_katinka.lexeme WHERE wordtype IS NULL LIMIT 1"
TEMP_TABLE_FUER_WORDTYPE=f"""{START_TEMP_TABLE}
	SELECT l.lid as lid, vl.wordtype AS wordtype
	FROM vocable_learning vl
	join lexeme l on vl.lexeme_id=l.lexeme_id
	WHERE l.wordtype IS NULL
	limit 100;"""
UPDATE_FUER_WORDTYPE="""UPDATE lexeme
	SET wordtype = (SELECT tt.wordtype FROM temp_table tt WHERE tt.lid = lexeme.lid)
	WHERE wordtype IS NULL;"""
CONTROLL_WORDTYPE="select count(wordtype) as wert from lexeme where wordtype is not null"

NOCH_WELCHE_ZU_BEFUELLEN_FUER_LEXEME_INFLECTED="SELECT lid FROM projekt_katinka.lexeme WHERE lexem_inflected IS NULL LIMIT 1"
TEMP_TABLE_FUER_LEXEME_INFLECTED=f"""{START_TEMP_TABLE}SELECT 
        distinct on (l.lid) l.lid AS lid,
        CASE 
            WHEN position('/' in lexeme) > 0 
            THEN substr(lexeme, 0, position('/' in lexeme)) 
            ELSE ''
        END AS lexem_inflected
    FROM 
        projekt_katinka.vocable_learning vl
        JOIN projekt_katinka.lexeme l ON vl.lexeme_id = l.lexeme_id
    WHERE 
        l.lexem_inflected IS NULL
        AND l.lid >{durchlauf_zaehler*100} AND l.lid <= {(durchlauf_zaehler+1)*100}"""
UPDATE_FUER_LEXEME_INFLECTED="""UPDATE projekt_katinka.lexeme
    SET lexem_inflected = tt.lexem_inflected
    FROM temp_table tt
    WHERE lexeme.lid = tt.lid
        AND lexeme.lexem_inflected IS NULL
        AND tt.lexem_inflected is not null"""
CONTROLL_LEXEME_INFLECTED="select count(lexeme_inflected) from lexeme where lexeme_inflected is not null;"

NOCH_WELCHE_ZU_BEFUELLEN_FUER_UNINFLECTED="SELECT lid FROM projekt_katinka.lexeme WHERE lexem_uninflected IS NULL LIMIT 1"
TEMP_TABLE_FUER_LEXEME_UNINFLECTED=f"""{START_TEMP_TABLE}SELECT distinct on (l.lid) l.lid AS lid,
            CASE WHEN position('/' in lexeme_string) > 0 
                THEN substr(lexeme_string, position('/' in lexeme_string)+1,POSITION('<' in lexeme_string)-position('/' in lexeme_string)-1) 
                ELSE substr(lexeme_string,0,position('<' in lexeme_string))
    		END AS lexem_uninflected,
            case when position('><'in lexeme_string)>0
                then substr(lexeme_string,position('><' in lexeme_string)+2)
                else ''
            end as other_definitions
            FROM projekt_katinka.lexeme l
            JOIN projekt_katinka.vocable_learning vl ON vl.lexeme_id = l.lexeme_id
            WHERE l.lexem_uninflected IS NULL
			AND l.lid >{durchlauf_zaehler*100} AND l.lid <= {(durchlauf_zaehler+1)*100}"""
UPDATE_FUER_LEXEME_UNINFLECTED=""
CONTROLL_LEXEME_UNINFLECTED=""

NOCH_WELCHE_ZU_BEFUELLEN_FUER_OTHER_DEFINITIONS="SELECT lid FROM projekt_katinka.lexeme WHERE other_definitions IS NULL LIMIT 1"
TEMP_TABLE_FUER_OTHER_DEFINITIONS=f""
UPDATE_FUER_OTHER_DEFINITIONS=""
CONTROLL_OTHER_DEFINITIONS=""

NOCH_WELCHE_ZU_BEFUELLEN_FUER_USER_ID="select vl.user_id from katinka_projekt.vocable_learning vl where user_id on in (select U.user_id from users u) limit 1"
TEMP_TABLE_FUER_USER_ID=f"{START_TEMP_TABLE}select distinct on (vl.user_id) vl.user_id from projekt_katinka.vocalbe_learning limit 100 offset {durchlauf_zaehler*100}"
INSERT_USER_ID=""
CONTROLL_USER_ID=""

NOCH_WELCHE_ZU_BEFUELLEN_FUER_USER_LANGUAGES_COMBINATIONS="SELECT uid FROM projekt_katinka.users_languages WHERE ui_language IS NULL LIMIT 1"
TEMP_TABLE_FUER_USER_LANGUAGES_COMBINATIONS=""
UPDATE_USER_LANGUAGES_COMBINATIONS=""
CONTROLL_LANGUAGES_CONBINATIONS=""


#sqls:
sqls=pd.DataFrame([(NOCH_WELCHE_ZU_BEFUELLEN_FUER_LEXEME_ID,TEMP_TABLE_FUER_LEXEME_ID, INSERT_LEXEME_ID, CONTROLL_LEXEME_ID),
(NOCH_WELCHE_ZU_BEFUELLEN_FUER_WORDTYPE, TEMP_TABLE_FUER_WORDTYPE, UPDATE_FUER_WORDTYPE, CONTROLL_WORDTYPE),
(NOCH_WELCHE_ZU_BEFUELLEN_FUER_LEXEME_INFLECTED, TEMP_TABLE_FUER_LEXEME_INFLECTED, UPDATE_FUER_LEXEME_INFLECTED, CONTROLL_LEXEME_INFLECTED),
(NOCH_WELCHE_ZU_BEFUELLEN_FUER_LEXEME_INFLECTED, TEMP_TABLE_FUER_LEXEME_UNINFLECTED, UPDATE_FUER_LEXEME_UNINFLECTED, CONTROLL_LEXEME_UNINFLECTED),
(NOCH_WELCHE_ZU_BEFUELLEN_FUER_OTHER_DEFINITIONS, TEMP_TABLE_FUER_OTHER_DEFINITIONS, UPDATE_FUER_OTHER_DEFINITIONS, CONTROLL_OTHER_DEFINITIONS),
(NOCH_WELCHE_ZU_BEFUELLEN_FUER_USER_ID, TEMP_TABLE_FUER_USER_ID, INSERT_USER_ID, CONTROLL_USER_ID),
(NOCH_WELCHE_ZU_BEFUELLEN_FUER_USER_LANGUAGES_COMBINATIONS, TEMP_TABLE_FUER_USER_LANGUAGES_COMBINATIONS, UPDATE_USER_LANGUAGES_COMBINATIONS, CONTROLL_LANGUAGES_CONBINATIONS)],
columns=["noch_welche","temp_table","update_insert","controll"])

try:
	with ENGINE.connect() as conn:
		for daten_fueller in sqls:
			while True:
				#noch welche zu befüllen?
				result = conn.execute(text(daten_fueller[0]))            
				if result.fetchone() is None:
					logger.info("Fertig - keine NULL-Werte mehr.")
					break
				#temp table erstellen
				conn.execute(text("DROP TABLE IF EXISTS temp_table"))
				conn.execute(text(daten_fueller[1]))
				logger.info("Temp_table erstellt")
				#temp_table anzeigen
				# df = pd.read_sql("SELECT * FROM temp_table", conn)
				# print(df)
				#daten füllen
				result=conn.execute(text(daten_fueller[2]))
				conn.commit() #die fehlte
				#kontrollieren
				updated_rows = conn.execute(text(daten_fueller[3]))
				for row in updated_rows:##muss ich noch schauen
					logger.info(f"Aktualisierte Zeile: neuer maximaler Wert={row.wert}")
					if max<= row.wert:
						logger.error(f"{row.wert} ist der neue Wert und {max} der alte max Wert.")
					else:
						max=row.wert
				logger.info(f"Batch {durchlauf_zaehler}: {result.rowcount} Zeilen aktualisiert.")
				durchlauf_zaehler += 1
except SQLAlchemyError as e:
    logger.error(f"Datenbankfehler: {str(e)}")
except Exception as e:
    logger.error(f"Allgemeiner Fehler: {str(e)}")
