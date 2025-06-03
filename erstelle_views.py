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

#Teste die Verbindung
try:
    test_query = pd.read_sql("SELECT 1;", CONN)
except SQLAlchemyError as e:
    print(f"Fehler bei der Verbindung: {e}")

#Listen für die Views
wordtypes = "select distinct lang from projekt_katinka.wordtype;"
wordtypes_list = pd.read_sql(wordtypes, CONN).values.flatten().tolist()
languages = "select abkuerzung from projekt_katinka.languages;"
languages_list = pd.read_sql(languages, CONN).values.flatten().tolist()
#language,wordtype= "",""  # Platzhalter für spätere Ersetzungen in den Views
# Views für THese 1
#2 Hauptviews
hauptview_formenreichtum_englischSprecher="""CREATE MATERIALIZED  VIEW vw_these1_englischSprecher_formenreichtum AS
SELECT
    l.lexem_uninflected,
    l.wordtype,
    COUNT(DISTINCT l.lexem_inflected) AS formenanzahl,
    AVG(1.0 - vl.history_correct::float / NULLIF(vl.history_seen, 0)) AS avg_history_error_rate,
    AVG(1.0 - vl.session_correct::float / NULLIF(vl.session_seen, 0)) AS avg_session_error_rate
FROM
    vocable_learning vl
JOIN users u ON vl.uid = u.uid
JOIN (
    SELECT uid
    FROM users_languages
    GROUP BY uid
    HAVING COUNT(DISTINCT ui_language) = 1 AND MIN(ui_language) = 'en'
) AS only_en_ui ON only_en_ui.uid = u.uid
JOIN users_languages ul ON ul.uid = u.uid
JOIN lexeme l ON vl.lid = l.lid
GROUP BY l.lexem_uninflected, l.wordtype;"""
hauptview_formenreichtum_nichtEnglischSprecher="""CREATE MATERIALIZED  VIEW vw_these1_nichtEnglischSprecher_formenreichtum AS
SELECT
    l.lexem_uninflected,
    l.wordtype,
    w.lang,
    ul.learning_language,
    COUNT(DISTINCT l.lexem_inflected) AS formenanzahl,
    AVG(1.0 - vl.history_correct::float / NULLIF(vl.history_seen, 0)) AS avg_history_error_rate,
    AVG(1.0 - vl.session_correct::float / NULLIF(vl.session_seen, 0)) AS avg_session_error_rate
FROM
    vocable_learning vl
JOIN users u ON vl.uid = u.uid
JOIN (
    SELECT uid, MIN(learning_language) AS second_language
    FROM (
        SELECT uid, learning_language,
               ROW_NUMBER() OVER (PARTITION BY uid ORDER BY learning_language) AS rn
        FROM users_languages
        WHERE ui_language = 'en'
    ) sub
    WHERE rn = 2
    GROUP BY uid
) AS second_lang ON second_lang.uid = u.uid
JOIN users_languages ul ON ul.uid = u.uid AND ul.learning_language = second_lang.second_language
JOIN lexeme l ON vl.lid = l.lid
join wordtype w ON l.wordtype = w.abkuerzung
GROUP BY l.lexem_uninflected, l.wordtype,w.lang, ul.learning_language;"""
wortartenvergleich_alle_sprachen_alle_lerner="""CREATE MATERIALIZED VIEW vw_these1_summary_wordtype_language AS
WITH inflected_counts AS (
    SELECT
        l.lexem_uninflected,
        w.lang AS wordtype,
        ul.learning_language,
        COUNT(DISTINCT l.lexem_inflected) AS anzahl_inflected,
        AVG(1.0 - vl.history_correct::float / NULLIF(vl.history_seen, 0)) AS avg_history_error_rate,
        AVG(1.0 - vl.session_correct::float / NULLIF(vl.session_seen, 0)) AS avg_session_error_rate
    FROM vocable_learning vl
    JOIN lexeme l ON vl.lid = l.lid
    JOIN users_languages ul ON vl.uid = ul.uid
    JOIN wordtype w ON l.wordtype = w.abkuerzung
    GROUP BY l.lexem_uninflected, w.lang, ul.learning_language
)
SELECT
    wordtype,
    learning_language,
    COUNT(*) AS anzahl_lexeme,
    AVG(anzahl_inflected) AS avg_formenanzahl,
    AVG(avg_history_error_rate) AS avg_history_error_rate,
    AVG(avg_session_error_rate) AS avg_session_error_rate
FROM inflected_counts
GROUP BY wordtype, learning_language;
"""
# einige Unterviews
unteviews_sprachen_E="""CREATE MATERIALIZED  VIEW vw_these1_englischSprecher_formenreichtum_{language} AS
WITH valid_users AS (
    SELECT uid
    FROM users_languages
    WHERE ui_language = 'en'
    GROUP BY uid
    HAVING BOOL_AND(learning_language <> 'en')
),
inflected_counts AS (
    SELECT
        l.lexem_uninflected,
        w.lang AS wordtype,
        ul.learning_language,
        COUNT(DISTINCT l.lexem_inflected) AS anzahl_inflected,
        AVG(1.0 - vl.history_correct::float / NULLIF(vl.history_seen, 0)) AS avg_history_error_rate,
        AVG(1.0 - vl.session_correct::float / NULLIF(vl.session_seen, 0)) AS avg_session_error_rate
    FROM vocable_learning vl
    JOIN lexeme l ON vl.lid = l.lid
    JOIN users_languages ul ON vl.uid = ul.uid
    JOIN wordtype w ON l.wordtype = w.abkuerzung
    WHERE vl.uid IN (SELECT uid FROM valid_users)
    and ul.learning_language = '{language}'
    GROUP BY l.lexem_uninflected, w.lang, ul.learning_language
)
SELECT
    wordtype,
    COUNT(*) AS anzahl_lexeme,
    AVG(anzahl_inflected) AS avg_formenanzahl,
    AVG(avg_history_error_rate) AS avg_history_error_rate,
    AVG(avg_session_error_rate) AS avg_session_error_rate
FROM inflected_counts
GROUP BY wordtype;"""
unteviews_sprachen_nE="""CREATE MATERIALIZED  VIEW vw_these1_nichtEnglischSprecher_formenreichtum_{language} AS
WITH valid_users AS (
    SELECT uid
    FROM users_languages
    GROUP BY uid
    HAVING BOOL_AND(ui_language <> 'en')
),
inflected_counts AS (
    SELECT
        l.lexem_uninflected,
        w.lang AS wordtype,
        ul.learning_language,
        COUNT(DISTINCT l.lexem_inflected) AS anzahl_inflected,
        AVG(1.0 - vl.history_correct::float / NULLIF(vl.history_seen, 0)) AS avg_history_error_rate,
        AVG(1.0 - vl.session_correct::float / NULLIF(vl.session_seen, 0)) AS avg_session_error_rate
    FROM vocable_learning vl
    JOIN lexeme l ON vl.lid = l.lid
    JOIN users_languages ul ON vl.uid = ul.uid
    JOIN wordtype w ON l.wordtype = w.abkuerzung
    WHERE vl.uid IN (SELECT uid FROM valid_users)
    and ul.learning_language = '{language}'
    GROUP BY l.lexem_uninflected, w.lang, ul.learning_language
)
SELECT
    wordtype,
    learning_language,
    COUNT(*) AS anzahl_lexeme,
    AVG(anzahl_inflected) AS avg_formenanzahl,
    AVG(avg_history_error_rate) AS avg_history_error_rate,
    AVG(avg_session_error_rate) AS avg_session_error_rate
FROM inflected_counts
GROUP BY wordtype,learning_language;"""
unterviews_wordart_E="""CREATE MATERIALIZED  VIEW vw_these1_englischSprecher_formenreichtum_{wordtype} AS
SELECT *
FROM vw_these1_englischSprecher_formenreichtum t1
JOIN wordtype w ON t1.wordtype = w.abkuerzung
WHERE w.lang = '{wordtype}';"""
unterviews_wordart_nE="""CREATE MATERIALIZED  VIEW vw_these1_nichtEnglischSprecher_formenreichtum_{wordtype} AS
SELECT *
FROM vw_these1_englischSprecher_formenreichtum t1
JOIN wordtype w ON t1.wordtype = w.abkuerzung
WHERE w.lang = '{wordtype}';"""
unterview_alle_wortarten_E="""CREATE MATERIALIZED  VIEW vw_these1_englischLerner_formenreichtum_mit_wordtyp_lang AS
SELECT t1.*, w.lang AS wortart
FROM vw_these1_englischSprecher_formenreichtum t1
JOIN wordtype w ON t1.wordtype = w.abkuerzung;"""
# views für These 2
multi_languages_englischsprecher="""CREATE MATERIALIZED  VIEW vw_users_en_multiple_learning AS
SELECT ul.uid
FROM users_languages ul
WHERE ul.ui_language = 'en'
GROUP BY ul.uid
HAVING COUNT(DISTINCT ul.ui_language) = 1
   AND COUNT(DISTINCT ul.learning_language) >= 2;"""
multi_languages_nicht_englischsprecher="""CREATE MATERIALIZED  VIEW vw_users_non_en_multiple_learning AS
SELECT ul.uid
FROM users_languages ul
GROUP BY ul.uid
HAVING COUNT(DISTINCT ul.ui_language) > 1
   AND COUNT(DISTINCT ul.learning_language) >= 2;"""
hauptsprache="""CREATE MATERIALIZED  VIEW vw_first_learning_language AS
WITH user_language_counts AS (
    SELECT
        vl.uid,
        learning_language,
        COUNT(DISTINCT lid) AS learned_lids
    FROM vocable_learning vl
    join users_languages ul ON vl.uid = ul.uid
    GROUP BY vl.uid, learning_language
),
multi_language_users AS (
    SELECT uid
    FROM user_language_counts
    GROUP BY uid
    HAVING COUNT(*) > 1
)
SELECT DISTINCT ON (ulc.uid)
    ulc.uid,
    ulc.learning_language AS first_learning_language,
    ulc.learned_lids
FROM user_language_counts ulc
JOIN multi_language_users mlu ON ulc.uid = mlu.uid
ORDER BY ulc.uid, ulc.learned_lids DESC;
"""
fehlerquote_bei_multilanguage_en_lerner="""CREATE MATERIALIZED  VIEW vw_uid_llang_stats_E AS
SELECT
    vl.uid,
    ul.learning_language,
    COUNT(DISTINCT vl.lid) AS learned_lids,
    AVG(1.0 - vl.history_correct::float / NULLIF(vl.history_seen, 0)) AS avg_history_error_rate,
    AVG(1.0 - vl.session_correct::float / NULLIF(vl.session_seen, 0)) AS avg_session_error_rate
FROM
    vocable_learning vl
JOIN vw_users_en_multiple_learning ufilter ON vl.uid = ufilter.uid
JOIN users_languages ul ON vl.uid = ul.uid
GROUP BY vl.uid, ul.learning_language;"""
fehlerquote_bei_multilanguage_non_en_lerner="""CREATE MATERIALIZED  VIEW vw_uid_llang_stats_nE AS
SELECT
    vl.uid,
    ul.learning_language,
    COUNT(DISTINCT vl.lid) AS learned_lids,
    AVG(1.0 - vl.history_correct::float / NULLIF(vl.history_seen, 0)) AS avg_history_error_rate,
    AVG(1.0 - vl.session_correct::float / NULLIF(vl.session_seen, 0)) AS avg_session_error_rate
FROM
    vocable_learning vl
JOIN vw_users_non_en_multiple_learning ufilter ON vl.uid = ufilter.uid
JOIN users_languages ul ON vl.uid = ul.uid
GROUP BY vl.uid, ul.learning_language;"""
############################
hauptview_sprachenvergleich_englischsprecher_multilanguage="""CREATE MATERIALIZED  VIEW vw_these2_englisch_mehrsprachler AS
SELECT
    u.uid,
    COUNT(DISTINCT ul.learning_language) AS sprachen_gelernt,
    AVG(1.0 - vl.history_correct::float / NULLIF(vl.history_seen, 0)) AS avg_history_error_rate,
    AVG(1.0 - vl.session_correct::float / NULLIF(vl.session_seen, 0)) AS avg_session_error_rate
FROM
    users u
JOIN users_languages ul ON ul.uid = u.uid
JOIN (
    SELECT uid
    FROM users_languages
    GROUP BY uid
    HAVING COUNT(DISTINCT ui_language) = 1 AND MIN(ui_language) = 'en'
) AS only_en_ui ON only_en_ui.uid = u.uid
JOIN vocable_learning vl ON vl.uid = u.uid
WHERE ul.ui_language = 'en'
GROUP BY u.uid
HAVING COUNT(DISTINCT ul.learning_language) > 1;
"""
hauptview_sprachenvergleich_englischsprecher_singlelanguage="""CREATE MATERIALIZED  VIEW vw_these2_englisch_eineSprache AS
SELECT
    u.uid,
    COUNT(DISTINCT ul.learning_language) AS sprachen_gelernt,
    AVG(1.0 - vl.history_correct::float / NULLIF(vl.history_seen, 0)) AS avg_history_error_rate,
    AVG(1.0 - vl.session_correct::float / NULLIF(vl.session_seen, 0)) AS avg_session_error_rate
FROM
    users u
JOIN users_languages ul ON ul.uid = u.uid
JOIN vocable_learning vl ON vl.uid = u.uid
WHERE ul.ui_language = 'en'
GROUP BY u.uid
HAVING COUNT(DISTINCT ul.learning_language) = 1;
"""
vergleich_1_und_weitere_sprachen="""CREATE MATERIALIZED  VIEW vw_these2_comparison AS
SELECT
    lls.uid,
    lls.learning_language,
    lls.learned_lids,
    lls.avg_history_error_rate,
    lls.avg_session_error_rate,
    CASE
        WHEN lls.learning_language = fll.first_learning_language THEN TRUE
        ELSE FALSE
    END AS is_first_language
FROM
    vw_uid_llang_stats_E
      lls
JOIN vw_first_learning_language fll ON lls.uid = fll.uid;
"""

#views für These3
fehler_wortarten="""CREATE MATERIALIZED  VIEW vw_these3_fehlerquote_pro_wortart AS
SELECT
    wt.lang AS wortart,
    COUNT(DISTINCT l.lid) AS anzahl_lexeme,
    COUNT(vl.uid) AS anzahl_nutzungen,
    AVG(1.0 - vl.history_correct::float / NULLIF(vl.history_seen, 0)) AS avg_history_error_rate,
    AVG(1.0 - vl.session_correct::float / NULLIF(vl.session_seen, 0)) AS avg_session_error_rate
FROM
    vocable_learning vl
JOIN lexeme l ON vl.lid = l.lid
JOIN wordtype wt ON l.wordtype = wt.abkuerzung
GROUP BY wt.lang;"""


views=[ hauptview_formenreichtum_englischSprecher,hauptview_formenreichtum_nichtEnglischSprecher,wortartenvergleich_alle_sprachen_alle_lerner,
	unteviews_sprachen_E,  unteviews_sprachen_nE, unterviews_wordart_E, unterviews_wordart_nE, unterview_alle_wortarten_E,
	multi_languages_englischsprecher, multi_languages_nicht_englischsprecher, hauptsprache,
	fehlerquote_bei_multilanguage_en_lerner, fehlerquote_bei_multilanguage_non_en_lerner,hauptview_sprachenvergleich_englischsprecher_multilanguage, 
	hauptview_sprachenvergleich_englischsprecher_singlelanguage, vergleich_1_und_weitere_sprachen, fehler_wortarten
]
def create_views(CONN, views, wordtypes_list, languages_list):
    """    Erstellt alle Views in der Datenbank."""
    #CONN.begin()
    for view in views:
        try:
            if "{wordtype}" in view:
                print("if wordtype in view")
                for w in wordtypes_list:
                    print(w," ")
                    view_with_wordtype = view.replace("{wordtype}", w)
                    CONN.execute(text(view_with_wordtype))
                    CONN.commit()
                    print(f"View erstellt: {view_with_wordtype}")
            elif "{language}" in view:
                for l in languages_list:
                    print(l)
                    view_with_language = view.replace("{language}", l)
                    CONN.execute(text(view_with_language))
                    CONN.commit()
                    print(f"View erstellt: {view_with_language}")
            else:
                CONN.execute(text(view))
                print(f"View erstellt: {view}")
                CONN.commit()
        except SQLAlchemyError as e:
            print(f"Fehler beim Erstellen der View: {view}\n{e}\n\n########\n")

#create_views(CONN, views, wordtypes_list, languages_list)
###### muss tabellen haben für power bi ####


# Verbindung zur Datenbank aufbauen

# Alle Materialized Views im Schema abfragen
views_result = CONN.execute(text(f"""
    SELECT matviewname
    FROM pg_matviews
    WHERE schemaname = 'projekt_katinka'
"""))

views = [row.matviewname for row in views_result]

for viewname in views:
    table_name = f"{viewname.replace('vw_','')}"

    # Bestehende Tabelle löschen (wenn vorhanden)
    CONN.execute(text(f"""
        DROP TABLE IF EXISTS projekt_katinka.{table_name} CASCADE;
    """))

    # Neue Tabelle aus View erstellen
    try:
        CONN.execute(text(f"""
            CREATE TABLE projekt_katinka.{table_name} AS
            SELECT * FROM projekt_katinka.{viewname};
        """))
        CONN.commit()
    except SQLAlchemyError as e:
        print(f"Fehler beim Erstellen der Tabelle {table_name} aus View {viewname}: {e}")
        continue
print("✅ Alle Tabellen wurden erfolgreich erstellt.")
