-- SQLBook: Code
--Tabellen Users , Lexeme, Users_Languages erstellen
create table lexeme(
    lid integer GENERATED ALWAYS AS IDENTITY NOT NULL,
    lexeme_id varchar(255) not null,
	lexem_inflected VARCHAR(255),
	lexem_uninflected VARCHAR(255),
	wordtype varchar(10),
	other_definitions VARCHAR(255),
    PRIMARY KEY(lid)
);
CREATE UNIQUE INDEX lexeme_lexem_key ON lexeme USING btree (lexeme_id);
create table users(
    uid integer GENERATED ALWAYS AS IDENTITY NOT NULL,
	user_id VARCHAR(10) not null,
	primary key (uid)
);
CREATE UNIQUE INDEX users_user_id_key ON users USING btree (user_id);
CREATE TABLE users_languages (
    ulid INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    uid INTEGER NOT NULL,
    ui_language VARCHAR(2),
    learning_language VARCHAR(2),
    FOREIGN KEY (uid) REFERENCES users(uid)
);
CREATE TABLE other_definitions (
    oid INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lid INTEGER NOT NULL,
    rest VARCHAR(255),
    FOREIGN KEY (lid) REFERENCES lexeme(lid)
);
CREATE UNIQUE INDEX other_definitions_lid_key ON other_definitions USING btree (lid);
CREATE UNIQUE INDEX languages_lang_key ON languages USING btree (lang);
alter table languages add PRIMARY KEY (abkuerzung);
CREATE UNIQUE INDEX wordtype_lang_key ON wordtype USING btree (lang);
alter table wordtype add PRIMARY KEY (abkuerzung);
--- einfache befüllungen machen, komplexere über skript

INSERT INTO projekt_katinka.users_languages (uid, ui_language, learning_language)
SELECT DISTINCT vl.uid, vl.ui_language, vl.learning_language
FROM vocable_learning vl
WHERE NOT EXISTS (SELECT 1 FROM projekt_katinka.users_languages ul
 WHERE ul.uid = vl.uid AND ul.ui_language = vl.ui_language AND ul.learning_language = vl.learning_language
 );
INSERT INTO projekt_katinka.other_definitions (lid,rest)
select l.lid,l.other_definitions from lexeme l 
where not exists (select 1 from  projekt_katinka.other_definitions o 
where o.lid=l.lid and l.other_definitions=o.rest);


--- ids mit integer ids aus den kleineren Tabellen ersetzen
update projekt_katinka.vocable_learning vl set lexeme_id=lid 
            FROM lexeme 
            WHERE lexeme.lexeme_id = vl.lexeme_id;
update projekt_katinka.vocable_learning vl set user_id=uid 
            FROM users 
            WHERE users.user_id = vl.user_id;
--    und die in den 3 neuen Tabellen verbunden Spalten entfernen
alter TABLE vocable_learning drop COLUMN lexeme_string;
--automatisch erstellte Tabellen "verbessern",dh die history und session spalten ändern(von str zu int) 
ALTER TABLE vocable_learning add COLUMN history_seen1 INTEGER;
ALTER TABLE vocable_learning add COLUMN history_correct1 INTEGER;
ALTER TABLE vocable_learning add COLUMN session_seen1 INTEGER;
ALTER TABLE vocable_learning add COLUMN session_correct1 INTEGER;
ALTER TABLE vocable_learning add COLUMN uid INTEGER;
ALTER TABLE vocable_learning add COLUMN lid INTEGER;
alter table vocable_learning add FOREIGN KEY (uid) REFERENCES users(uid);
alter table vocable_learning add FOREIGN KEY (lid) REFERENCES lexeme(lid);
alter table users_languages add FOREIGN KEY (ui_language) REFERENCES languages(abkuerzung);
alter table users_languages add FOREIGN KEY (learning_language) REFERENCES languages(abkuerzung);
UPDATE vocable_learning SET session_correct1 = session_correct::INTEGER, session_seen1 = session_seen::INTEGER;
UPDATE vocable_learning SET history_correct1 = history_correct::INTEGER, history_seen1 = history_seen::INTEGER;
UPDATE vocable_learning SET uid = user_id::INTEGER;
UPDATE vocable_learning SET lid = lexeme_id::INTEGER;-- einzeln sonst bricht es ab
alter Table vocable_learning DROP COLUMN user_id;
alter table vocable_learning drop COLUMN lexeme_id;
alter Table vocable_learning DROP COLUMN history_seen;
alter Table vocable_learning DROP COLUMN session_seen;
alter Table vocable_learning DROP COLUMN history_correct;
alter Table vocable_learning DROP COLUMN history_seen;
alter Table vocable_learning DROP COLUMN ui_language;
alter Table vocable_learning DROP COLUMN learning_language;
alter table lexeme drop column other_definitions;
alter table vocable_learning RENAME COLUMN history_seen1="history_seen";
alter table vocable_learning RENAME COLUMN history_correct1="history_correct";
alter table vocable_learning RENAME COLUMN session_seen1="session_seen";
alter table vocable_learning RENAME COLUMN session_correct1="session_correct";
-------------------------------------------
-- auswertungstabellen
--- ich will nachweisen, dasss englische ui-language Lerner (die keine andere Ui_language haben) mehr fehlversuche habe,
---- wenn das Wort verschiedene Flexionsformen hat (THese1)
select session_seen,session_correct,(session_correct/session_seen)::float as s1,p_recall from vocable_learning where p_recall='0.875'
 ;
---- views
CREATE OR REPLACE VIEW vw_these1_englischLerner_formenreichtum AS
SELECT
    l.lexem_uninflected,
    l.wordtype,
    COUNT(DISTINCT l.lexem_inflected) AS formenanzahl,
    AVG(1.0 - vl.history_correct::float / NULLIF(vl.history_seen, 0)) AS avg_history_error_rate,
    AVG(1.0 - vl.session_correct::float / NULLIF(vl.session_seen, 0)) AS avg_session_error_rate
FROM
    vocable_learning vl
JOIN users u ON vl.uid = u.uid
JOIN users_languages ul ON ul.uid = u.uid
JOIN lexeme l ON vl.lid = l.lid
WHERE
    ul.ui_language = 'en'
GROUP BY
    l.lexem_uninflected,
    l.wordtype;

CREATE OR REPLACE VIEW vw_these1_englischLerner_formenreichtum_nur_verben AS
SELECT *
FROM vw_these1_englisch_formenreichtum t1
JOIN wordtype w ON t1.wordtype = w.abkuerzung
WHERE w.lang = 'Verb';
CREATE OR REPLACE VIEW vw_these1_englischLerner_formenreichtum_nur_franz AS
SELECT *
FROM vw_these1_englisch_formenreichtum
WHERE learning_language = 'fr';

SELECT matviewname AS view_name
FROM pg_matviews
WHERE schemaname = 'projekt_katinka';
select * from languages limit 1000 offset 0;


ALTER TABLE uid_llang_stats_ne DISABLE TRIGGER ALL;

ALTER TABLE uid_llang_stats_ne enABLE TRIGGER ALL;
alter TABLE these2_englisch_einesprache add column learning_language varchar(2);
update these2_englisch_einesprache set learning_language=ul.learning_language 
FROM users_languages ul
where ul.uid=these2_englisch_einesprache.uid;
ALTER TABLE lexeme
ADD CONSTRAINT lexeme_language_fkey FOREIGN key(language) REFERENCES languages(abkuerzung);

ALTER TABLE uid_llang_stats_ne
ADD CONSTRAINT uid_llang_stats_ne_language_fkey 
FOREIGN key(learning_language) REFERENCES languages(abkuerzung);

select count(*) as c, lexem_uninflected from these1_englischlerner_formenreichtum_mit_wordtyp_lang 
GROUP BY lexem_uninflected order by c desc;
select * from lexeme where lexem_uninflected='pouco';