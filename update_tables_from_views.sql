
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS TRIGGER AS $$
DECLARE
    view_rec RECORD;
    base_table_name TEXT;
BEGIN
    IF TG_TABLE_NAME = 'vocable_learning' THEN
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_en;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_it;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_de;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_summary_wordtype_language;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_pt;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_es;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_fr;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_en;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_interjektion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_de;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_es;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_position;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_pt;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_verb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_it;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_fr;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_konjunktion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_nomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_pronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_relativpronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_adverb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_zahlwort;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_position;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_adjektiv;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_determinativ;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_adverb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_konjunktion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_pronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_interjektion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_verb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_nomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_relativpronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_zahlwort;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_adjektiv;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_determinativ;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischlerner_formenreichtum_mit_wordtyp_lang;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_users_en_multiple_learning;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_users_non_en_multiple_learning;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_first_learning_language;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_uid_llang_stats_e;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_uid_llang_stats_ne;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_englisch_mehrsprachler;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_englisch_einesprache;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_comparison;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these3_fehlerquote_pro_wortart;

    ELSIF TG_TABLE_NAME = 'lexeme' THEN
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_en;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_it;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_de;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_summary_wordtype_language;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_pt;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_es;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_fr;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_en;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_interjektion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_de;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_es;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_position;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_pt;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_verb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_it;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_fr;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_konjunktion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_nomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_pronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_relativpronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_adverb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_zahlwort;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_position;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_adjektiv;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_determinativ;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_adverb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_konjunktion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_pronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_interjektion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_verb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_nomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_relativpronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_zahlwort;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_adjektiv;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_determinativ;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischlerner_formenreichtum_mit_wordtyp_lang;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_users_en_multiple_learning;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_users_non_en_multiple_learning;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_first_learning_language;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_uid_llang_stats_e;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_uid_llang_stats_ne;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_englisch_mehrsprachler;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_englisch_einesprache;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_comparison;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these3_fehlerquote_pro_wortart;

    ELSIF TG_TABLE_NAME = 'languages' THEN
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_en;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_it;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_de;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_summary_wordtype_language;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_pt;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_es;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_fr;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_en;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_interjektion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_de;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_es;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_position;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_pt;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_verb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_it;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_fr;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_konjunktion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_nomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_pronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_relativpronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_adverb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_zahlwort;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_position;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_adjektiv;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_determinativ;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_adverb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_konjunktion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_pronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_interjektion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_verb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_nomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_relativpronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_zahlwort;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_adjektiv;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_determinativ;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischlerner_formenreichtum_mit_wordtyp_lang;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_users_en_multiple_learning;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_users_non_en_multiple_learning;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_first_learning_language;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_uid_llang_stats_e;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_uid_llang_stats_ne;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_englisch_mehrsprachler;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_englisch_einesprache;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_comparison;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these3_fehlerquote_pro_wortart;

    ELSIF TG_TABLE_NAME = 'wordtype' THEN
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_en;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_it;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_de;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_summary_wordtype_language;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_pt;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_es;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_fr;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_en;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_interjektion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_de;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_es;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_position;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_pt;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_verb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_it;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_fr;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_konjunktion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_nomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_pronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_relativpronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_adverb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_zahlwort;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_position;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_adjektiv;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_determinativ;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_adverb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_konjunktion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_pronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_interjektion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_verb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_nomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_relativpronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_zahlwort;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_adjektiv;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_determinativ;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischlerner_formenreichtum_mit_wordtyp_lang;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_users_en_multiple_learning;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_users_non_en_multiple_learning;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_first_learning_language;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_uid_llang_stats_e;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_uid_llang_stats_ne;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_englisch_mehrsprachler;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_englisch_einesprache;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_comparison;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these3_fehlerquote_pro_wortart;

    ELSIF TG_TABLE_NAME = 'users' THEN
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_en;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_it;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_de;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_summary_wordtype_language;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_pt;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_es;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_fr;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_en;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_interjektion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_de;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_es;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_position;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_pt;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_verb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_it;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_fr;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_konjunktion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_nomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_pronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_relativpronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_adverb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_zahlwort;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_position;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_adjektiv;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_determinativ;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_adverb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_konjunktion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_pronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_interjektion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_verb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_nomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_relativpronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_zahlwort;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_adjektiv;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_determinativ;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischlerner_formenreichtum_mit_wordtyp_lang;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_users_en_multiple_learning;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_users_non_en_multiple_learning;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_first_learning_language;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_uid_llang_stats_e;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_uid_llang_stats_ne;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_englisch_mehrsprachler;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_englisch_einesprache;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_comparison;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these3_fehlerquote_pro_wortart;

    ELSIF TG_TABLE_NAME = 'users_languages' THEN
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_en;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_it;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_de;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_summary_wordtype_language;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_pt;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_es;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_fr;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_en;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_interjektion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_de;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_es;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_position;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_pt;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_verb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_it;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_fr;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_konjunktion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_nomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_pronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_relativpronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_adverb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_zahlwort;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_position;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_adjektiv;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischsprecher_formenreichtum_determinativ;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_adverb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_konjunktion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_pronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_interjektion;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_verb;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_nomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_relativpronomen;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_zahlwort;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_adjektiv;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_nichtenglischsprecher_formenreichtum_determinativ;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these1_englischlerner_formenreichtum_mit_wordtyp_lang;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_users_en_multiple_learning;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_users_non_en_multiple_learning;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_first_learning_language;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_uid_llang_stats_e;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_uid_llang_stats_ne;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_englisch_mehrsprachler;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_englisch_einesprache;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these2_comparison;
        REFRESH MATERIALIZED VIEW CONCURRENTLY vw_these3_fehlerquote_pro_wortart;

    END IF;



    FOR view_rec IN
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = 'public'
    LOOP
        -- Tabellenname ohne "vw_"-Präfix
        base_table_name := replace(view_rec.matviewname, 'vw_', '');

        -- Alte Tabelle löschen, falls vorhanden
        EXECUTE format('DROP TABLE IF EXISTS %I', base_table_name);

        -- Neue Tabelle aus der View erstellen
        EXECUTE format('CREATE TABLE %I AS TABLE %I', base_table_name, view_rec.matviewname);
    END LOOP;


    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
