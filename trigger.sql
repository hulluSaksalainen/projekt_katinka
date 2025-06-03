

CREATE TRIGGER trg_refresh_on_users
AFTER INSERT OR UPDATE OR DELETE ON users
FOR EACH STATEMENT EXECUTE FUNCTION refresh_materialized_views();

CREATE TRIGGER trg_refresh_on_users_languages
AFTER INSERT OR UPDATE OR DELETE ON users_languages
FOR EACH STATEMENT EXECUTE FUNCTION refresh_materialized_views();

CREATE TRIGGER trg_refresh_on_vocable_learning
AFTER INSERT OR UPDATE OR DELETE ON vocable_learning
FOR EACH STATEMENT EXECUTE FUNCTION refresh_materialized_views();

CREATE TRIGGER trg_refresh_on_lexeme
AFTER INSERT OR UPDATE OR DELETE ON lexeme
FOR EACH STATEMENT EXECUTE FUNCTION refresh_materialized_views();

CREATE TRIGGER trg_refresh_on_wordtype
AFTER INSERT OR UPDATE OR DELETE ON wordtype
FOR EACH STATEMENT EXECUTE FUNCTION refresh_materialized_views();

CREATE TRIGGER trg_refresh_on_languages
AFTER INSERT OR UPDATE OR DELETE ON languages
FOR EACH STATEMENT EXECUTE FUNCTION refresh_materialized_views();
