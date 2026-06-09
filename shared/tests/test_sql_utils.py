from shared.sql_utils import construct_sql_uri, get_sqlalchemy_uri


def test_ssl_postgresql():
    uri = construct_sql_uri(
        database_type="postgresql", host="h", port=5432, username="u", password="p", database="d", ssl_enabled=True
    )
    assert uri == "postgresql://u:p@h:5432/d?sslmode=require"


def test_ssl_postgres_family_aliases():
    for db_type in ("postgres", "redshift"):
        uri = construct_sql_uri(database_type=db_type, host="h", username="u", database="d", ssl_enabled=True)
        assert "sslmode=require" in uri, db_type


def test_ssl_mysql_omitted():
    uri = construct_sql_uri(
        database_type="mysql", host="h", port=3306, username="u", database="d", ssl_enabled=True, connect_timeout=10
    )
    assert uri == "mysql://u@h:3306/d"


def test_ssl_sqlite_omitted():
    uri = construct_sql_uri(database_type="sqlite", database="/tmp/x.db", ssl_enabled=True, connect_timeout=10)
    assert uri == "sqlite:////tmp/x.db"


def test_connect_timeout_postgres():
    uri = construct_sql_uri(database_type="postgresql", host="h", username="u", database="d", connect_timeout=10)
    assert uri == "postgresql://u@h/d?connect_timeout=10"


def test_ssl_and_timeout_param_join():
    uri = construct_sql_uri(
        database_type="postgresql", host="h", username="u", database="d", ssl_enabled=True, connect_timeout=7
    )
    assert uri.count("?") == 1
    assert uri.endswith("?sslmode=require&connect_timeout=7")


def test_extra_kwargs_appended():
    uri = construct_sql_uri(
        database_type="postgresql", host="h", username="u", database="d", ssl_enabled=True, application_name="ff"
    )
    assert "sslmode=require" in uri and "application_name=ff" in uri
    assert uri.count("?") == 1


def test_url_passthrough_ignores_ssl():
    raw = "postgresql://u:p@h:5432/d"
    assert construct_sql_uri(url=raw, ssl_enabled=True, connect_timeout=10) == raw


def test_no_params_unchanged():
    uri = construct_sql_uri(database_type="postgresql", host="h", port=5432, username="u", password="p", database="d")
    assert uri == "postgresql://u:p@h:5432/d"


def test_sqlalchemy_uri_keeps_query_params():
    uri = construct_sql_uri(database_type="postgresql", host="h", username="u", database="d", ssl_enabled=True)
    assert get_sqlalchemy_uri(uri) == uri
