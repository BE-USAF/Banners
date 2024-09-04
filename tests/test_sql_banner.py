"""Tests for the PostgresBanners."""

import os
import time

import pytest
from sqlalchemy import inspect

from banners import PostgresBanner


@pytest.fixture(name="second_sql_banner")
def fixture_second_sql_banner(sql_table):
    """Generate and cleanup a default banner using PostgresBanner"""
    banner2 = PostgresBanner(table_name=sql_table)
    yield banner2
    # This forces thread deletion.
    # pylint: disable-next=unnecessary-dunder-call
    banner2.__del__()


@pytest.fixture(name="remove_conn_string")
def fixture_remove_conn_string():
    """Remove connection string from environ"""
    conn_str = None
    if "SQL_CONNECTION_STRING" in os.environ:
        conn_str = os.environ.pop("SQL_CONNECTION_STRING")
    yield True
    if conn_str:
        os.environ["SQL_CONNECTION_STRING"] = conn_str


def test_sql_engine_creation_fail(sql_table, remove_conn_string):
    """Test for SQL Engine value error"""
    assert "SQL_CONNECTION_STRING" not in os.environ
    if remove_conn_string:
        with pytest.raises(ValueError):
            PostgresBanner(table_name=sql_table)

def test_sql_engine_kwargs(sql_table, remove_conn_string):
    """Test for SQL Engine value error"""
    assert "SQL_CONNECTION_STRING" not in os.environ

    connection_params = ["SQL_USER", "SQL_PASSWORD",
                         "SQL_HOSTNAME", "SQL_DATABASE"]
    if all(x in os.environ for x in connection_params) and remove_conn_string:
        PostgresBanner(
            table_name=sql_table,
            username=os.environ["SQL_USER"],
            password=os.environ["SQL_PASSWORD"],
            host=os.environ["SQL_HOSTNAME"],
            database=os.environ["SQL_DATABASE"],
        )


def test_sql_when_table_not_exist(sql_table):
    """Test that the banner class creates a new table"""
    banner = PostgresBanner(table_name=sql_table)
    # pylint: disable-next=protected-access
    inspector = inspect(banner._engine)
    schemas = inspector.get_schema_names()

    all_tables = []
    for schema in schemas:
        for table_name in inspector.get_table_names(schema=schema):
            all_tables.append(table_name)

    assert sql_table in all_tables


def test_sql_get_event_bad_id(sql_banner):
    """Verify get event throws error with bad id"""
    error_msg = "Event ID 0 not found"
    with pytest.raises(ValueError, match=error_msg):
        ## Ignore pylint because we're testing this function
        # pylint: disable-next=protected-access
        sql_banner._get_event_by_id(0)


def test_sql_watch_callback_called(sql_banner, second_sql_banner):
    """Verify watch hits the supplied callback"""
    ## Only using this global to modify within the nested function.
    # pylint: disable-next=global-variable-undefined
    global TEST_CALLBACK_COUNTER
    TEST_CALLBACK_COUNTER = 0
    def test_cb(data):
        ## Only using this global to modify within the nested function.
        # pylint: disable-next=global-variable-undefined
        global TEST_CALLBACK_COUNTER
        TEST_CALLBACK_COUNTER += 1
    second_sql_banner.watch_rate = 0.1

    second_sql_banner.watch("test", test_cb)
    time.sleep(0.1)
    sql_banner.wave("test")
    time.sleep(second_sql_banner.watch_rate+0.1)
    assert TEST_CALLBACK_COUNTER == 1
    second_sql_banner.ignore("test")
