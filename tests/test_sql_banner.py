"""Tests for the PostgresBanners."""

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
    wave_banner = sql_banner
    watch_banner = second_sql_banner
    watch_banner.watch_rate = 0.1

    watch_banner.watch("test", test_cb)
    time.sleep(0.1)
    wave_banner.wave("test")
    time.sleep(watch_banner.watch_rate)
    assert TEST_CALLBACK_COUNTER == 1