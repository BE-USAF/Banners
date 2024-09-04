"""Fixtures common for the BaseBanner validation"""

import random

import pytest
import s3fs

from banners import LocalBanner, S3Banner, PostgresBanner


@pytest.fixture(name="local_banner")
def fixture_local_banner(tmp_path):
    """Generate and cleanup a default banner using LocalBanner"""
    banner = LocalBanner(root_path=tmp_path)
    yield banner
    ## This forces thread deletion.
    # pylint: disable-next=unnecessary-dunder-call
    banner.__del__()


@pytest.fixture(name="s3_bucket")
def fixture_s3_bucket():
    """Generate and cleanup a default bucket for S3 testing"""
    s3 = s3fs.S3FileSystem(
        # client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
    )
    bucket_name = f"banners-test-bucket-{random.randint(0,100000)}"
    s3.mkdir(bucket_name)
    yield bucket_name
    s3.rm(bucket_name, recursive=True)


@pytest.fixture(name="s3_banner")
def fixture_s3_banner(s3_bucket):
    """Generate and cleanup a default banner using S3Banner"""
    banner = S3Banner(root_path=s3_bucket)
    yield banner
    # This forces thread deletion.
    # pylint: disable-next=unnecessary-dunder-call
    banner.__del__()


@pytest.fixture(name="sql_table")
def fixture_sql_table():
    """Generate and cleanup a default bucket for S3 testing"""
    table_name = f"sql_banner_test_{random.randint(0,100000)}"
    yield table_name
    banner = PostgresBanner(table_name=table_name)
    ## Disabling so we can cleanup
    # pylint: disable-next=protected-access
    banner.banner_event.__table__.drop(banner._engine)


@pytest.fixture(name="sql_banner")
def fixture_sql_banner(sql_table):
    """Generate and cleanup a default banner using PostgresBanner"""
    banner = PostgresBanner(table_name=sql_table)
    yield banner
    # This forces thread deletion.
    # pylint: disable-next=unnecessary-dunder-call
    banner.__del__()


@pytest.fixture(name="banner", params=['local_banner', 's3_banner', "sql_banner"])
def fixture_banner(request):
    """Parameterize the inherited banner tests"""
    return request.getfixturevalue(request.param)


@pytest.fixture(name="loaded_banner")
def fixture_loaded_banner(banner):
    """Load the default banner with 10 events"""
    banner.max_events_in_topic = 10
    for i in range(banner.max_events_in_topic):
        banner.wave("test", {"event": i})
    yield banner
