"""Tests for the S3Banners."""

import random

import pytest
import s3fs

from banners import S3Banner


@pytest.fixture(name="new_s3_bucket")
def fixture_new_s3_bucket():
    """Generate and cleanup a default bucket for S3 testing"""
    bucket_name = f"banners-test-bucket-{random.randint(0,100000)}"
    yield bucket_name

    s3 = s3fs.S3FileSystem(
        # client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
    )
    s3.rm(bucket_name, recursive=True)

def test_s3_when_bucket_not_exist(new_s3_bucket):
    """Test that the banner class creates a new bucket"""
    S3Banner(root_path=new_s3_bucket)
