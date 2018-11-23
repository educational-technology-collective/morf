import pytest
from morf.utils import get_bucket_from_url, get_key_from_url

def test_get_bucket_from_url():
    assert get_bucket_from_url("s3://my-bucket/some/file.txt") == "my-bucket"
    assert get_bucket_from_url("s3://anotherbucket/some/file.txt") == "anotherbucket"

def test_get_key_from_url():
    assert get_key_from_url("s3://my-bucket/some/file.txt") == "some/file.txt"
    with pytest.raises(AttributeError):
        get_key_from_url("s3://my-bucket/") # tests case of path without a key

