"""Tests for the base Banners functionality"""

import os
import threading
import time

import pytest

from banners import LocalBanner


@pytest.fixture(name="banner")
def fixture_banner(tmp_path):
    """Generate and cleanup a default banner using LocalBanner"""
    banner = LocalBanner(root_path=tmp_path)
    yield banner
    ## This forces thread deletion.
    # pylint: disable-next=unnecessary-dunder-call
    banner.__del__()

@pytest.fixture(name="loaded_banner")
def fixture_loaded_banner(banner):
    """Load the default banner with 10 events"""
    banner.max_events_in_topic = 10
    for i in range(banner.max_events_in_topic):
        banner.wave("test", {"event": i})
    yield banner

def test_validate_body_all_fields(banner):
    """Verify validate body"""
    good_body = {"random": 4, "topic": "test"}
    # Disabling pylint to test the given function
    # pylint: disable-next=protected-access
    banner._validate_body(good_body)

@pytest.mark.parametrize("body, error_msg", [
    ({"random": 4}, "Required field topic not found in body"),
    ({"topic": 4}, "Field topic is wrong type, must be str"),
])
def test_validate_body_fail_cases(banner, body, error_msg):
    """Verify fail cases for validate body"""
    with pytest.raises(ValueError, match=error_msg):
        # Disabling pylint to test the given function
        # pylint: disable-next=protected-access
        banner._validate_body(body)


def test_del_removes_threads():
    """Verify __del__ removes all watch threads"""
    banner = LocalBanner()
    banner.watch_rate = 0.05
    test_threads = ["test1", "test2", "test3"]
    for test_thread in test_threads:
        banner.watch(test_thread, lambda a: None)
    time.sleep(0.05)

    ## This forces thread deletion.
    # pylint: disable-next=unnecessary-dunder-call
    banner.__del__()
    time.sleep(0.05)
    threads = [t.name for t in threading.enumerate()]
    for test_thread in test_threads:
        assert f"banners_watch_{test_thread}" not in threads

@pytest.mark.parametrize("body", [(None), ({"data": "value"})])
def test_wave_(banner, body):
    """Verify wave can be used with recall_events"""
    banner.retire("test", 0)
    banner.wave("test", body)
    waved_banners = os.listdir(os.path.join(banner.root_path, "test"))
    assert len(waved_banners) == 1

    recalled_body = banner.recall_events("test", 1)
    if body is None:
        body = {}
    body['topic'] = 'test'
    assert recalled_body[0] == body


def test_wave_auto_retires(banner):
    """Verify wave auto retires old events"""
    banner.max_events_in_topic = 2
    for i in range(banner.max_events_in_topic):
        banner.wave("test", {"iter": i})
    assert len(banner.recall_events("test", 10)) == banner.max_events_in_topic
    banner.wave("test", {"iter": "new"})
    assert len(banner.recall_events("test", 10)) == banner.max_events_in_topic


def test_watch_existing_topic(banner):
    """Verify watch on watched topic throws error"""
    banner.watch("test", lambda a: None)

    error_msg = "Topic: test already being watched"
    with pytest.raises(ValueError, match=error_msg):
        banner.watch("test", lambda a: None)

def test_watch_callback_called(banner):
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
    banner.watch_rate = 0.2
    banner.watch("test", test_cb)
    time.sleep(0.1)
    banner.wave("test")
    time.sleep(0.5)
    assert TEST_CALLBACK_COUNTER == 1


def test_watch_spawns_thread(banner):
    """verify watch creates a watcher thread"""
    existing_threads = [t.name for t in threading.enumerate()]
    assert "banners_watch_test" not in existing_threads

    banner.watch_rate = 0.05
    banner.watch("test", lambda a: None)

    new_threads = [t.name for t in threading.enumerate()]
    assert "banners_watch_test" in new_threads


@pytest.mark.parametrize("watch_rate", [(0.1), (0.5)])
def test_watch_sleeps(banner, watch_rate):
    """Verify the watch_rate changes the cycle time"""
    ## Only using this global to modify within the nested function.
    # pylint: disable-next=global-variable-undefined
    global end_time
    end_time = None
    def test_cb(data):
        ## Only using this global to modify within the nested function.
        # pylint: disable-next=global-variable-undefined
        global end_time
        end_time = time.time()
    banner.watch_rate = watch_rate
    banner.watch("test", test_cb)
    banner.wave("test")
    start_time = time.time()
    while end_time is None and time.time() - start_time < banner.watch_rate:
        time.sleep(0.02)

    assert end_time - start_time < banner.watch_rate + 0.01


def test_ignore_with_nonexistant_topic(banner):
    """Ignoring a nonexistant topic should not fail"""
    banner.ignore("BAD_TOPIC")


def test_ignore_removes_topic(banner):
    """Verify ignore deletes indicated thread"""
    banner.watch_rate = 0.05
    banner.watch("test", lambda a: None)
    banner.ignore("test")
    time.sleep(0.05)
    threads = [t.name for t in threading.enumerate()]
    assert "banners_watch_test" not in threads


@pytest.mark.parametrize("arg_val, expected", [
    (3, 3),
    (0, 0),
    (-1, 10),
    (None, 5),
])
def test_retire_with_input(loaded_banner, arg_val, expected):
    """Test the function defaults to max_events_in_topic"""
    loaded = loaded_banner
    if arg_val is None:
        loaded.max_events_in_topic = expected
    loaded.retire("test", arg_val)
    after_recall = len(os.listdir(os.path.join(loaded.root_path, "test")))
    assert after_recall == expected


@pytest.mark.parametrize("arg_val, expected", [
    (3, 3),
    (None, 10),
    (11, 10),
])
def test_recall_events(loaded_banner, arg_val, expected):
    """Verify the last N events are recalled"""
    events = loaded_banner.recall_events("test", arg_val)
    assert len(events) == expected


@pytest.mark.parametrize("arg_val", [(0), (-1)])
def test_recall_events_errors(loaded_banner, arg_val):
    """Verify positive integers required"""
    error_msg = f"Recall number must be a positive integer, input: {arg_val}"
    with pytest.raises(ValueError, match=error_msg):
        loaded_banner.recall_events("test", arg_val)

## TODO: Add test for nonexistant topic. Add test for timestamp in default body