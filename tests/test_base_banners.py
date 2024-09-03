"""Tests for the base Banners functionality"""

import copy
import os
import threading
import time

import pytest

from banners import LocalBanner
from .conftest import fixture_banner, fixture_loaded_banner

def test_validate_body_all_fields(local_banner):
    """Verify validate body"""
    good_body = {"random": 4, "topic": "test", "banner_timestamp": "test"}
    # Disabling pylint to test the given function
    # pylint: disable-next=protected-access
    local_banner._validate_body(good_body)

@pytest.mark.parametrize("body, error_msg", [
    ({"skip_banner_timestamp": "test"},
         "Required field banner_timestamp not found in body"),
    ({"skip_topic": "test"}, "Required field topic not found in body"),
    ({"topic": 4}, "Field topic is wrong type, must be str"),
    ({"banner_timestamp": 4},
         "Field banner_timestamp is wrong type, must be str"),
])
def test_validate_body_fail_cases(local_banner, body, error_msg):
    """Verify fail cases for validate body"""
    test = {
        "topic": "test",
        "banner_timestamp": "test"
    }
    if "topic" in body:
        test['topic'] = body['topic']
    if "banner_timestamp" in body:
        test['banner_timestamp'] = body['banner_timestamp']
    if "skip_banner_timestamp" in body:
        test.pop("banner_timestamp")
    if "skip_topic" in body:
        test.pop("topic")
    with pytest.raises(ValueError, match=error_msg):
        # Disabling pylint to test the given function
        # pylint: disable-next=protected-access
        local_banner._validate_body(test)


def test_del_removes_threads(local_banner):
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
    if body is None:
        comp_body = {}
    else:
        comp_body = copy.deepcopy(body)
    banner.retire("test", 0)
    banner.wave("test", body)
    waved_banners = banner.recall_events("test", 100)
    assert len(waved_banners) == 1

    assert 'topic' in waved_banners[0]
    assert 'banner_timestamp' in waved_banners[0]
    if 'data' in comp_body:
        assert waved_banners[0]['data'] == comp_body['data']


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
    after_recall = len(loaded.recall_events("test", 1000))
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

def test_recall_events_nonexistant_topic(loaded_banner):
    """Verify nonexistant topic returns empty list"""
    assert loaded_banner.recall_events("BAD_VAL", 1) == []