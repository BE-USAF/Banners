"""Base class for banners."""

import abc
import threading
from datetime import datetime
from typing import Callable

class BaseBanner(abc.ABC):
    """BaseBanner class, used to enforce I/O for Banner instantiations."""
    def __init__(self, **kwargs):
        """Initializer for the base class.

        Parameters
        ----------
        **max_events_in_topic: int (default=50)
            The maximum number of events before they are deleted automatically
        **watch_rate: int (default=5)
            The cycle rate to repeat looking for new events
        """
        # Consider implementing with a singleton, at least for wathed_topics
        self.watched_topics = {}
        self.max_events_in_topic = kwargs.get('max_events_in_topic', 50)
        self.watch_rate = kwargs.get('watch_rate', 5)

        ## Alias Functions
        self.publish = self.wave
        self.subscribe = self.watch
        self.cleanup = self.retire

    def __del__(self):
        """Destructor that kills all active threads."""
        self.watch_rate = 0
        topics = list(self.watched_topics.keys())
        for topic in topics:
            self.ignore(topic)

    @abc.abstractmethod
    def wave(self, topic: str, body: dict = None):
        """Create a new event in a given topic.

        Parameters
        ----------
        topic: str
            Topic under which to publish the new event.
        body: dict
            Information to publish to the topic.
        """
        raise NotImplementedError

    def _validate_body(self, body: dict, topic: str):
        """Validate new event body.

        Enforce toipc and banner_timestamp are present.

        Parameters
        ----------
        body: dict
            Information to publish to the topic.
        topic: str
            Topic being published to
        """
        if body is None:
            body = {}
        if "topic" not in body:
            body['topic'] = topic
        if "banner_timestamp" not in body:
            body['banner_timestamp'] = self._generate_timestamp_string()
        return body

    def watch(self, topic: str,
              callback: Callable[dict, None],
              start_time: str="") -> None:
        """Subscribe to a new topic.

        Spawn a new thread that watches for a new topic.

        Parameters
        ----------
        topic: str
            Topic to watch.
        callback: Callable[dict, None]:
            Callback function to execute when new data is available.
        start_time: str (default="")
            Timestamp to ignore previous events
        """
        if topic in self.watched_topics:
            raise ValueError(f"Topic: {topic} already being watched")
        self.watched_topics[topic] = {
            "thread": threading.Thread(
                        target=self._watch_thread,
                        name=f"banners_watch_{topic}",
                        args=(topic, callback, start_time),
                      ),
            "event": threading.Event()
        }
        self.watched_topics[topic]['thread'].start()

    @abc.abstractmethod
    def _watch_thread(self, topic: str,
              callback: Callable[dict, None],
              start_time: str="") -> None:
        """Abstracted implementation of logic to watch for new events.

        Parameters
        ----------
        topic: str
            Topic to watch.
        callback: Callable[dict, None]:
            Callback function to execute when new data is available.
        start_time: str (default="")
            Timestamp to ignore previous events
        """
        raise NotImplementedError

    def ignore(self, topic: str):
        """Unsubscribe from a topic.

        Parameters
        ----------
        topic: str
            Topic to ignore.
        """
        if topic not in self.watched_topics:
            return
        thread = self.watched_topics.pop(topic)
        thread['event'].set()
        thread['thread'].join()

    @abc.abstractmethod
    def retire(self, topic: str, num_keep: int =10):
        """Delete old events in a given topic.

        Parameters
        ----------
        topic: str
            Topic to clean up.
        num_keep: int (default=10)
            Number of events to keep in the topic
        """
        raise NotImplementedError

    def _generate_timestamp_string(self) -> str:
        """Create a timestamp of the current time.

        Timestamp is formatted is %Y%m%d-%H%M%S%f

        Returns
        -------
        A string of the current timestamp
        """
        return datetime.now().strftime("%Y%m%d-%H%M%S%f")

    @abc.abstractmethod
    def recall_events(self, topic: str, num_retrieve: int=None):
        """Get the most recent N events in the topic.

        Parameters
        ----------
        topic: str
            Topic to recall events.
        num_retrieve: int (default=None)
            Number of events to retrieve. None returns max_events_in_topic
        Returns
        -------
        A list of events
        """
        raise NotImplementedError
