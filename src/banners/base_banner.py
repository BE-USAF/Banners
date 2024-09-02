import abc
from datetime import datetime
from typing import Callable

class BaseBanner(abc.ABC):
    def __init__(self, **kwargs):
        self.watched_topics = {}
        self.max_events_in_topic = kwargs.get('max_events_in_topic', 50)
        self.watch_rate = kwargs.get('watch_rate', 5)

        ## Alias Functions
        self.publish = self.wave
        self.subscribe = self.watch
        self.cleanup = self.retire

    def __del__(self):
        self.watch_rate = 0
        topics = list(self.watched_topics.keys())
        for topic in topics:
            self.ignore(topic)

    @abc.abstractmethod
    def wave(self, topic: str, body: dict = None):
        raise NotImplementedError

    def _validate_body(self, body: dict) -> bool:
        required_fields = {
            "topic": str
        }
        for k, v in required_fields.items():
            if k not in body:
                raise ValueError(f"Required field {k} not found in body")
            if not isinstance(body[k], v):
                raise ValueError(
                    f"Field {k} is wrong type, must be {v.__name__}"
                )

    @abc.abstractmethod
    def watch(self, topic: str,
              callback: Callable[dict, None],
              start_time: str="") -> None:
        raise NotImplementedError

    def ignore(self, topic: str):
        if topic not in self.watched_topics:
            return
        thread = self.watched_topics.pop(topic)
        thread['event'].set()
        thread['thread'].join()

    @abc.abstractmethod
    def retire(self, topic: str, num_keep: int =10):
        raise NotImplementedError

    def _generate_timestamp_string(self) -> str:
        return datetime.now().strftime("%Y%m%d-%H%M%S%f")

    @abc.abstractmethod
    def recall_events(self, topic: str, num_retrieve: int=None):
        raise NotImplementedError
