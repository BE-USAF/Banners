"""Implementation of LocalBanner"""

import json
import os
import tempfile
from pathlib import Path
from typing import Callable

from .base_banner import BaseBanner


class LocalBanner(BaseBanner):
    """Banner implementation that uses a local filesystem"""
    def __init__(self, **kwargs):
        """Initializer for LocalBanner.

        Parameters
        ----------
        **root_path: str (default=banners)
            The folder path to look for banner events
        """
        # Get root_path from 1) Kwarg, 2) Env, 3) default
        super().__init__(**kwargs)
        self.root_path = kwargs.get(
            "root_path", os.environ.get(
                "root_path",
                os.path.join(tempfile.gettempdir(), "banners")
            )
        )
        Path(self.root_path).mkdir(exist_ok=True)

    def wave(self, topic: str, body: dict = None) -> None:
        """Create a new event in a given topic.

        Parameters
        ----------
        topic: str
            Topic under which to publish the new event.
        body: dict
            Information to publish to the topic.
        """
        body = self._validate_body(body, topic)

        topic_path = Path(self.root_path)  / topic
        topic_path.mkdir(exist_ok=True)
        file_name = self._generate_timestamp_string()
        file_path = topic_path / (file_name + ".json")
        file_path.write_text(json.dumps(body))

        self.retire(topic)

    def _watch_thread(self, topic: str,
              callback: Callable[dict, None],
              start_time: str="") -> None:
        """Look for events by ls'ing the topic directory

        Parameters
        ----------
        topic: str
            Topic to watch.
        callback: Callable[dict, None]:
            Callback function to execute when new data is available.
        start_time: str (default="")
            Timestamp to ignore previous events
        """
        topic_folder = os.path.join(self.root_path, topic)
        exit_event = self.watched_topics[topic]['event']

        ## Loop until the thread is removed, or the event is thrown
        while topic in self.watched_topics and not exit_event.is_set():
            exit_event.wait(self.watch_rate)
            if not os.path.exists(topic_folder):
                continue
            topic_files = sorted(os.listdir(topic_folder))
            new_files = [f for f in topic_files if f > start_time]
            for file in new_files:
                # Ignore old files
                if Path(file).stem <= start_time:
                    continue
                start_time = Path(file).stem # Update start time

                # Load json into callback
                file_path = os.path.join(topic_folder, file)
                with open(file_path, encoding="utf-8") as f:
                    callback(json.load(f))

    def retire(self, topic: str, num_keep: int=None) -> None:
        """Delete old events in a given topic.

        Parameters
        ----------
        topic: str
            Topic to clean up.
        num_keep: int (default=10)
            Number of events to keep in the topic
        """
        if num_keep is None:
            num_keep = self.max_events_in_topic
        if num_keep < 0: # Do not delete if num_keep is negative
            return
        topic_folder = os.path.join(self.root_path, topic)
        if not os.path.exists(topic_folder):
            return
        topic_files = os.listdir(topic_folder)
        files_to_delete = topic_files[:-num_keep or None]
        for file in files_to_delete:
            (Path(topic_folder) / file).unlink()

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
        num_retrieve = self._verify_recall_num_retrieve(num_retrieve)

        topic_folder = os.path.join(self.root_path, topic)
        if not os.path.exists(topic_folder):
            return []
        topic_files = sorted(os.listdir(topic_folder)[-num_retrieve:])
        out = []
        for file in topic_files:
            file_path = os.path.join(topic_folder, file)
            with open(file_path, encoding="utf-8") as f:
                out.append(json.load(f))
        return out
