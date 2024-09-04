import json
import os
import tempfile
from pathlib import Path

from .base_banner import BaseBanner


class LocalBanner(BaseBanner):
    def __init__(self, **kwargs):
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
        file_name = self._generate_timestamp_string()
        if body is None:
            body = {}
        if "topic" not in body:
            body['topic'] = topic
        if "banner_timestamp" not in body:
            body['banner_timestamp'] = file_name
        self._validate_body(body)
        topic_path = Path(self.root_path)  / topic
        topic_path.mkdir(exist_ok=True)
        file_path = topic_path / (file_name + ".json")
        file_path.write_text(json.dumps(body))
        self.retire(topic)

    def _watch_thread(self, topic, callback, start_time):
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
                with open(os.path.join(topic_folder, file)) as f:
                    callback(json.load(f))

    def retire(self, topic: str, num_keep: int=None) -> None:
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
        if num_retrieve is None:
            num_retrieve = self.max_events_in_topic

        if num_retrieve < 1:
            error_msg = "Recall number must be a positive integer, input: "
            raise ValueError(error_msg + str(num_retrieve))

        topic_folder = os.path.join(self.root_path, topic)
        if not os.path.exists(topic_folder):
            return []
        topic_files = sorted(os.listdir(topic_folder)[-num_retrieve:])
        out = []
        for file in topic_files:
            with open(os.path.join(topic_folder, file)) as f:
                out.append(json.load(f))
        return out