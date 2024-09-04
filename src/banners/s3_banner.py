import json
import os
from pathlib import Path

import s3fs

from .base_banner import BaseBanner


class S3Banner(BaseBanner):
    def __init__(self, **kwargs):
        # Get root_path from 1) Kwarg, 2) Env, 3) default
        super().__init__(**kwargs)
        self.root_path = kwargs.get(
            "root_path",
            os.environ.get("root_path", "banners")
        )
        self.s3 = s3fs.S3FileSystem(
            client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
        )

        if not self.s3.exists(self.root_path):
            self.s3.mkdir(self.root_path, create_parents=True)

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
        if not self.s3.exists(topic_path):
            self.s3.mkdir(topic_path)
        file_path = topic_path / (file_name + ".json")
        with self.s3.open(file_path, "wt") as f:
            json.dump(body, f)
        self.retire(topic)

    def _watch_thread(self, topic, callback, start_time):
        topic_folder = "/".join([self.root_path, topic])
        exit_event = self.watched_topics[topic]['event']

        ## Loop until the thread is removed, or the event is thrown
        while topic in self.watched_topics and not exit_event.is_set():
            exit_event.wait(self.watch_rate)
            if not self.s3.exists(topic_folder):
                continue
            topic_files = sorted(self.s3.ls(topic_folder))
            for file in topic_files:
                # Ignore old files
                if Path(file).stem <= start_time:
                    continue
                start_time = Path(file).stem # Update start time

                # Load json into callback
                with self.s3.open(file) as f:
                    callback(json.load(f))

    def retire(self, topic: str, num_keep: int=None) -> None:
        if num_keep is None:
            num_keep = self.max_events_in_topic
        if num_keep < 0: # Do not delete if num_keep is negative
            return
        topic_folder = "/".join([self.root_path, topic])
        if not self.s3.exists(topic_folder):
            return
        topic_files = sorted(self.s3.ls(topic_folder))
        files_to_delete = topic_files[:-num_keep or None]
        if len(files_to_delete) > 0:
            self.s3.rm(files_to_delete)

    def recall_events(self, topic: str, num_retrieve: int=None):
        if num_retrieve is None:
            num_retrieve = self.max_events_in_topic

        if num_retrieve < 1:
            error_msg = "Recall number must be a positive integer, input: "
            raise ValueError(error_msg + str(num_retrieve))

        topic_folder = "/".join([self.root_path, topic])
        if not self.s3.exists(topic_folder):
            return []
        topic_files = sorted(self.s3.ls(topic_folder)[-num_retrieve:])
        out = []
        for file in topic_files:
            with self.s3.open(file) as f:
                out.append(json.load(f))
        return out