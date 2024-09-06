"""Implementation of Postgres Banner."""

import json
import os
import select
from pathlib import Path
from typing import Callable

import psycopg2
from psycopg2 import sql
import s3fs

from .base_banner import BaseBanner



class PostgresBanner(BaseBanner):
    """Banner implementation that uses an S3 filesystem"""
    def __init__(self, **kwargs):
        """Initializer for S3Banner.

        Parameters
        ----------
        **root_path: str (default=banners)
            The folder path to look for banner events
        """
        super().__init__(**kwargs)
        # Parameterize this connection
        self.wave_conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="postgres",
            host="jhub-postgresql"
        )
        ## Consider using connection pool
        self.wave_conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        )

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

        curs = self.wave_conn.cursor()
        curs.execute(
            sql.SQL("NOTIFY {}, %s;").format(sql.Identifier(topic)),
            (json.dumps(body),)
        )
        # self.retire(topic)

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
        exit_event = self.watched_topics[topic]['event']

        curs = self.wave_conn.cursor()
        curs.execute(sql.SQL("LISTEN {};").format(sql.Identifier(topic)))

        ## Loop until the thread is removed, or the event is thrown
        while topic in self.watched_topics and not exit_event.is_set():
            empty = ([],[],[])
            if select.select([self.wave_conn],[],[],self.watch_rate) == empty:
                continue
            else:
                self.wave_conn.poll()
                while self.wave_conn.notifies:
                    notify = self.wave_conn.notifies.pop(0)
                    callback(json.loads(notify.payload))


    def retire(self, topic: str, num_keep: int=None) -> None:
        """Delete old events in a given topic.

        Parameters
        ----------
        topic: str
            Topic to clean up.
        num_keep: int (default=10)
            Number of events to keep in the topic
        """
        ## TODO
        return

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
        # TODO
        return []
