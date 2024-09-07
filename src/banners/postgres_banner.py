"""Implementation of Postgres Banner."""

import copy
import json
import select
from typing import Callable, Optional

import psycopg2
from psycopg2 import sql
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

from .base_banner import BaseBanner

class PostgresBanner(BaseBanner):
    """Banner implementation that uses an PostgresSQL table"""
    def __init__(self, **kwargs):
        """Initializer for PostgresBanner.

        Parameters
        ----------
        **table_name: str (default=sql_banner)
            The folder path to look for banner events
        """
        super().__init__(**kwargs)
        self.table_name = kwargs.get("table_name", "sql_banner")
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
        self._engine = create_engine(URL.create(
            drivername="postgresql",
            username="postgres",
            password="postgres",
            host="jhub-postgresql",
            database="postgres",
        ))
        self.sql_banner = self._create_table(self.table_name)

    def _create_table(self, table_name):
        class Base(DeclarativeBase):
            pass

        class SQLBanner(Base):
            __tablename__ = table_name

            id: Mapped[int] = mapped_column(primary_key=True)
            topic: Mapped[str]
            timestamp: Mapped[str]
            body: Mapped[Optional[str]]

            def __repr__(self) -> str:
                return (f"SQLBanner(topic={self.topic!r}, "
                        f"timestamp={self.timestamp!r}, body={self.body!r})")

        Base.metadata.create_all(self._engine)
        return SQLBanner

    def _get_event_by_id(self, event_id: int):
        with Session(self._engine) as session:
            res = session.query(self.sql_banner) \
                         .where(self.sql_banner.id == event_id)[0]
        return  self._convert_sql_object_to_dict(res)

    def _convert_sql_object_to_dict(self, obj):
        return  {
                'topic': obj.topic,
                'banner_timestamp': obj.timestamp,
                **json.loads(obj.body)
        }

    def _add_event_to_table(self, body):
        ## Add to sql table
        timestamp = body.pop("banner_timestamp")
        topic = body.pop("topic")
        with Session(self._engine) as session:
            event = self.sql_banner(
                topic=topic,
                timestamp=timestamp,
                body=json.dumps(body)
            )
            session.add(event)
            session.commit()
            event_id = event.id
        return event_id

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
        event_id = self._add_event_to_table(copy.deepcopy(body))
        curs.execute(
            sql.SQL("NOTIFY {}, %s;").format(sql.Identifier(topic)),
            (str(event_id),)
        )
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
        exit_event = self.watched_topics[topic]['event']

        curs = self.wave_conn.cursor()
        curs.execute(sql.SQL("LISTEN {};").format(sql.Identifier(topic)))

        ## Loop until the thread is removed, or the event is thrown
        while topic in self.watched_topics and not exit_event.is_set():
            empty = ([],[],[])
            if select.select([self.wave_conn],[],[],self.watch_rate) == empty:
                continue
            self.wave_conn.poll()
            while self.wave_conn.notifies:
                notify = self.wave_conn.notifies.pop(0)
                callback(self._get_event_by_id(notify.payload))

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
        if num_keep < 0:
            return

        with Session(self._engine) as session:
            total_rows = session.query(self.sql_banner).count()

            if num_keep >= total_rows:
                return

            res = session.query(self.sql_banner) \
                   .where(self.sql_banner.topic == topic) \
                   .order_by(self.sql_banner.timestamp) \
                   .limit(total_rows-num_keep)[:]
            for obj in res:
                session.delete(obj)
            session.commit()

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

        with Session(self._engine) as session:
            results = session.query(self.sql_banner) \
                   .where(self.sql_banner.topic == topic) \
                   .order_by(self.sql_banner.timestamp.desc()) \
                   .limit(num_retrieve)[::-1]
        return [
            self._convert_sql_object_to_dict(res)
            for res in results
        ]
