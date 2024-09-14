"""Implementation of Postgres Banner."""

import copy
import json
import os
import select
import threading
from typing import Callable, Optional

from sqlalchemy import URL, create_engine, text
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
        self._engine = None
        self._exit_event = threading.Event()
        self._exit_event.set()
        self._thread = None
        self._create_engine(**kwargs)

        self.banner_event = self._create_table(self.table_name)

    def __del__(self):
        """Destructor that kills DB connection."""
        super().__del__()
        if not self._exit_event.is_set():
            self._exit_event.set()
        if self._thread is not None:
            self._thread.join()
        if self._engine:
            self._engine.dispose()

    def _create_engine(self, **kwargs):
        """Create SQL Alchemy Engine.

        The engine can be created by two inputs.
        1) Inputting directly the connection parameters as kwargs
        2) Using the environment variable SQL_CONNECTION_STRING

        Parameters
        ----------
        **username: str (optional)
            Username of PostgreSQL database
        **password: str (optional)
            Password of PostgreSQL database
        **host: str (optional)
            PostgreSQL database host
        **database: str (optional)
            PostgreSQL database
        """
        connection_params = ["username", "password",
                             "host", "database"]
        if all(x in kwargs for x in connection_params):
            self._engine = create_engine(URL.create(
                drivername="postgresql",
                username=kwargs["username"],
                password=kwargs["password"],
                host=kwargs["host"],
                database=kwargs["database"],
                port=kwargs.get("port", 5432)
            ))
            return
        if "SQL_CONNECTION_STRING" in os.environ:
            self._engine = create_engine(
                os.environ["SQL_CONNECTION_STRING"]
            )
            return
        raise ValueError((
            "SQL Engine could not be constructed. "
            "Must provide connection params in kwargs, "
            "or env variable SQL_CONNECTION_STRING."
        ))

    def _create_table(self, table_name):
        """Create SQL Alchemy ORM tables and objects.

        Parameters
        ----------
        table_name: str
            The name for the table.

        Returns
        ----------
        The SQL Alchemy ORM object class.
        """
        # pylint: disable-next=too-few-public-methods
        class Base(DeclarativeBase):
            """SQL Alchemy base class to create tables."""

        # pylint: disable-next=too-few-public-methods
        class BannerEvent(Base):
            """SQL Alchemy model."""
            __tablename__ = table_name

            id: Mapped[int] = mapped_column(primary_key=True)
            topic: Mapped[str]
            timestamp: Mapped[str]
            body: Mapped[Optional[str]]

        Base.metadata.create_all(self._engine)
        return BannerEvent

    def _get_event_by_id(self, event_id: int):
        """Query an event by id.

        Parameters
        ----------
        event_id: int
            ID to query.

        Returns
        ----------
        The SQL Alchemy ORM object of the saved event.
        """
        with self._engine.connect() as connection:
            with Session(bind=connection) as session:
                res = session.query(self.banner_event) \
                             .where(self.banner_event.id == event_id)
                if res.count() == 0:
                    raise ValueError(f"Event ID {event_id} not found")
                out = self._convert_sql_object_to_dict(res[0])
        return  out

    def _convert_sql_object_to_dict(self, obj):
        """Convert SQLAlchemy ORM object to dictionary.

        Parameters
        ----------
        obj: BannerEvent
            ORM Object to convert.

        Returns
        ----------
        A dictionary of the event.
        """
        return  {
                'topic': obj.topic,
                'banner_timestamp': obj.timestamp,
                **json.loads(obj.body)
        }

    def _add_event_to_table(self, body):
        """Add event to the SQL table.

        Parameters
        ----------
        body: dict
            Information to save.

        Returns
        ----------
        The id of the saved event.
        """
        ## Add to sql table
        timestamp = body.pop("banner_timestamp")
        topic = body.pop("topic")
        with self._engine.connect() as connection:
            with Session(bind=connection) as session:
                event = self.banner_event(
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

        event_id = self._add_event_to_table(copy.deepcopy(body))

        with self._engine.connect() as con:
            con.execute(
                text(f"NOTIFY {topic}, '{event_id}';")
            )
            con.commit()
        self.retire(topic)

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
        self.watched_topics[topic] = callback

        if self._exit_event.is_set():
            self._exit_event.clear()
            self._thread = threading.Thread(
                        target=self._watch_thread,
                        name="banners_watch_sql",
                        args=("sql", callback, start_time),
            )
            self._thread.start()

        with self._engine.connect() as con:
            con.execute(text(f"LISTEN {topic};"))
            con.commit()

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
        while not self._exit_event.is_set():
            empty = ([],[],[])
            with self._engine.connect() as conn:
                if not select.select(
                    [conn.connection],[],[],self.watch_rate
                ) == empty:
                    conn.connection.poll()
                    while conn.connection.notifies:
                        notify = conn.connection.notifies.pop(0)
                        if notify.channel in self.watched_topics:
                            callback = self.watched_topics[notify.channel]
                            callback(self._get_event_by_id(notify.payload))

    def ignore(self, topic: str):
        """Unsubscribe from a topic.

        Parameters
        ----------
        topic: str
            Topic to ignore.
        """
        if topic not in self.watched_topics:
            return
        self.watched_topics.pop(topic)
        with self._engine.connect() as con:
            con.execute(text(f"UNLISTEN {topic};"))
            con.commit()
        if not self.watched_topics: # If no more watched topics, kill thread
            self._exit_event.set()

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

        with self._engine.connect() as connection:
            with Session(bind=connection) as session:
                total_rows = session.query(self.banner_event).count()

                if num_keep >= total_rows:
                    return

                res = session.query(self.banner_event) \
                       .where(self.banner_event.topic == topic) \
                       .order_by(self.banner_event.timestamp) \
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

        with self._engine.connect() as connection:
            with Session(bind=connection) as session:
                results = session.query(self.banner_event) \
                       .where(self.banner_event.topic == topic) \
                       .order_by(self.banner_event.timestamp.desc()) \
                       .limit(num_retrieve)[::-1]

        return [
            self._convert_sql_object_to_dict(res)
            for res in results
        ]
