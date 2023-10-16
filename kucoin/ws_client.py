import asyncio
import logging

from itertools import count
from typing import Optional
from .websocket.websocket import ConnectWebsocket

MAX_BATCH_SUBSCRIPTIONS = 100  # 100 - max number of topics in a single request
MAX_TOPICS_PER_CLIENT = 300  # max number of topics for a single client/instance

logger = logging.getLogger("main.ws_client")


class TooManyRequests(Exception):
    ...


class Topics:
    """Helper class to keep track of the number of subscribers for each topic."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or logging.getLogger(__name__)

        self._topics: list = []
        self.warnings: int = 0

    def __repr__(self) -> str:
        return self._topics.__repr__()

    def __len__(self) -> int:
        return len(self._topics)

    def __contains__(self, topic) -> bool:
        return topic in self._topics

    @property
    def topics(self) -> list[str]:
        return self._topics

    @property
    def unique_topics(self) -> set[str]:
        return set(self._topics)

    @property
    def topics_count(self) -> int:
        return len(self.unique_topics)

    @property
    def batched_topics(self) -> list[list[str]]:
        """Returns a list of comma-separated strings of topic names.

        The sub-strings are of the length of MAX_BATCH_SUBSCRIPTIONS.
        """
        return self.batch_topics(self.topics_as_list)

    @property
    def batched_topics_str(self) -> list[str]:
        """Returns a list of comma-separated strings of topic names.

        The sub-strings are of the length of MAX_BATCH_SUBSCRIPTIONS.
        """
        return self.batch_topics_str(self.topics_as_list)

    # ..................................................................................
    async def add_subscriber(self, topic: str) -> bool:
        """Adds a subscriber for a topic.

        Parameters
        ----------
        topic : str
            the topic

        Returns
        -------
        bool
            True if this is the first subscriber for the topic, False otherwise.
        """
        self._topics.append(topic)
        return True if self._topics.count(topic) == 1 else False

    async def remove_subscriber(self, topic: str) -> int:
        """Removes a subscriber for a topic.

        Parameters
        ----------
        topic : str
            the topic

        Returns
        -------
        bool
            True if this was the last subscriber for the topic, False otherwise.
        """
        try:
            self.topics.remove(topic)
        except ValueError:
            return False

        return True if topic not in self._topics else False

    async def subscribers(self, topic: str) -> int:
        """Returns the number of subscribers for a topic."""
        return self._topics.count(topic)

    # ..................................................................................
    async def batch_topics(self, topics: list[str]) -> list[list[str]]:
        return [
            topics[i: i + MAX_BATCH_SUBSCRIPTIONS]
            for i in range(0, len(topics), MAX_BATCH_SUBSCRIPTIONS)
        ]

    async def batch_topics_str(self, topics: list[str]) -> list[str]:
        return [
            ",".join(topics[i: i + MAX_BATCH_SUBSCRIPTIONS])
            for i in range(0, len(topics), MAX_BATCH_SUBSCRIPTIONS)
        ]

    async def to_list(self, topics: str) -> list:
        return topics.split(",") if "," in topics else [topics]

    async def to_str(self, topics: list[str]) -> str:
        return ",".join(topics)


class KucoinWsClient:
    """ https://docs.kucoin.com/#websocket-feed """

    def __init__(self):
        self._callback = None
        self._conn = None
        self._loop = None
        self._client = None
        self._private = False
        self._topics = Topics()

    @classmethod
    async def create(cls, loop, client, callback, private=False):
        self = KucoinWsClient()
        loop = loop if loop else asyncio.get_running_loop()
        self._loop = loop
        self._client = client
        self._private = private
        self._callback = callback
        self._conn = ConnectWebsocket(loop, self._client, self._recv, private)
        return self

    @property
    def topics(self) -> list:
        return self._conn.topics

    async def _recv(self, msg):
        if 'data' in msg:
            await self._callback(msg)

    async def subscribe(self, topic):
        """Subscribe to a channel
        :param topic: required
        :type topic: str
        :returns: None
        """

        req_msg = {
            'type': 'subscribe',
            'topic': topic,
            'response': True
        }

        if "," in topic:
            prefix, topics = topic.split(":")
            single_topics = topics.split(",")

            for topic in single_topics:
                self._conn.topics.append(f"{prefix}:{topic}")
        else:
            self._conn.topics.append(topic)

        await self._conn.send_message(req_msg)

    async def unsubscribe(self, topic):
        """Unsubscribe from a topic

        :param topic: required
        :type topic: str
        :returns: None
        """
        endpoint, topics = topic.split(":")

        if "," in topic:
            prefix, topics = topic.split(":")
            single_topics = topics.split(",")

            for topic in single_topics:
                self._conn.topics.remove(f"{prefix}:{topic}")
        else:
            self._conn.topics.remove(topic)

        # req_msg = {
        #     'type': 'unsubscribe',
        #     'topic': topic,
        #     'response': True
        # }

        logger.info("subscribing to topic: %s", topic)
        # await self._conn.send_message(req_msg)
