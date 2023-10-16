import asyncio
import logging

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
    def unique_topics_count(self) -> int:
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
    async def process_subscribe(self, req: str) -> tuple[list[str], list[str]]:
        """Processes a subscribe request.

        Parameters
        ----------
        req : str
            the subscribe request

        Returns
        -------
        tuple[list[str], list[str]]
            a list of topics and a list of topics to unsubscribe
        """
        single_topics = await self.as_list(req)

        # handle topics with existing subcribers
        for topic in tuple(single_topics):
            if topic in self._topics:
                self._topics.append(topic)
                single_topics.remove(topic)

        topics_left = MAX_TOPICS_PER_CLIENT - self.unique_topics_count

        return single_topics[:topics_left], single_topics[topics_left:]

    async def process_unsubscribe(self, req: str) -> tuple[list[str], list[str]]:
        logger.debug("process_unsubscribe: ...")
        single_topics = await self.as_list(req)

        # handle topics with existing subcribers
        single_topics = [t for t in single_topics if await self.remove_subscriber(t)]

        return await self.batch_topics_str(single_topics) if single_topics else []

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
            logger.warning("%s not in topics" % topic)
            return False
        else:
            logger.debug(f"removed {topic} from topics")

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
        subject = topics[0].split(':')[0]
        stripped_subject = tuple(t.split(':')[1] for t in (topics))
        return [
            f"{subject}:{(',').join(stripped_subject[i: i + MAX_BATCH_SUBSCRIPTIONS])}"
            for i in range(0, len(topics), MAX_BATCH_SUBSCRIPTIONS)
        ]

    async def as_list(self, topic: str) -> list:
        if "," not in topic:
            return [topic]

        subject, topics_str = topic.split(":")[:2]
        return [f"{subject}:{t}" for t in topics_str.split(",")]

    async def as_string(self, topics: list[str]) -> str:
        return f"{topics[0].split(':')[0]}:{(',').join(t.split(':') for t in topics)}"


class KucoinWsClient:
    """ https://docs.kucoin.com/#websocket-feed """

    def __init__(self):
        self._callback = None
        self._conn = None
        self._loop = None
        self._client = None
        self._private = False
        self._topics: Topics = Topics()

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
        return self._conn.unique_topics

    async def _recv(self, msg):
        if 'data' in msg:
            await self._callback(msg)

    async def subscribe(self, topic):
        """Subscribe to a channel
        :param topic: required
        :type topic: str
        :returns: None
        """
        req_msg = {'type': 'subscribe', 'topic': None, 'response': True}
        to_subscribe, exceeds_topic_limit = await self._topics.process_subscribe(topic)

        for batch in to_subscribe:
            logger.info("subscribing to topic: %s", topic)
            req_msg["topic"] = batch
            await self._conn.send_message(req_msg)

        return exceeds_topic_limit

    async def unsubscribe(self, topic):
        """Unsubscribe from a topic

        :param topic: required
        :type topic: str
        :returns: None
        """
        req_msg = {'type': 'unsubscribe', 'topic': topic, 'response': True}
        for batch in await self._topics.process_unsubscribe(topic):
            logger.info("unsubscribing from topic: %s", topic)
            req_msg[topic] = batch
            await self._conn.send_message(req_msg)
