import asyncio
import logging

from typing import Optional, Iterator
from .websocket.websocket import ConnectWebsocket

MAX_BATCH_SUBSCRIPTIONS = 100  # 100 - max number of topics in a single request
MAX_TOPICS_PER_CLIENT = 300  # max number of topics for a single client/instance

logger = logging.getLogger("main.ws_client")
logger.setLevel(logging.DEBUG)


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

    def __iter__(self) -> Iterator[str]:
        return iter(self._topics)

    @property
    def topics(self) -> list[str]:
        return self._topics

    @property
    def unique_topics(self) -> list[str]:
        try:
            return list(set(self._topics))
        except TypeError:
            logger.error("unable to create set from: %s" % self._topics)

    @property
    def unique_topics_count(self) -> int:
        return len(self.unique_topics)

    @property
    def batched_topics(self) -> list[list[str]]:
        """Returns a list of comma-separated strings of topic names.

        The sub-strings are of the length of MAX_BATCH_SUBSCRIPTIONS.
        """
        return self.batch_topics(self.unique_topics)

    @property
    def batched_topics_str(self) -> list[str]:
        """Returns a list of comma-separated strings of topic names.

        The sub-strings are of the length of MAX_BATCH_SUBSCRIPTIONS.
        """
        return self.batch_topics_str(self.unique_topics)

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
            - one list of (batch-)strings that will be subscribed to
            - one list of single-topic strings that would exceed the
            limit of MAX_TOPICS_PER_CLIENT
        """
        single_topics = self.as_list(req)

        # handle topics with existing subcribers
        for topic in tuple(single_topics):
            if topic in self._topics:
                await self.add_subscriber(topic)
                single_topics.remove(topic)

        topics_left = MAX_TOPICS_PER_CLIENT - self.unique_topics_count

        return (
            self.batch_topics_str(single_topics[:topics_left]),
            single_topics[topics_left:]
        )

    async def process_unsubscribe(self, req: str) -> tuple[list[str], list[str]]:
        single_topics = self.as_list(req)

        # handle topics with existing subcribers
        single_topics = [t for t in single_topics if await self.remove_subscriber(t)]

        return self.batch_topics_str(single_topics) if single_topics else []

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

        subs = await self.subscribers(topic)
        if subs == 1:
            logger.info("created new topic: %s", topic)
        else:
            logger.info("increased subscriber count for %s to: %s", topic, subs)

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
            logger.warning("unable to remove topic: %s --> not found" % topic)
            return False
        else:
            subs = await self.subscribers(topic)
            if subs:
                logger.info("decreased subscriber count for %s to: %s", topic, subs)
            else:
                logger.info("no more subscribers, topic removed: %s", topic)

        return True if topic not in self._topics else False

    async def add_batch(self, batch: str) -> None:
        """Adds a batch of topics.

        Parameters
        ----------
        batch : str
            the batch of topics
        """
        [await self.add_subscriber(t) for t in self.as_list(batch)]

    async def remove_batch(self, batch: str) -> None:
        """Removes a batch of topics.

        Parameters
        ----------
        batch : str
            the batch of topics
        """
        logger.debug("remove_batch called ...")
        [await self.remove_subscriber(t) for t in self.as_list(batch)]

    async def subscribers(self, topic: str) -> int:
        """Returns the number of subscribers for a topic."""
        return self._topics.count(topic)

    async def remove_topic(self, topic: str) -> None:
        """Removes a topic.

        Parameters
        ----------
        topic : str
            the topic
        """
        try:
            while topic in self._topics:
                self.topics.remove(topic)
        except ValueError:
            logger.warning("unable to remove topic: %s --> not found" % topic)
        else:
            logger.info("removed topic: %s", topic)

    async def add_pending(batch: str) -> None:
        ...

    async def remove_pending(self, response: dict) -> None:
        """Removes pending topics.

        Parameters
        ----------
        response : dict
            the response from the websocket
        """
        ...

    # ..................................................................................
    def batch_topics(self, topics: list[str]) -> list[list[str]]:
        return [
            topics[i: i + MAX_BATCH_SUBSCRIPTIONS]
            for i in range(0, len(topics), MAX_BATCH_SUBSCRIPTIONS)
        ]

    def batch_topics_str(self, topics: list[str]) -> list[str]:
        if not topics:
            return []

        logger.debug("Batch topics got: %s", topics)

        subject = topics[0].split(':')[0]

        try:
            stripped_subject = tuple(
                sorted(
                    [t.split(':')[1] for t in (topics)]
                )
            )
        except IndexError:
            logger.error("unable to create batched topic string. forgot the subject?")
            logger.error("topics: %s", [t for t in topics if ":" not in t])
            return []

        return [
            f"{subject}:{(',').join(stripped_subject[i: i + MAX_BATCH_SUBSCRIPTIONS])}"
            for i in range(0, len(topics), MAX_BATCH_SUBSCRIPTIONS)
        ]

    def as_list(self, topic: str) -> list:
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
        self._conn._topics = self._topics
        await asyncio.sleep(3)
        return self

    @property
    def topics(self) -> list:
        return self._conn.unique_topics

    async def _recv(self, msg):
        if msg.get('type') == 'error':
            logger.error("subscription error: %s" % msg)
        elif 'data' in msg:
            await self._callback(msg)
        elif msg.get('type') == 'ack':
            logger.info("subscription acknowledged: %s" % msg)
        elif msg.get('type') == 'welcome':
            logger.info("websocket connection established: OK")
        elif msg.get('type') == 'pong':
            pass
        else:
            logger.info("unknown message type: %s" % msg)

    async def subscribe(self, topic):
        """Subscribe to a channel
        :param topic: required
        :type topic: str
        :returns: None
        """
        req_msg = {'type': 'subscribe', 'topic': None, 'response': True}
        to_subscribe, exceeds_topic_limit = await self._topics.process_subscribe(topic)

        logger.debug("got batched topics to subscribe: %s" % to_subscribe)

        for batch in to_subscribe:
            logger.info("SUBSCRIBING TO topic: %s", batch)
            req_msg["topic"] = batch
            await self._conn.send_message(req_msg)
            await self._topics.add_batch(batch)

        return exceeds_topic_limit

    async def unsubscribe(self, topic):
        """Unsubscribe from a topic

        :param topic: required
        :type topic: str
        :returns: None
        """
        req_msg = {'type': 'unsubscribe', 'topic': topic, 'response': True}
        for batch in await self._topics.process_unsubscribe(topic):
            logger.info("unsubscribing from topic: %s", batch)
            req_msg[topic] = batch
            await self._conn.send_message(req_msg)
            # await self._topics.remove_batch(batch)
