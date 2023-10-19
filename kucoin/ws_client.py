import asyncio
import logging

from typing import Optional, Iterator
from .websocket.websocket import ConnectWebsocket

MAX_BATCH_SUBSCRIPTIONS = 100  # 100 - max number of topics in a single request
MAX_TOPICS_PER_CLIENT = 300  # max number of topics for a single client/instance

logger = logging.getLogger("main.ws_client")
logger.setLevel(logging.DEBUG)


def batch_topics(topics: list[str]) -> list[list[str]]:
    """Create a list of batched/chunked topics.

    Parameters
    ----------
    topics : list[str]
        A list of topics.

    Returns
    -------
    list[list[str]]
        A list of lists of topics, sub-lists have a maximum length,
        as defined by MAX_BATCH_SUBSCRIPTIONS.
    """
    return [
        topics[i: i + MAX_BATCH_SUBSCRIPTIONS]
        for i in range(0, len(topics), MAX_BATCH_SUBSCRIPTIONS)
    ]


def batch_topics_str(topics: list[str]) -> list[str]:
    """Create a list of batched/chunked topics.

    Parameters
    ----------
    topics : list[str]
        A list of topics.

    Returns
    -------
    list[str]
        A list of concatenated topic strings, for batch requests.
        the maximum number of topics per string is defined by
        MAX_BATCH_SUBSCRIPTIONS.
    """
    if not topics:
        return []

    logger.debug("Batch topics got: %s", topics)

    subject = topics[0].split(":")[0]

    try:
        stripped_subject = tuple(sorted([t.split(":")[1] for t in (topics)]))
    except IndexError:
        logger.error("unable to create batched topic string. forgot the subject?")
        logger.error("topics: %s", [t for t in topics if ":" not in t])
        return []

    return [
        f"{subject}:{(',').join(stripped_subject[i: i + MAX_BATCH_SUBSCRIPTIONS])}"
        for i in range(0, len(topics), MAX_BATCH_SUBSCRIPTIONS)
    ]


def as_list(topic: str) -> list[str]:
    """Turns a concatenated topic string into a list of single topics.

    Parameters
    ----------
    topic : str
        A topic string that may or may not contain multiple topics.

    Returns
    -------
    list[str]
        A list of single topic strings.
    """
    if "," not in topic:
        return [topic]

    subject, topics_str = topic.split(":")[:2]
    return [f"{subject}:{t}" for t in topics_str.split(",")]


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
        """Returns all topics of this instance

        Topics with multiple subscriptions will appear multiple times.
        """
        return self._topics

    @property
    def unique_topics(self) -> list[str]:
        """Returns all unique topics of this instance"""
        return list(set(self._topics))

    @property
    def unique_topics_count(self) -> int:
        """Returns the number of unique topics of this instance"""
        return len(self.unique_topics)

    @property
    def batched_topics(self) -> list[list[str]]:
        """Returns a list of comma-separated strings of topic names.

        The sub-lists are of the length of MAX_BATCH_SUBSCRIPTIONS.
        """
        return batch_topics(self.unique_topics)

    @property
    def batched_topics_str(self) -> list[str]:
        """Returns a list of comma-separated strings of topic names.

        The sub-strings are of the length of MAX_BATCH_SUBSCRIPTIONS,
        and the topics are the unique topics currently registered
        with the class instance.
        """
        return batch_topics_str(self.unique_topics)

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
        # handle topics with existing subcribers
        for topic in tuple(topics := as_list(req)):
            if topic in self._topics:
                await self.add_subscriber(topic)
                # can be removed from request as we already have it
                topics.remove(topic)

        # see how much capacity this client has left
        capacity = MAX_TOPICS_PER_CLIENT - self.unique_topics_count

        return batch_topics_str(topics[:capacity]), topics[capacity:]

    async def process_unsubscribe(self, req: str) -> tuple[list[str], list[str]]:
        """Processes an unsubscribe request.

        Parameters
        ----------
        req : str
            the unsubscribe request

        Returns
        -------
        list[str]
            A list of batched topic strings, for all topics that can be unsubscribed.
        """
        return batch_topics_str(
            [t for t in as_list(req) if await self.remove_subscriber(t)]
        )

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

        if (subs := self._topics.count(topic)) == 1:
            logger.info("created new topic: %s" % topic)
            return True
        else:
            logger.info("increased subscriber count for %s to: %s", topic, subs)
            return False

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
            self._topics.remove(topic)
        except ValueError:
            logger.warning("unable to remove topic: %s --> not found" % topic)
            can_be_unsubbed = False
        else:
            if subs := self._topics.count(topic):
                logger.info("decreased subscriber count for %s to: %s", topic, subs)
                can_be_unsubbed = False
            else:
                logger.info("no more subscribers, topic removed: %s", topic)
                can_be_unsubbed = True
        finally:
            return can_be_unsubbed


class KucoinWsClient:
    """https://docs.kucoin.com/#websocket-feed"""

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
        return self._topics.unique_topics

    @property
    def topics_left(self) -> int:
        return MAX_TOPICS_PER_CLIENT - self._topics.unique_topics_count

    async def _recv(self, msg: dict) -> None:
        if msg.get("type") == "error":
            logger.error("subscription error: %s" % msg)
            await self._callback(msg)
        elif "data" in msg:
            await self._callback(msg)
        elif msg.get("type") == "ack":
            logger.debug("subscription acknowledged: %s" % msg)
        elif msg.get("type") == "welcome":
            logger.info("websocket connection established: OK")
        elif msg.get("type") == "pong":
            pass
        else:
            logger.warning("unknown message type: %s" % msg)

    async def subscribe(self, topic: str) -> None:
        """Subscribe to a channel
        :param topic: required
        :type topic: str
        :returns: None
        """
        req_msg = {"type": "subscribe", "topic": None, "response": True}
        to_subscribe, exceeds_topic_limit = await self._topics.process_subscribe(topic)

        logger.debug("got batched topics to subscribe: %s" % to_subscribe)

        for batch in to_subscribe:
            logger.info("SUBSCRIBING TO topic: %s", batch)
            req_msg["topic"] = batch
            await self._conn.send_message(req_msg)
            [await self._topics.add_subscriber(t) for t in as_list(batch)]

        return exceeds_topic_limit

    async def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a topic

        :param topic: required
        :type topic: str
        :returns: None
        """
        logger.debug(" ... processing unsubscribe request")
        req_msg = {"type": "unsubscribe", "topic": None, "response": True}
        for batch in await self._topics.process_unsubscribe(topic):
            logger.info("unsubscribing from topic: %s", batch)
            req_msg["topic"] = batch
            await self._conn.send_message(req_msg)

    async def close(self):
        asyncio.create_task(self._conn.close())
