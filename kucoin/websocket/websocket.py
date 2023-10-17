import asyncio
import json
import logging
import time
import websockets

from random import random
from uuid import uuid4

from ..rate_limiter import async_rate_limiter as rate_limiter

logger = logging.getLogger("main.websocket")
logger.setLevel(logging.DEBUG)

MSG_LIMIT = 100  # 100 per 10 seconds
MSG_LIMIT_LOOKBACK = 10  # seconds


class ConnectWebsocket:
    MAX_RECONNECTS = 5
    MAX_RECONNECT_SECONDS = 60

    def __init__(self, loop, client, callback, private=False):
        self._loop = loop
        self._client = client
        self._callback = callback
        self._reconnect_num = 0
        self._ws_details = None
        self._connect_id = None
        self._private = private
        self._last_ping = None
        self._socket = None
        self._topics = []
        self._just_started = True
        self._shutdown_flag = False
        asyncio.ensure_future(self.run_forever(), loop=self._loop)

    @property
    def topics(self):
        return self._topics

    async def _run(self, event: asyncio.Event):
        keep_alive, should_reconnect = True, True

        while keep_alive:
            try:
                self._last_ping = time.time()  # record last ping
                self._ws_details = self._client.get_ws_token(self._private)
                logger.debug(self._ws_details)

                async with websockets.connect(
                    self.get_ws_endpoint(), ssl=self.get_ws_encryption()
                ) as socket:
                    self._socket = socket
                    self._reconnect_num = 0

                    if not event.is_set():
                        await self.send_ping()
                        event.set()

                    while keep_alive:
                        if time.time() - self._last_ping > self.get_ws_pingtimeout():
                            await self.send_ping()

                        try:
                            _msg = await asyncio.wait_for(
                                self._socket.recv(), timeout=self.get_ws_pingtimeout()
                            )
                        except asyncio.TimeoutError:
                            await self.send_ping()
                        except websockets.ConnectionClosed as e:
                            logger.warning(f"websocket connection closed (inner): {e}")
                            break  # Break the inner loop to trigger reconnection
                        except asyncio.CancelledError:
                            logger.info("cancelled ... initiating shutdown")
                            self._shutdown_flag = True
                            keep_alive, should_reconnect = False, False
                            await self._socket.ping()
                        else:
                            try:
                                msg = json.loads(_msg)
                            except ValueError:
                                logger.warning(_msg)
                            else:
                                await self._callback(msg)

            except websockets.ConnectionClosed as e:
                if not self._shutdown_flag:
                    logger.warning(f"websocket connection closed (outer): {e}")
            except Exception as e:
                logger.error(f"an unexpected error occurred: {e}", exc_info=1)

            # Check if reconnection is needed
            if keep_alive and should_reconnect:
                logger.info("websocket closed --> reconnect: %s", should_reconnect)
                await self._reconnect()
            else:
                logger.info("websocket closed --> reconnect: %s", should_reconnect)

    def get_ws_endpoint(self):
        if not self._ws_details:
            raise Exception("Websocket details Error")
        ws_connect_id = str(uuid4()).replace("-", "")
        token = self._ws_details["token"]
        endpoint = self._ws_details["instanceServers"][0]["endpoint"]
        ws_endpoint = f"{endpoint}?token={token}&connectId={ws_connect_id}"
        return ws_endpoint

    def get_ws_encryption(self):
        if not self._ws_details:
            raise Exception("Websocket details Error")
        return self._ws_details["instanceServers"][0]["encrypt"]

    def get_ws_pingtimeout(self):
        if not self._ws_details:
            raise Exception("Websocket details Error")
        _timeout = int(self._ws_details["instanceServers"][0]["pingTimeout"] / 1000) - 2
        return _timeout

    async def run_forever(self):
        while True:
            await self._reconnect()

    async def _reconnect(self):
        # exception handler for tasks
        def exception_handler(task):
            name = task.get_name()

            try:
                if exception := task.exception():
                    logger.warning("task %s EXCEPTION: %s", name, exception)
                else:
                    logger.info("task %s DONE: %s", name, task.done())
            except asyncio.CancelledError:
                logger.debug("task %s cancelled: %s", name, task.cancelled())
                logger.debug("task %s DONE: %s", name, task.done())
            except Exception as e:
                logger.error("exception in handler while processing %s: %s", name, e)

        logger.info("Websocket start connect/reconnect")

        self._reconnect_num += 1

        if not self._just_started:
            reconnect_wait = self._get_reconnect_wait(self._reconnect_num)
            logger.info(
                "asyncio sleep reconnect_wait=%s s reconnect_num=%s",
                reconnect_wait,
                self._reconnect_num,
            )
            await asyncio.sleep(reconnect_wait)
            logger.info("asyncio sleep: ok")

        event = asyncio.Event()

        # Create tasks using asyncio.create_task and set an exception handler
        tasks = {
            asyncio.create_task(
                self._recover_topic_req_msg(event), name="recover_topic"
            ): self._recover_topic_req_msg,
            asyncio.create_task(self._run(event), name="run"): self._run,
        }

        for task in tasks.keys():
            task.add_done_callback(exception_handler)

        self._just_started = False

        while set(tasks.keys()):
            finished, pending = await asyncio.wait(
                tasks.keys(), return_when=asyncio.FIRST_EXCEPTION
            )

            if self._shutdown_flag:  # New shutdown flag
                logger.info("Shutdown flag active, cancelling all pending tasks ...")
                for pt in pending:
                    pt.cancel()

                await asyncio.gather(list(tasks.keys()))
                break

            exception_occur = False

            for task in finished:
                if task.exception():
                    exception_occur = True
                    logger.warning(
                        f"{task.get_name()} got an exception {task.exception()}"
                    )
                    for pt in pending:
                        logger.warning(f"pending {pt.get_name()}")
                        try:
                            pt.cancel()
                        except asyncio.CancelledError:
                            logger.exception("CancelledError ")
                        logger.warning("cancel ok.")

            if exception_occur:
                break

        logger.warning("_reconnect over.")

    async def _recover_topic_req_msg(self, event):
        if self._just_started:
            return

        await event.wait()

        for batch in self.topics.batched_topics_str:
            logger.info(f"recover topic batch {batch} waiting")
            await self.send_message(
                {"type": "subscribe", "topic": batch, "response": True}
            )
            logger.info(f"{batch} OK")

    def _get_reconnect_wait(self, attempts):
        expo = 2**attempts
        return round(random() * min(self.MAX_RECONNECT_SECONDS, expo - 1) + 1)

    async def send_ping(self):
        msg = {"id": str(int(time.time() * 1000)), "type": "ping"}
        await self._socket.send(json.dumps(msg))
        self._last_ping = time.time()

    @rate_limiter(
        max_rate=MSG_LIMIT, time_window=MSG_LIMIT_LOOKBACK, send_immediately=False
    )
    async def send_message(self, msg, retry_count=0):
        if not self._socket:
            if retry_count < self.MAX_RECONNECTS:
                await asyncio.sleep(1)
                await self.send_message(msg, retry_count + 1)
        else:
            msg["id"] = str(int(time.time() * 1000))
            msg["privateChannel"] = self._private
            logger.debug("sending message: %s", msg)
            await self._socket.send(json.dumps(msg))


# --------------------------------------------------------------------------------------
#                                    simple test
async def test_callback(msg):
    logger.info(msg)


async def main():
    loop = asyncio.get_event_loop()
    client = WsToken()
    ws = ConnectWebsocket(loop=loop, client=client, callback=test_callback)

    topic = "/market/ticker:XDC-BTC"
    sub_msg = {"type": "subscribe", "topic": topic, "response": True}

    await ws.send_message(sub_msg)
    ws._topics.append(topic)

    while True:
        try:
            await asyncio.sleep(2)
        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.info("cancelled ...")
            break

    logger.info("master coroutine shutdown complete: OK")


if __name__ == "__main__":
    from kucoin.client import WsToken

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    asyncio.run(main())
