#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Provides a rate limiter for functions.

Created on Sun Nov 28 13:12:20 2022

@author dhaneor
"""
import asyncio
import logging

from collections import deque
from functools import wraps
from time import time, sleep
from threading import Thread

logger = logging.getLogger('main.rate_limiter')
logger.setLevel(logging.WARNING)


def rate_limiter(max_rate, time_window=1):
    """
    A decorator function to implement rate limiting with parked requests.

    Args:
        max_rate (int): Maximum number of requests allowed per time_window.
        time_window (int): The length of the time window for rate limiting in seconds.
    """
    def decorator(func):
        timestamps = deque()
        parked_requests = deque()

        def process_queue():
            """Separate thread to process the parked requests queue."""
            nonlocal timestamps, parked_requests
            while True:
                current_time = time()

                # Remove expired timestamps
                while timestamps and timestamps[0] < current_time - time_window:
                    timestamps.popleft()

                # Execute a parked request if rate allows
                if parked_requests and len(timestamps) < max_rate:
                    parked_requests.popleft()()
                    timestamps.append(current_time)

                sleep(time_window / max_rate)

        # Start the queue processor in a separate thread
        Thread(target=process_queue, daemon=True).start()

        @wraps(func)
        def wrapper(*args, **kwargs):
            """Wrapper function to check rate limit."""
            nonlocal timestamps, parked_requests

            current_time = time()

            # Remove expired timestamps
            while timestamps and timestamps[0] < current_time - time_window:
                timestamps.popleft()

            # Execute immediately if rate allows, else park the request
            if len(timestamps) < max_rate:
                result = func(*args, **kwargs)
                timestamps.append(current_time)
                return result
            else:
                logger.debug("waiting ...")
                parked_requests.append(lambda: func(*args, **kwargs))

        return wrapper
    return decorator


# ======================================================================================
def async_rate_limiter(max_rate, time_window=1, send_immediately=True):
    """
    An asynchronous decorator function to implement rate limiting with parked requests.

    Args:
        max_rate (int): Maximum number of requests allowed per time_window.
        time_window (int): The length of the time window for rate limiting in seconds.
        send_immediately (bool): If True, send the first request(s) immediately,
        otherwise send them evenly spaced according to the rate limit.
    """
    timestamps = deque()
    parked_requests = deque(maxlen=10_000)
    process_task = None  # To store the asyncio Task object

    async def process_queue():
        """Asynchronous coroutine to process the parked requests queue."""
        nonlocal timestamps, parked_requests
        while True:
            current_time = time()

            # Remove expired timestamps
            while timestamps and timestamps[0] < current_time - time_window:
                timestamps.popleft()

            # Execute a parked request if rate allows
            if parked_requests and len(timestamps) < max_rate:
                parked_request = parked_requests.popleft()
                # asyncio.create_task(parked_request())
                logger.debug("executing parked request")
                try:
                    await parked_request()
                except Exception as e:
                    logger.error(
                        "parked request caused an exception: %s", e, exc_info=1
                    )
                    # logger.error(parked_request)
                timestamps.append(current_time)

            await asyncio.sleep(time_window / max_rate)

    def decorator(func):
        nonlocal process_task

        @wraps(func)
        async def wrapper(*args, **kwargs):
            """Wrapper function to check rate limit."""
            nonlocal timestamps, parked_requests, process_task

            # Initialize the process_queue coroutine if it's not already running
            if process_task is None:
                process_task = asyncio.create_task(process_queue())

            current_time = time()

            # Remove expired timestamps
            while timestamps and timestamps[0] < current_time - time_window:
                timestamps.popleft()

            # Execute immediately if rate allows & flag is set,
            # else park the request
            if len(timestamps) < max_rate and send_immediately:
                result = await func(*args, **kwargs)
                timestamps.append(current_time)
                return result
            else:
                parked_requests.append(lambda: func(*args, **kwargs))

        return wrapper

    return decorator


# --------------------------------------------------------------------------------------
@async_rate_limiter(max_rate=10, time_window=1, send_immediately=True)
async def async_send(message):
    """Simulates sending a message asynchronously."""
    logger.info(f"Sent: {message}")
    await asyncio.sleep(0.0001)


# Test the async_rate_limiter decorator in an asyncio event loop
async def test_async_rate_limiter():
    for i in range(100):
        await async_send(f"Async Message {i}")
        await asyncio.sleep(0.01)

    await asyncio.sleep(10)


if __name__ == '__main__':

    logger = logging.getLogger('main')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
    )
    handler.setFormatter(formatter)

    asyncio.run(test_async_rate_limiter())

    # # Test the rate_limiter decorator
    # @rate_limiter(1)
    # def send(message):
    #     """Simulates sending a message."""
    #     logger.debug(f"Sent: {message}")

    # # These should execute immediately
    # send("Message 1")
    # send("Message 2")

    # # These should be parked and executed later
    # send("Message 3")
    # send("Message 4")

    # # Wait to see parked messages being sent
    # sleep(2)

    # # These should execute immediately
    # send("Message 5")
    # send("Message 6")

    # sleep(5)
