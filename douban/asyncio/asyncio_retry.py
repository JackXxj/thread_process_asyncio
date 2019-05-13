# coding:utf-8
__author__ = 'xxj'

# 实现对asyncio异步库和aiohttp的重试机制

import logging
import time
import asyncio
import requests
import json
from queue import Queue
import re

from functools import wraps

log = logging.getLogger(__name__)


class Retry(BaseException):
    def __init__(self, message):
        super(Retry, self).__init__()
        self.message = message


def retry(*exceptions, retries=1, cooldown=1, verbose=True):
    """Decorate an async function to execute it a few times before giving up.
    Hopes that problem is resolved by another side shortly.

    Args:
        exceptions (Tuple[Exception]) : The exceptions expected during function execution
        retries (int): Number of retries of function execution.
        cooldown (int): Seconds to wait before retry.
        verbose (bool): Specifies if we should log about not successful attempts.
    """

    def wrap(func):
        @wraps(func)
        async def inner(*args, **kwargs):
            retries_count = 0

            while True:
                try:    # 实现对异常的捕抓
                    result = await func(*args, **kwargs)
                except exceptions as err:
                    # print('函数：', func.name())
                    # print('异常信息：', err.message)
                    retries_count += 1
                    # 对于重试异常的打印内容：时间、那个函数异常、什么异常、重试的次数
                    message = "{} {}. {} of {} retries attempted".format(time.strftime('[%Y-%m-%d %H:%M:%S]'), err.message, retries_count, retries)

                    if retries_count > retries:
                        # verbose and log.exception(message)    # 当超过最大重试次数的时候，打印异常信息
                        # raise RetryExhaustedError(func.__qualname__, args, kwargs) from err
                        # raise Retry('retry count > 3')     # 对于重试次数超过3次的就丢弃
                        print('{} {}重试次数超过{}次，丢弃'.format(time.strftime('[%Y-%m-%d %H:%M:%S]'), err.message, retries))
                        return None
                    else:
                        verbose and log.warning(message)    # 每重试一次，打印一次warn

                    if cooldown:    # 异常时重试之间的时间间隔
                        await asyncio.sleep(cooldown)
                else:    # 说明整个函数执行无异常
                    return result
        return inner
    return wrap

