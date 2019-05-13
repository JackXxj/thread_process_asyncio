# coding:utf-8
__author__ = 'xxj'

import asyncio
import aiohttp
import time

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
}


# 异步的递归重试机制
async def douban(url):
    try:
        print(time.strftime('[%Y-%m-%d %H:%M:%S]'), '豆瓣url：', url)
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url, headers=headers, timeout=0.01) as response:
                status = response.status
                print('响应状态码：', status)
                response_text = await response.text()
                if '豆瓣电影' in response_text:
                    print('无效页面。。。')
                    asyncio.sleep(2)
                    return await douban(url)

    except asyncio.TimeoutError as e:
        print('异步请求时超时异常。。。')
        asyncio.sleep(2)
        return await douban(url)


def main():
    print(time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start')
    loop = asyncio.get_event_loop()
    url = 'https://movie.douban.com/subject/26100958/'
    cor = douban(url)
    future = asyncio.ensure_future(cor)
    loop.run_until_complete(future)
    loop.close()
    print(time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end')


if __name__ == '__main__':
    main()