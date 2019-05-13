# coding:utf-8
__author__ = 'xxj'

import sys
import time
import math
import os
import json
import re
import asyncio
import aiohttp
from queue import Queue
import lxml.etree
import requests
# from retrying import retry
from asyncio_t.asyncio_retry import retry
import functools

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
}
PROXY_IP_Q = Queue()  # 代理ip队列
semaphore = asyncio.Semaphore(50)


class DoubanException(BaseException):
    def __init__(self, message):
        super(DoubanException, self).__init__()
        self.message = message


def get_proxy_ips(num):
    '''
    获取代理ip
    :return:
    '''
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
        }
        url = 'https://proxyapi.mimvp.com/api/fetchsecret.php?orderid=860068921904605585&num={num}&http_type=3&result_fields=1,2,3&result_format=json'.format(
            num=num)
        response = requests.get(url=url, headers=headers, timeout=10)
        ip_list = json.loads(response.text).get('result')
        if not ip_list:
            print(time.strftime('[%Y-%m-%d %H:%M:%S]'), '获取代理ip异常：', response.text)
            content_json = response.json()
            code_msg = content_json.get('code_msg')  # 异常信息
            code_msg = code_msg.encode('utf-8')
            search_obj = re.search(r'.*?，【(.*?)秒】', code_msg, re.S)
            stop_time = search_obj.group(1)
            stop_time = int(stop_time)
            print('代理ip接口限制,限制时间为：', stop_time, '秒')
            time.sleep(stop_time)
            return get_proxy_ips(num)
        for ip in ip_list:
            ip = ip.get('ip:port')
            # proxies = {
            #     'http': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip),
            #     'https': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip)
            # }
            proxies = "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip)  # aiohttp目前不支持https代理
            PROXY_IP_Q.put(proxies)
    except BaseException as e:
        print('BaseException：', '代理ip异常')
        time.sleep(60)
        return get_proxy_ips(num)


async def async_get_proxy_ips(num):
    '''
    获取代理ip
    :return:
    '''
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
        }
        url = 'https://proxyapi.mimvp.com/api/fetchsecret.php?orderid=860068921904605585&num={num}&http_type=3&result_fields=1,2,3&result_format=json'.format(
            num=num)
        # response = requests.get(url=url, headers=headers, timeout=10)
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url, headers=headers, timeout=10) as response:
                response_text = await response.text()
                ip_list = json.loads(response_text).get('result')
                if not ip_list:
                    print(time.strftime('[%Y-%m-%d %H:%M:%S]'), '获取代理ip异常：', response.text)
                    content_json = response.json()
                    code_msg = content_json.get('code_msg')  # 异常信息
                    code_msg = code_msg.encode('utf-8')
                    search_obj = re.search(r'.*?，【(.*?)秒】', code_msg, re.S)
                    stop_time = search_obj.group(1)
                    stop_time = int(stop_time)
                    print(time.strftime('[%Y-%m-%d %H:%M:%S]'), '代理ip接口限制,限制时间为：', stop_time, '秒')
                    await asyncio.sleep(stop_time)
                    # time.sleep(stop_time)
                    return await async_get_proxy_ips(num)
                for ip in ip_list:
                    ip = ip.get('ip:port')
                    proxies = "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip)    # aiohttp目前不支持https代理
                    PROXY_IP_Q.put(proxies)
    except BaseException as e:
        print(time.strftime('[%Y-%m-%d %H:%M:%S]'), 'BaseException：', '代理ip异常')
        await asyncio.sleep(60)
        # time.sleep(60)
        return await async_get_proxy_ips(num)


@retry(DoubanException, retries=10)
async def movie_req(session, url, lock):
    '''
    豆瓣索引页
    :param session: session
    :param url: url
    :return:
    '''
    global proxies
    try:
        print('{t} 豆瓣url：{url} 代理ip：{proxies}'.format(t=time.strftime('[%Y-%m-%d %H:%M:%S]'),
                                                      url=url, proxies=proxies))
        async with session.get(url=url, headers=headers, proxy=proxies, timeout=10) as response:  # aiohttp异步请求库暂时支持的代理设置只能使用http代理，暂无https代理使用方法。
            response_status = response.status
            print('状态码：', response_status, type(response_status))
            response_text = await response.text()     # 有两种请求情况需要视为异常：1、就是python中抛出的相关异常；2、出现异常时并没有抛出异常，而是一些错误信息，所以需要将其也视为异常情况处理
            # return response_text
            if response_status != 200:
                # 注意点：（协程锁的概念）出现异常的地方如果不加锁，当代理ip空时，此时多个协程代理ip都失效之后（接着查询代理ip队列中的代理ip为空），那么就会出现短时间多个协程对代理IP接口进行请求（次数比较多的时候会出现接口失效的情况）（所以对于这块还是需要添加锁）
                # 希望当代理ip队列为空时，多个协程的代理ip失效，但是只允许一个协程进行代理ip接口的请求。
                # 通过异步io库中的锁解决：具体如下
                with await lock:
                    if PROXY_IP_Q.empty():
                        await async_get_proxy_ips(100)  # 获取代理ip接口
                        num = PROXY_IP_Q.qsize()
                        print('豆瓣搜索页重新获取代理ip的数量：{}'.format(num))
                    proxies = PROXY_IP_Q.get(False)  # 在重试之前获取代理ip时，下一次重试就会使用新代理进行请求
                    print('豆瓣搜索页获取到新的代理ip：{proxies}'.format(proxies=proxies))
                    raise DoubanException('movie()出现状态码非200')  # 进行重试

            if ('<script>var d=[navigator.platform,navigator.userAgent,navigator.vendor].join("|");' in response_text) or \
                    ('检测到有异常请求从你的 IP 发出' in response_text):  # ip被限制
                # 实现代理ip的切换，并抛出异常
                with await lock:
                    if PROXY_IP_Q.empty():
                        await async_get_proxy_ips(100)  # 获取代理ip接口
                        num = PROXY_IP_Q.qsize()
                        print('豆瓣搜索页重新获取代理ip的数量：', num)
                    proxies = PROXY_IP_Q.get(False)  # 在重试之前获取代理ip时，下一次重试就会使用新代理进行请求
                    print('豆瓣搜索页获取到新的代理ip：{proxies}'.format(proxies=proxies))
                    raise DoubanException('movie()出现douban exception')  # 进行重试

    # 两大类的异常：请求异常、ip限制异常
    except asyncio.TimeoutError as e:  # 由于希望在捕获异常时，可以打印出相关异常的信息。(但是asyncio.TimeoutError的异常捕获时，无法捕获异常信息。所以将该异常封装一下，以便获取自定义的异常信息)
        with await lock:
            print('movie()出现asyncio.TimeoutError异常。。。')
            if PROXY_IP_Q.empty():
                await async_get_proxy_ips(100)  # 获取代理ip接口
                num = PROXY_IP_Q.qsize()
                print('豆瓣搜索页重新获取代理ip的数量：', num)
            proxies = PROXY_IP_Q.get(False)
            print('获取新的代理ip：', proxies)
            raise DoubanException('movie()出现asyncio.TimeoutError异常')

    except aiohttp.client_exceptions.ClientHttpProxyError as e:
        with await lock:
            print('movie()出现aiohttp.client_exceptions.ClientHttpProxyError异常。。。')
            if PROXY_IP_Q.empty():
                await async_get_proxy_ips(100)  # 获取代理ip接口
                num = PROXY_IP_Q.qsize()
                print('豆瓣搜索页重新获取代理ip的数量：', num)
            proxies = PROXY_IP_Q.get(False)
            print('获取新的代理ip：', proxies)
            raise DoubanException('movie()出现aiohttp.client_exceptions.ClientHttpProxyError异常')

    except BaseException as e:
        with await lock:
            print('BaseException异常类型：{type}'.format(type=e))
            if PROXY_IP_Q.empty():
                await async_get_proxy_ips(100)  # 获取代理ip接口
                num = PROXY_IP_Q.qsize()
                print('豆瓣搜索页重新获取代理ip的数量：', num)
            proxies = PROXY_IP_Q.get(False)
            print('获取新的代理ip：', proxies)

    else:
        return response_text


async def movie(line, fileout, lock):
    '''
    豆瓣搜索页
    :param line: 源文件的行数据
    :param fileout: 数据存储文件
    :return:
    '''
    async with semaphore:    # 通过asyncio.Semaphore()控制并发量(Semaphore信号量机制控制并发数量)
        movie_name = line.split('\t')[0]  # 电影名
        # print('电影名：', movie_name)
        movie_url = 'https://www.douban.com/search?cat=1002&q={q}'.format(q=movie_name)    # 豆瓣搜索页url
        # response = await aiohttp.request('GET', movie_url)    # 该方法不存在超时设置
        async with aiohttp.ClientSession() as session:
            response_text = await movie_req(session, movie_url, lock)    # 豆瓣电影索引页
            if response_text is not None:
                # print(response_text)
                await http_parse(response_text, line, fileout, lock)    # 页面解析


async def http_parse(response_text, line, fileout, lock):
    '''
    页面解析
    :param response_text: 响应内容
    :param line: 源文件的行数据
    :param fileout: 数据存储文件
    :return:
    '''
    xpath_obj = lxml.etree.HTML(response_text)
    results = xpath_obj.xpath('//div[@class="result"]')
    if results:
        result = results[0]  # 获取搜索结果的第一条
        pic_url = result.xpath('.//a[@class="nbg"]/img/@src')[0]  # 图片url
        # print('图片url：', pic_url)
        tag = result.xpath('.//div[@class="title"]/h3/span/text()')[0].replace('[', '').replace(']', '')  # 标签
        # print('标签：', tag)
        name = result.xpath('.//div[@class="title"]/h3/a/text()')[0]  # 电影名
        print('电影名：', name)
        url = result.xpath('.//div[@class="title"]/h3/a/@href')[0]  # 电影url
        url = await douban_url(url, lock)  # 获取跳转后的链接接口
        # print '电影url：', url
        des = result.xpath('.//span[@class="subject-cast"]/text()')[0]
        # print des, repr(des)
        movie_time = des.split(' / ')[-1]  # 时间
        # print '年份：', movie_time
        try:
            content = '\t'.join([line, tag, name, url, pic_url, movie_time])
            fileout.write(content)
            fileout.write('\n')
            fileout.flush()
        except TypeError as e:
            print('TypeError异常 line：{line} tag：{tag} name：{name} url：{url} pic_url：{pic_url} movie_time：{movie_time}'
                  .format(line=line, tag=tag, name=name, url=url, pic_url=pic_url, movie_time=movie_time))

    else:  # 该影视无搜索结果
        print('该影视无搜索结果：{}'.format(line))
        # print response.text


async def douban_url(url, lock):
    '''
    获取跳转后的链接
    :param url: 跳转前的链接
    :return:
    '''
    # response = aiohttp.request('GET', url=url)
    async with aiohttp.ClientSession() as session:
        response_url = await douban_url_req(session, url, lock)    # url跳转接口
        if response_url is None:
            response_url = ''
        return response_url


@retry(DoubanException, retries=10)
async def douban_url_req(session, url, lock):
    global proxies
    try:
        print('{t}豆瓣跳转前的链接url：{url} proxies：{proxies}'.format(t=time.strftime('[%Y-%m-%d %H:%M:%S]'),
                                                              url=url, proxies=proxies))
        async with session.get(url=url, headers=headers, proxy=proxies, timeout=10) as response:  # 在请求内部参数中加入请求超时时间
            response_status = response.status
            response_url = str(response.url)  # 通过response.url获取的是url对象，通过str()将其转换为字符串类型
            # print('响应状态码：', status, '响应url:', response_url)
            if response_status != 200:
                with await lock:
                    if PROXY_IP_Q.empty():
                        await async_get_proxy_ips(100)  # 获取代理ip接口
                        num = PROXY_IP_Q.qsize()
                        print('豆瓣搜索页重新获取代理ip的数量：', num)
                    proxies = PROXY_IP_Q.get(False)  # 在重试之前获取代理ip时，下一次重试就会使用新代理进行请求
                    print('豆瓣搜索页获取到新的代理ip：{proxies}'.format(proxies=proxies))
                    raise DoubanException('douban_url()出现状态码非200')  # 进行重试

            if 'sec.douban.com' in response_url:  # ip被限制，抛异常
                with await lock:
                    if PROXY_IP_Q.empty():
                        await async_get_proxy_ips(100)  # 获取代理ip接口
                        num = PROXY_IP_Q.qsize()
                        print('豆瓣url跳转重新获取代理ip的数量：', num)
                    proxies = PROXY_IP_Q.get(False)
                    print('获取到新的代理ip：{proxies}'.format(proxies=proxies))
                    raise DoubanException('douban_url()出现douban exception')

    except asyncio.TimeoutError as e:    # 由于希望在捕获异常时，可以打印出相关异常的信息。(但是asyncio.TimeoutError的异常捕获时，无法捕获异常信息。所以将该异常封装一下，以便获取自定义的异常信息)
        with await lock:
            print('douban_url()出现asyncio.TimeoutError异常。。。')
            if PROXY_IP_Q.empty():
                await async_get_proxy_ips(100)   # 获取代理ip接口
                num = PROXY_IP_Q.qsize()    #
                print('豆瓣url跳转重新获取代理ip的数量：', num)
            proxies = PROXY_IP_Q.get(False)
            raise DoubanException('douban_url()出现asyncio.TimeoutError异常')

    except aiohttp.client_exceptions.ClientHttpProxyError as e:
        with await lock:
            print('douban_url()出现aiohttp.client_exceptions.ClientHttpProxyError异常。。。')
            if PROXY_IP_Q.empty():
                await async_get_proxy_ips(100)  # 获取代理ip接口
                num = PROXY_IP_Q.qsize()
                print('豆瓣url跳转重新获取代理ip的数量：', num)
            proxies = PROXY_IP_Q.get(False)
            print('获取新的代理ip：', proxies)
            raise DoubanException('douban_url()出现aiohttp.client_exceptions.ClientHttpProxyError异常')

    else:
        return response_url


def unlock(lock):
    print('callback releasing lock')
    lock.release()


async def main(loop):
    lock = asyncio.Lock()    # 锁
    # await lock.acquire()
    print(time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start')
    date = time.strftime('%Y%m%d')
    keyword_file_dir = r'/ftp_samba/112/file_4spider/dmn_fanyule_movie/'
    keyword_file_name = r'dmn_fanyule_movie_{date}_1.txt'.format(date=date)
    keyword_file_path = os.path.join(keyword_file_dir, keyword_file_name)    # 影视的来源文件
    keyword_file_path = r'C:\Users\xj.xu\Desktop\dmn_fanyule_movie_20190413_1.txt'
    if not os.path.exists(keyword_file_path):
        keyword_file_name = os.listdir(keyword_file_dir)[-1]
        keyword_file_path = os.path.join(keyword_file_dir, keyword_file_name)
        print('目标文件不存在，获取该路径下的最新文件：', keyword_file_path)
    file = open(keyword_file_path, 'r', encoding='utf-8')

    dest_path = '/ftp_samba/112/spider/fanyule_two/douban'  # 数据存储文件路径
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'douban_movie_' + date)
    tmp_file_name = os.path.join(dest_path, 'douban_movie_' + date + '.tmp')
    fileout = open(tmp_file_name, 'a')

    # loop = asyncio.get_event_loop()
    # loop.call_later(0.1, functools.partial(unlock, lock))    # call_later() 表达推迟一段时间的回调, 第一个参数是以秒为单位的延迟, 第二个参数是回调函数

    tasks = []
    for line in file:
        line = line.strip()
        if line:
            cor = movie(line, fileout, lock)    # 根据源文件的关键词数量创建了相应数量的协程对象（每个协程对象初始时都携带上了关键词和文件对象）
            future = asyncio.ensure_future(cor)
            tasks.append(future)    # 任务列表
    # loop.run_until_complete(asyncio.wait(tasks))
    await asyncio.wait(tasks),
    loop.close()

    try:
        fileout.flush()
        fileout.close()
    except IOError as e:
        time.sleep(1)
        fileout.close()
    print(time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end')
    os.rename(tmp_file_name, dest_file_name)


if __name__ == '__main__':
    get_proxy_ips(100)  # 获取代理ip接口
    ip_num = PROXY_IP_Q.qsize()
    print(time.strftime('[%Y-%m-%d %H:%M:%S]'), '获取代理ip的数量：', ip_num)
    proxies = PROXY_IP_Q.get(
        False)  # 初衷：就是提供一个第一次异常前的有效的代理ip（就是说当代理ip失效之后，会获取新的代理ip，那么之后的请求就是用获取到的有效的代理ip）（所以也就没有将第一次获取到的代理ip放入到协程对象的参数列表中。因为这样当代理IP失效之后，依然会使用）
    print('初始时获取代理ip：', proxies)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(loop))
    except BaseException as e:
        loop.close()







# 一、asyncio + aiohttp实现并发爬虫

# 二、对于并发爬虫的重试机制（自定义的异步重试模块）
    # 重试模块的核心问题：采用异步重试模块（asyncio_retry.py）, python中的retrying模块无效(因为该模块时同步模块不适用于asyncio库)。
    # 重试模块主要注重的是异常模块重试的功能：如：1、检测是否有需要重试的异常；2、重试次数；3、重试之间的间隔时间。
    # 注意：而对于ip被限制是的ip切换应该是在主模块中实现，而不是嵌入到重试模块中实现该功能。

# 三、异步模块中锁的使用




# 在协程中不要运行同步代码，只要有同步代码，协程并发效果立马作废。









# 并发爬虫缺点：（单线程实现并发爬虫）
'''
1、因为这是单线程的并发爬虫，所以在代理ip有效的时间内，所有的协程用的都是同一个代理ip。（如果某网站的ip限制非常严格时。就会导致大量的代理ip失效。因为某一个代理ip在短时间内请求的次数过于频繁）

'''