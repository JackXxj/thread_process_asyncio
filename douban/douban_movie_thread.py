# coding:utf-8
__author__ = 'xxj'

import sys
import time
import math
import os
from rediscluster import StrictRedisCluster
import json
import re
import Queue
import lxml.etree
import threading
import thread
from threading import Lock, Thread
from Queue import Empty
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout
from bs4 import BeautifulSoup
import requests

reload(sys)
sys.setdefaultencoding('utf-8')
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
}
MOVIE_QUEUE = Queue.Queue()  # 电影名队列
PROXY_IP_Q = Queue.Queue()  # 代理ip队列
THREAD_PROXY_MAP = {}    # 线程与代理字典


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
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '获取代理ip异常：', response.text
            content_json = response.json()
            code_msg = content_json.get('code_msg')  # 异常信息
            code_msg = code_msg.encode('utf-8')
            search_obj = re.search(r'.*?，【(.*?)秒】', code_msg, re.S)
            stop_time = search_obj.group(1)
            stop_time = int(stop_time)
            print '代理ip接口限制,限制时间为：', stop_time, '秒'
            # time.sleep(stop_time)
            # return get_proxy_ips(num)
        for ip in ip_list:
            ip = ip.get('ip:port')
            proxies = {
                'http': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip),
                'https': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip)
            }
            PROXY_IP_Q.put(proxies)
    except BaseException as e:
        print 'BaseException：', '代理ip异常'
        # time.sleep(60)
        # return get_proxy_ips(num)


def get_redis_proxy():
    '''
    从redis相应的key中获取代理ip
    :return:
    '''
    startup_nodes = [{'host': 'redis1', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    weibo_user_info_proxy_length = r.llen('spider:weibo_user_info:proxy')  # weibo_user_info
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中weibo_user_info的代理ip长度：', weibo_user_info_proxy_length
    if weibo_user_info_proxy_length == 0:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中的代理ip数量为0，等待60s'
        time.sleep(60)
        return get_redis_proxy()
    for i in xrange(weibo_user_info_proxy_length):
        ip = r.lpop('spider:weibo_user_info:proxy')
        proxies = {
            'http': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip),
            'https': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip)
        }
        PROXY_IP_Q.put(proxies)


class DoubanException(BaseException):
    def __init__(self, message):
        super(DoubanException, self).__init__()
        self.message = message


def movie(fileout, lock):
    while not MOVIE_QUEUE.empty():
        try:
            thread_name = threading.currentThread().name
            if not THREAD_PROXY_MAP.get(thread_name):    # 实现每个线程对应与一个代理ip
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies
            proxies = THREAD_PROXY_MAP.get(thread_name)

            line = MOVIE_QUEUE.get(False)
            movie_name = line.split('\t')[0]  # 电影名
            movie_url = 'https://www.douban.com/search?cat=1002&q={q}'.format(q=movie_name)
            print time.strftime('[%Y-%m-%d %H:%M:%S] 豆瓣url：{url} thread_name：{thread_name} proxies：{proxies}'
                                .format(url=movie_url.replace('%', ''), thread_name=thread_name, proxies=proxies))    # format()解决打印内容乱序（replace('%', '')处理time.strftime()方法中出现无效的%字符）
            response = requests.get(url=movie_url, headers=headers, proxies=proxies, timeout=10)
            response_text = response.text
            xpath_obj = lxml.etree.HTML(response_text)

            if ('<script>var d=[navigator.platform,navigator.userAgent,navigator.vendor].join("|");' in response_text) or \
                    ('检测到有异常请求从你的 IP 发出' in response_text):
                raise DoubanException('douban exception')
            else:
                # no_result = xpath_obj.xpath('//p[@class="no-result"]')  # 判断是否在豆瓣中无法搜索到该电影
                # if no_result:  # 不存在该电影
                #     print '豆瓣中不存在该电影：{}'.format(movie_name)
                #     continue
                # else:  # 存在该电影或ip被限制
                results = xpath_obj.xpath('//div[@class="result"]')
                if results:
                    result = results[0]  # 获取搜索结果的第一条
                    pic_url = result.xpath('.//a[@class="nbg"]/img/@src')[0]  # 图片url
                    # print '图片url：', pic_url
                    tag = result.xpath('.//div[@class="title"]/h3/span/text()')[0].replace('[', '').replace(']',
                                                                                                            '')  # 标签
                    # print '标签：', tag
                    name = result.xpath('.//div[@class="title"]/h3/a/text()')[0]  # 电影名
                    # print '电影名：', name
                    url = result.xpath('.//div[@class="title"]/h3/a/@href')[0]  # 电影url
                    url = douban_url(url, proxies)    # 获取跳转后的链接接口
                    # print '电影url：', url
                    des = result.xpath('.//span[@class="subject-cast"]/text()')[0]
                    # print des, repr(des)
                    movie_time = des.split(' / ')[-1]  # 时间
                    # print '年份：', movie_time
                    content = '\t'.join([line, tag, name, url, pic_url, movie_time])

                    data_write_file(fileout, lock, content)    # 数据写入文件接口（处理多处使用锁的方法，防止锁异常）

                else:  # 该影视无搜索结果
                    print '该影视无搜索结果：{}'.format(movie_name)
                    # print response.text

        except ConnectTimeout as e:
            with lock:
                print time.strftime('[%Y-%m-%d %H:%M:%S] ConnectTimeout异常：{line} {thread_name}'
                                    .format(line=line.replace('%', ''), thread_name=thread_name))
                MOVIE_QUEUE.put(line)
                THREAD_PROXY_MAP.pop(thread_name)    # 从THREAD_PROXY_MAP中删除失效的代理ip
                if PROXY_IP_Q.empty():
                    print '代理ip队列中的代理ip为空，重新获取代理ip'
                    get_proxy_ips(100)
                proxies = PROXY_IP_Q.get(False)
                print 'ip被限制, 切换新的代理ip：{}'.format(proxies)
                THREAD_PROXY_MAP[thread_name] = proxies

        except ConnectionError as e:
            with lock:
                print time.strftime('[%Y-%m-%d %H:%M:%S] ConnectionError异常：{line} {thread_name}'
                                    .format(line=line.replace('%', ''), thread_name=thread_name))
                MOVIE_QUEUE.put(line)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    print '代理ip队列中的代理ip为空，重新获取代理ip'
                    get_proxy_ips(100)
                proxies = PROXY_IP_Q.get(False)
                print 'ip被限制, 切换新的代理ip：{}'.format(proxies)
                THREAD_PROXY_MAP[thread_name] = proxies

        except DoubanException as e:
            with lock:
                print time.strftime('[%Y-%m-%d %H:%M:%S] DoubanException异常：{line} {thread_name}'
                                    .format(line=line.replace('%', ''), thread_name=thread_name))
                MOVIE_QUEUE.put(line)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    print '代理ip队列中的代理ip为空，重新获取代理ip'
                    get_proxy_ips(100)
                proxies = PROXY_IP_Q.get(False)
                print 'ip被限制, 切换新的代理ip：{}'.format(proxies)
                THREAD_PROXY_MAP[thread_name] = proxies

        except ReadTimeout as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S] ReadTimeout异常：{line} {thread_name}'
                                .format(line=line.replace('%', ''), thread_name=thread_name))
            MOVIE_QUEUE.put(line)

        except Empty as e:
            # print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'Empty异常：', line
            pass

        except BaseException as e:
            with lock:
                print time.strftime('[%Y-%m-%d %H:%M:%S] BaseException异常：{line} {thread_name}'
                                    .format(line=line.replace('%', ''), thread_name=thread_name))
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    print '代理ip队列中的代理ip为空，重新获取代理ip'
                    get_proxy_ips(100)
                proxies = PROXY_IP_Q.get(False)
                print 'ip被限制, 切换新的代理ip：{}'.format(proxies)
                THREAD_PROXY_MAP[thread_name] = proxies


def douban_url(url, proxies):
    # print url
    response = requests.get(url=url, headers=headers, proxies=proxies, timeout=10)
    # print response.status_code
    response_url = response.url
    if 'sec.douban.com' in response_url:    # ip被限制，抛异常
        raise DoubanException('douban exception')
    else:
        # print response_url
        return response_url


def data_write_file(fileout, lock, content):
    with lock:
        fileout.write(content)
        fileout.write('\n')
        fileout.flush()


def main():
    lock = Lock()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    date = time.strftime('%Y%m%d')
    keyword_file_dir = r'/ftp_samba/112/file_4spider/dmn_fanyule_movie/'  # 影视的来源目录
    keyword_file_name = r'dmn_fanyule_movie_{date}_1.txt'.format(date=date)  # 影视的来源文件名
    keyword_file_path = os.path.join(keyword_file_dir, keyword_file_name)
    keyword_file_path = r'C:\Users\xj.xu\Desktop\dmn_fanyule_movie_20190413_1.txt'
    if not os.path.exists(keyword_file_path):
        keyword_file_name = os.listdir(keyword_file_dir)[-1]
        keyword_file_path = os.path.join(keyword_file_dir, keyword_file_name)
        print '目标文件不存在，获取该路径下的最新文件：', keyword_file_path
    file = open(keyword_file_path, 'r')
    for line in file:
        line = line.strip()
        if line:
            MOVIE_QUEUE.put(line)
    print '源数据队列中的数量：', MOVIE_QUEUE.qsize()

    get_proxy_ips(100)  # 获取代理ip接口
    ip_num = PROXY_IP_Q.qsize()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '获取代理ip的数量：', ip_num

    dest_path = '/ftp_samba/112/spider/fanyule_two/douban'  # linux上的文件目录
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'douban_movie_' + date)
    tmp_file_name = os.path.join(dest_path, 'douban_movie_' + date + '.tmp')
    fileout = open(tmp_file_name, 'a')

    threads = []
    for i in xrange(30):
        t = threading.Thread(target=movie, args=(fileout, lock))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    try:
        fileout.flush()
        fileout.close()
    except IOError as e:
        time.sleep(1)
        fileout.close()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end'
    os.rename(tmp_file_name, dest_file_name)


if __name__ == '__main__':
    main()





























