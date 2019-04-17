# coding:utf-8
__author__ = 'xxj'

import sys
import time
import math
import os
from rediscluster import StrictRedisCluster
import json
import re
import lxml.etree
import multiprocessing
from multiprocessing import Process, Pool, Manager, Queue
from Queue import Empty
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout
import requests

reload(sys)
sys.setdefaultencoding('utf-8')
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
}

# 不能如此定义
# MOVIE_QUEUE = Manager().Queue()    # 电影名队列
# PROXY_IP_Q = Manager().Queue()    # 代理ip队列
# PROCESS_PROXY_MAP = Manager().dict()    # 进程与代理字典


def get_proxy_ips(num):
    '''
    获取代理ip
    :return:
    '''
    # global PROXY_IP_Q    python多进程默认不能共享全局变量
    proxies_ls = []
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
        }
        url = 'https://proxyapi.mimvp.com/api/fetchsecret.php?orderid=860068921904605585&num={num}&http_type=3&result_fields=1,2,3&result_format=json'.format(num=num)
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
            # PROXY_IP_Q.put(proxies)
            proxies_ls.append(proxies)
        return proxies_ls
    except BaseException as e:
        print 'BaseException：', '代理ip异常', e
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


def movie(lock, tmp_file_name, MOVIE_QUEUE, PROCESS_PROXY_MAP, PROXY_IP_Q):
    fileout = open(tmp_file_name, 'a')    # 部分对象（如：文本对象、redis对象等）无法通过Manager()实现数据共享,所以将该对象放在函数内（也就是说每一个子进程都会生成一个文本对象或redis对象）
    while not MOVIE_QUEUE.empty():
        try:
            process_name = multiprocessing.current_process().name
            if not PROCESS_PROXY_MAP.get(process_name):
                proxies = PROXY_IP_Q.get(False)
                PROCESS_PROXY_MAP[process_name] = proxies
            proxies = PROCESS_PROXY_MAP.get(process_name)

            line = MOVIE_QUEUE.get(False)
            movie_name = line.split('\t')[0]    # 电影名
            movie_url = 'https://www.douban.com/search?cat=1002&q={q}'.format(q=movie_name)
            print time.strftime('[%Y-%m-%d %H:%M:%S] 豆瓣url：{url} process_name：{process_name} proxies：{proxies}'
                                .format(url=movie_url.replace('%', ''), process_name=process_name, proxies=proxies))    # 要求格式化字符串中没有%字符，不然使用time.strftime()方法就会报错ValueError: Invalid format string(因为该方法会识别%为特殊字符使用)
            response = requests.get(url=movie_url, headers=headers, proxies=proxies, timeout=10)
            response_text = response.text
            xpath_obj = lxml.etree.HTML(response_text)
            if ('<script>var d=[navigator.platform,navigator.userAgent,navigator.vendor].join("|");' in response_text) or \
                    ('检测到有异常请求从你的 IP 发出' in response_text):
                raise DoubanException('douban exception')
            else:    # 存在该电影或ip被限制
                results = xpath_obj.xpath('//div[@class="result"]')
                if results:
                    result = results[0]  # 获取搜索结果的第一条
                    pic_url = result.xpath('.//a[@class="nbg"]/img/@src')[0]  # 图片url
                    # print '图片url：', pic_url
                    tag = result.xpath('.//div[@class="title"]/h3/span/text()')[0].replace('[', '').replace(']', '')  # 标签
                    # print '标签：', tag
                    name = result.xpath('.//div[@class="title"]/h3/a/text()')[0]  # 电影名
                    # print '电影名：', name
                    url = result.xpath('.//div[@class="title"]/h3/a/@href')[0]  # 电影url
                    url = douban_url(url, proxies)    # 跳转后的url接口
                    # print '电影url：', url
                    des = result.xpath('.//span[@class="subject-cast"]/text()')[0]
                    # print des, repr(des)
                    movie_time = des.split(' / ')[-1]  # 时间
                    # print '年份：', movie_time
                    content = '\t'.join([line, tag, name, url, pic_url, movie_time])
                    fileout.write(content)
                    fileout.write('\n')
                    fileout.flush()
                else:    # ip被限制
                    print '该影视无搜索结果：{}'.format(movie_name)

        except ConnectTimeout as e:
            with lock:
                print time.strftime('[%Y-%m-%d %H:%M:%S] ConnectTimeout异常：{line} {process_name}'
                                    .format(line=line.replace('%', ''), process_name=process_name))
                MOVIE_QUEUE.put(line)
                PROCESS_PROXY_MAP.pop(process_name)    # 从THREAD_PROXY_MAP中删除失效的代理ip
                if PROXY_IP_Q.empty():
                    print '代理ip队列中的代理ip为空，重新获取代理ip'
                    # get_proxy_ips(8)
                    proxies_ls = get_proxy_ips(8)    # 解决代理ip队列（多进程不会共享全局变量）
                    for proxies in proxies_ls:
                        PROXY_IP_Q.put(proxies)
                    print '获取到的代理ip的数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                print 'ip被限制, 切换新的代理ip：{}'.format(proxies)
                PROCESS_PROXY_MAP[process_name] = proxies

        except ConnectionError as e:
            with lock:
                print time.strftime('[%Y-%m-%d %H:%M:%S] ConnectionError异常：{line} {process_name}'
                                    .format(line=line.replace('%', ''), process_name=process_name))
                MOVIE_QUEUE.put(line)
                PROCESS_PROXY_MAP.pop(process_name)    # 从THREAD_PROXY_MAP中删除失效的代理ip
                if PROXY_IP_Q.empty():
                    print '代理ip队列中的代理ip为空，重新获取代理ip'
                    # get_proxy_ips(8)
                    proxies_ls = get_proxy_ips(8)
                    for proxies in proxies_ls:
                        PROXY_IP_Q.put(proxies)
                    print '获取到的代理ip的数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                print 'ip被限制, 切换新的代理ip：{}'.format(proxies)
                PROCESS_PROXY_MAP[process_name] = proxies

        except DoubanException as e:
            with lock:
                print time.strftime('[%Y-%m-%d %H:%M:%S] DoubanException异常：{line} {process_name}'
                                    .format(line=line.replace('%', ''), process_name=process_name))
                MOVIE_QUEUE.put(line)
                PROCESS_PROXY_MAP.pop(process_name)    # 从THREAD_PROXY_MAP中删除失效的代理ip
                if PROXY_IP_Q.empty():
                    print '代理ip队列中的代理ip为空，重新获取代理ip'
                    # get_proxy_ips(8)
                    proxies_ls = get_proxy_ips(8)
                    for proxies in proxies_ls:
                        PROXY_IP_Q.put(proxies)
                    print '获取到的代理ip的数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                print 'ip被限制, 切换新的代理ip：{}'.format(proxies)
                PROCESS_PROXY_MAP[process_name] = proxies

        except ReadTimeout as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'ReadTimeout异常：', line
            MOVIE_QUEUE.put(line)

        except Empty as e:
            pass

        except BaseException as e:
            with lock:
                print time.strftime('[%Y-%m-%d %H:%M:%S] BaseException异常：'), line, process_name
                PROCESS_PROXY_MAP.pop(process_name)    # 从THREAD_PROXY_MAP中删除失效的代理ip
                if PROXY_IP_Q.empty():
                    print '代理ip队列中的代理ip为空，重新获取代理ip'
                    # get_proxy_ips(8)
                    proxies_ls = get_proxy_ips(8)
                    for proxies in proxies_ls:
                        PROXY_IP_Q.put(proxies)
                    print '获取到的代理ip的数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                print 'ip被限制, 切换新的代理ip：{}'.format(proxies)
                PROCESS_PROXY_MAP[process_name] = proxies


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


def main():
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    lock = Manager().Lock()
    MOVIE_QUEUE = Manager().Queue()  # 电影名队列
    PROXY_IP_Q = Manager().Queue()  # 代理ip队列
    PROCESS_PROXY_MAP = Manager().dict()  # 进程与代理字典

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
            MOVIE_QUEUE.put(line.strip())
    print '源数据队列中的数量：', MOVIE_QUEUE.qsize()

    proxies_ls = get_proxy_ips(8)
    for proxies in proxies_ls:
        PROXY_IP_Q.put(proxies)
    ip_num = PROXY_IP_Q.qsize()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '获取代理ip的数量：', ip_num
    # print 'PROXY_IP_Q的id1：', id(PROXY_IP_Q)

    dest_path = '/ftp_samba/112/spider/fanyule_two/douban'  # linux上的文件目录
    # dest_path = os.getcwd()    # windows上的文件目录
    if not os.path.exists(dest_path):    # 在使用多线程时，当
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'douban_movie_' + date)
    tmp_file_name = os.path.join(dest_path, 'douban_movie_' + date + '.tmp')
    fileout = open(tmp_file_name, 'a')

    # processes = []
    # for i in xrange(8):
    #     p = Process(target=movie, args=(lock, tmp_file_name, MOVIE_QUEUE, PROCESS_PROXY_MAP, PROXY_IP_Q))     # 尽管相关参数是全局变量，但是初始化进程类时，还是需要将这些参数写入，不然就会报错 NameError: global name 'MOVIE_QUEUE' is not defined（所以说这些变量放在if __name__=='__main__'下和放在main()方法中是一样的）
    #     p.start()
    #     processes.append(p)
    #
    # for p in processes:
    #     p.join()

    pool = Pool(8)    # 进程池中进程的数量
    for i in xrange(8):    # 开启的进程数量
        pool.apply_async(movie, args=(lock, tmp_file_name, MOVIE_QUEUE, PROCESS_PROXY_MAP, PROXY_IP_Q))    # # 对于multiprocessing 下的Lock和 Queue模块只适合Process方法使用，不适合进程池pool使用；但是Manager下的Lock和Queue都适合它们使用。所以推荐使用Manager下的Lock和Queue，都可以实现进程间共享

    pool.close()
    pool.join()

    try:
        fileout.flush()
        fileout.close()
    except IOError as e:
        time.sleep(1)
        fileout.close()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end'
    os.rename(tmp_file_name, dest_file_name)


if __name__ == '__main__':
    # MOVIE_QUEUE = Manager().Queue()  # 电影名队列
    # PROXY_IP_Q = Manager().Queue()  # 代理ip队列
    # PROCESS_PROXY_MAP = Manager().dict()  # 进程与代理字典      # 简而言之，全局变量对多进程是无效的（如：）
    # lock = Manager().Lock()
    main()
    # movie_t()