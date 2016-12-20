# -*- coding: utf-8 -*-
# @Time    : 16/12/19 上午9:53
# @Author  : liulei
# @Brief   : 
# @File    : redis_lda.py
# @Software: PyCharm Community Edition
from redis import Redis
redis_inst = Redis(host='localhost', port=6379)
nid_queue = 'nid_queue_for_lda'


def produce_nid(nid):
    global redis_inst
    print 'produce ' + str(nid) + ' for lda'
    redis_inst.lpush(nid_queue, nid)


def consume_nid():
    global redis_inst
    import requests
    while True:
        nid = redis_inst.brpop(nid_queue)[1]
        url = 'http://127.0.0.1:9989/topic_model/get_topic_on_nid'
        data = {}
        data['nid'] = nid
        print 'consume id =' + str(nid)
        response = requests.get(url, params=data)
