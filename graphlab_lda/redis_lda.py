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

user_click_queue = 'user_click_queue'
def produce_user_click(uid, nid, ctime):
    global redis_inst
    redis_inst.lpush(user_click_queue, (uid, nid, ctime))


#消费用户点击行为
def consume_user_click():
    global redis_inst
    import requests
    while True:
        data = redis_inst.brpop(user_click_queue)
        print type(data)
        print data
        uid = redis_inst.brpop(user_click_queue)[1][0]
        nid = redis_inst.brpop(user_click_queue)[1][1]
        ctime = redis_inst.brpop(user_click_queue)[1][2]
        url = 'http://127.0.0.1:9986/topic_model/predict_one_click'
        data = {}
        data['uid'] = uid
        data['nid'] = nid
        data['ctime'] = ctime
        print 'consume click =' + str(uid) + str(nid)
        response = requests.get(url, params=data)
