# -*- coding: utf-8 -*-
# @Time    : 16/12/19 上午9:53
# @Author  : liulei
# @Brief   : 
# @File    : redis_lda.py
# @Software: PyCharm Community Edition
from redis import Redis
import traceback
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
        topic_model.lda_predict_and_save(nid)
        #url = 'http://127.0.0.1:9989/topic_model/get_topic_on_nid'
        #data = {}
        #data['nid'] = nid
        #print 'consume id =' + str(nid)
        #response = requests.get(url, params=data)

import json
user_click_queue = 'user_click_queue'
def produce_user_click(uid, nid, ctime):
    global redis_inst
    print 'produce user ' + str(uid) + ' ' + str(nid)
    d = json.dumps([uid, nid, ctime])
    print type(d)
    print d
    redis_inst.lpush(user_click_queue, json.dumps([uid, nid, ctime]))


from graphlab_lda import topic_model
#消费用户点击行为
def consume_user_click():
    global redis_inst
    try:
        while True:
            data = json.loads(redis_inst.blpop(user_click_queue)[1])
            uid = data[0]
            nid = data[1]
            ctime = data[2]
            topic_model.predict_user_topic_core(uid, nid, ctime)
            print 'consum ' + str(uid) + ' ' + str(nid) + ' ' + 'finished--------'
    except :
        traceback.print_exc()

