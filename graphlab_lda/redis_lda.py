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

import logging
logger_produce = logging.getLogger(__name__)
logger_produce.setLevel(logging.INFO)
handler = logging.FileHandler('log/lda_log.txt')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger_produce.addHandler(handler)

logger_consume = logging.getLogger(__name__)
logger_consume.setLevel(logging.INFO)
handler = logging.FileHandler('log/lda_consume_log.txt')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger_consume.addHandler(handler)

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
    global redis_inst, logger_produce
    logger_produce.info('produce user ' + str(uid) + ' ' + str(nid))
    redis_inst.lpush(user_click_queue, json.dumps([uid, nid, ctime]))


from graphlab_lda import topic_model
#消费用户点击行为
def consume_user_click():
    global redis_inst, logger_consume
    try:
        while True:
            data = json.loads(redis_inst.brpop(user_click_queue)[1])
            uid = data[0]
            nid = data[1]
            ctime = data[2]
            logger_consume.info('consum ' + str(uid) + ' ' + str(nid) + ' ' + 'begin')
            topic_model.predict_user_topic_core(uid, nid, ctime)
            logger_consume.info('consum ' + str(uid) + ' ' + str(nid) + ' ' + 'finished--------')
    except :
        traceback.print_exc()

