# -*- coding: utf-8 -*-
# @Time    : 17/2/8 下午3:19
# @Author  : liulei
# @Brief   : 
# @File    : nid_queue.py
# @Software: PyCharm Community Edition

from graphlab_lda import topic_model
from graphlab_kmeans import kmeans
from sim_hash import sim_hash
from redis import Redis
import traceback
import json
import datetime

redis_inst = Redis(host='localhost', port=6379)
nid_queue = 'nid_queue'
lda_queue = 'lda_queue'
kmeans_queue = 'kmeans_queue'
simhash_queue = 'simhash_queue'
ads_queue = 'ads_queue'


def produce_nid(nid):
    global redis_inst
    #redis_inst.lpush(nid_queue, nid)
    redis_inst.lpush(lda_queue, nid)
    redis_inst.lpush(kmeans_queue, nid)
    redis_inst.lpush(simhash_queue, nid)
    redis_inst.lpush(ads_queue, nid)


'''
def consume_nid():
    global redis_inst
    while True:
        nid = redis_inst.brpop(nid_queue)[1]
        redis_inst.lpush(lda_queue, nid)
        redis_inst.lpush(kmeans_queue, nid)
        redis_inst.lpush(simhash_queue, nid)
        redis_inst.lpush(ads_queue, nid)
'''


def consume_nid_lda(num=1):
    global redis_inst
    n = 0
    nid_list = []
    t0 = datetime.datetime.now()
    while True:
        nid = redis_inst.brpop(lda_queue)[1]
        nid_list.append(nid)
        n += 1
        t1 = datetime.datetime.now()
        if n >=num or (t1 - t0).total_seconds() > 10:
            topic_model.predict_topic_nids(nid_list)
            n = 0
            del nid_list[:]
            t0 = datetime.datetime.now()


def consume_nid_kmeans(num=1):
    global redis_inst
    n = 0
    nid_list = []
    t0 = datetime.datetime.now()
    while True:
        nid = redis_inst.brpop(kmeans_queue)[1]
        nid_list.append(nid)
        n += 1
        t1 = datetime.datetime.now()
        if n >=num or (t1 - t0).total_seconds() > 10:
            kmeans.kmeans_predict(nid_list)
            n = 0
            del nid_list[:]
            t0 = datetime.datetime.now()


def consume_nid_simhash(num=1):
    global redis_inst
    n = 0
    nid_list = []
    t0 = datetime.datetime.now()
    while True:
        nid = redis_inst.brpop(simhash_queue)[1]
        nid_list.append(nid)
        n += 1
        t1 = datetime.datetime.now()
        if n >=num or (t1 - t0).total_seconds() > 10:
            sim_hash.cal_and_check_news_hash(nid_list)
            n = 0
            del nid_list[:]
            t0 = datetime.datetime.now()


def consume_nid_ads():
    global redis_inst
    import requests
    while True:
        nid = redis_inst.brpop(ads_queue)[1]
        url = 'http://127.0.0.1:9999/news_process/RemoveAdsOnnidCore'
        data = {}
        data['nid'] = nid
        response = requests.get(url, params=data)


user_click_queue = 'user_click_queue'
def produce_user_click(uid, nid, ctime):
    global redis_inst
    print 'produce user ' + str(uid) + ' ' + str(nid) + ' ' + ctime
    redis_inst.lpush(user_click_queue, json.dumps([uid, nid, ctime]))


#消费用户点击行为
def consume_user_click():
    global redis_inst
    while True:
        try:
            data = json.loads(redis_inst.brpop(user_click_queue)[1])
            uid = data[0]
            nid = data[1]
            ctime = data[2]
            print 'consum ' + str(uid) + ' ' + str(nid) + ' ' + 'begin'
            #for topic model
            topic_model.predict_click((uid, nid, ctime))
            #for kmeans
            kmeans.predict_click((uid, nid, ctime))
        except :
            traceback.print_exc()
            continue