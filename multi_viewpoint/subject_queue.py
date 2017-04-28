# -*- coding: utf-8 -*-
# @Time    : 17/4/28 上午11:16
# @Author  : liulei
# @Brief   : 
# @File    : subject_queue.py
# @Software: PyCharm Community Edition
from redis import Redis
import json
import traceback


redis_inst = Redis(host='localhost', port=6378)
subject_queue = 'subject_queue' #专题队列

def product_subject(sub_nids):
    redis_inst.lpush(subject_queue, json.dumps(sub_nids))

def consume_subject():
    from subject import generate_subject
    global redis_inst
    while True:
        try:
            nids = json.loads(redis_inst.brpop(subject_queue)[1])
            print nids
            generate_subject(nids)

        except :
            traceback.print_exc()
            continue
