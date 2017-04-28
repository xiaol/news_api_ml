# -*- coding: utf-8 -*-
# @Time    : 17/4/27 下午5:36
# @Author  : liulei
# @Brief   : 生成专题
# @File    : subject.py
# @Software: PyCharm Community Edition
import requests
import json
from util.doc_process import get_postgredb_query
#生成专题
def generate_subject(sub_nids):
    prefix = 'http://fez.deeporiginalx.com:9001'
    create_url = prefix + '/topics'
    cookie = {'Authorization':'f76f3276c1ac832b935163c451f62a2abf5b253c'}
    #set subject name as one title of one piece of news
    sql = "select title from newslist_v2 where nid=%s"
    conn, cursor = get_postgredb_query()
    cursor.execute(sql, (sub_nids[0],))
    row = cursor.fetchone()
    sub_name = row[0]
    data = {'name':sub_name}
    response = requests.post(create_url, data=data, cookies=cookie)
    content = json.loads(response.content)
    id = content['id']

    topic_class_url = prefix + '/topic_classes'
    data = {'topic': id, 'name': 'random'}
    response = requests.post(topic_class_url, data=data, cookies=cookie)
    class_id = json.loads(response.content)['id']

    add_nid_url = prefix + '/topic_news'
    for nid in sub_nids:
        data = {'topic_id':id, 'news_id':nid, 'topic_class_id':class_id}
        requests.post(add_nid_url, data=data, cookies=cookie)