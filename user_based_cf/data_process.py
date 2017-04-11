# -*- coding: utf-8 -*-
# @Time    : 17/4/5 下午1:56
# @Author  : liulei
# @Brief   : 协同过滤数据处理; user-based cf算法。 首先收集用户点击行为; 用户相似度矩阵每隔一两天就需要更新
#             item使用LDA形成的topic_id代替news_id,降维
# @File    : data_process.py
# @Software: PyCharm Community Edition
import os
import traceback
from util import logger
import pandas as pd
from util.doc_process import get_postgredb_query
import datetime
import math


real_dir_path = os.path.split(os.path.realpath(__file__))[0] #文件所在路径
log_cf = logger.Logger('log_cf', os.path.join(real_dir_path, 'log', 'log.txt'))


################################################################################
#@brief: 获取最新的topic版本
################################################################################
def get_newest_topic_v():
    topic_sql = "select model_v from user_topics_v2 group by model_v"
    conn, cursor = get_postgredb_query()
    cursor.execute(topic_sql)
    rows = cursor.fetchall()
    topic_vs = []
    for row in rows:
        topic_vs.append(row[0])
    conn.close()
    return max(topic_vs)


click_file = ''
click_query_sql = "select uid, nid, ctime from newsrecommendclick where ctime > now() - interval '%s day'"
#收集用户一段时间内的的点击行为
def coll_click():
    global click_file
    try:
        conn, cursor = get_postgredb_query()
        cursor.execute(click_query_sql, (30,))
        rows = cursor.fetchall()
        user_ids = []
        nid_ids = []
        click_time = []
        for r in rows:
            user_ids.append(r[0])
            nid_ids.append(r[1])
            click_time.append(r[2])

        df = pd.DataFrame({'uid':user_ids, 'nid':nid_ids, 'ctime':click_time}, columns=('uid', 'nid', 'ctime'))
        time_str = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        click_file = os.path.join(real_dir_path, 'data', time_str)
        df.to_csv(click_file, index=False)
        #calculate similarity.  user_dict is a map of userid and index, W is simility matrix with index representing userid
        W = get_user_click_similarity(user_ids, nid_ids, click_time)
        conn.close()
    except:
        log_cf.exception(traceback.format_exc())
        conn.close()


user_topic_prop = "select uid, topic_id, probability from user_topics_v2 where model_v = '{}'"
def coll_user_topics():
    try:
        log_cf.info('coll_user_topics begin ...')
        conn, cursor = get_postgredb_query()
        cursor.execute(user_topic_prop.format(get_newest_topic_v()))
        rows = cursor.fetchall()
        user_ids = []
        topic_ids = []
        props = []
        log_cf.info('query user topic finished. {} item found.'.format(len(rows)))
        for r in rows:
            user_ids.append(r[0])
            topic_ids.append(r[1])
            props.append(r[2])

        df = pd.DataFrame({'uid': user_ids, 'topic': topic_ids, 'property': props}, columns=('uid', 'topic', 'property'))
        time_str = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        topic_file = os.path.join(real_dir_path, 'data', 'user_topic_data'+time_str + '.txt')
        df.to_csv(topic_file, index=False)
        log_cf.info('uid-topic-property are save to {}'.format(topic_file))
        conn.close()
        #calcute similarity and save to file
        W = get_user_topic_similarity(user_ids, topic_ids, props)
        user_user_file = os.path.join(real_dir_path, 'data', 'user_topic_similarity'+time_str + '.txt')
        master_user = []
        slave_user = []
        similarity = []
        for item in W:
            #master_user.append(item[0])
            for i2 in item[1].items():
                master_user.append(item[0])
                slave_user.append(i2[0])
                similarity.append(i2[1])
        df2 = pd.DataFrame({'uid1':master_user, 'uid2':slave_user, 'similarity':similarity}, columns=('uid1', 'uid2', 'similarity'))
        df2.to_csv(user_user_file, index=False)
        log_cf.info('uid1-uid2-similarity are save to {}'.format(user_user_file))
    except:
        log_cf.exception(traceback.format_exc())



#获取item-user倒排表, 格式  {nid1:[uid1, uid2...], nid2:...}
def get_user_click_similarity(users, nids, times):
    user_set = set(users)
    #记录用户id的索引id
    user_dict = {}
    user_invert_dict = {}
    n = 0
    for i in user_set:
        user_dict[i] = n
        user_invert_dict[n] = i
        n += 1
    item_user_dict = dict()
    for i in xrange(len(nids)):
        if nids[i] not in item_user_dict.keys():
            item_user_dict[nids[i]] = set()
        item_user_dict[nids[i]].add(user_dict[users[i]])

    C = dict()  #记录用户相关矩阵
    N = dict()  #记录每个用户看了多少item
    for i, i_users in item_user_dict.items():
        for u in i_users:
            N[u] += 1
            for v in i_users:
                if u == v:
                    continue
                C[u][v] += 1

    #calcute final similirity matrix W
    W = dict()
    for u, related_users in C.items():
        for v, cuv in related_users.items():
            W[user_invert_dict[u]][user_invert_dict[v]] = cuv / math.sqrt(N[u] * N[v])

    return W


#获取topic-user倒排表, 格式  {topic1:[uid1, uid2...], topic2:...}
def get_user_topic_similarity(users, topics, props):
    user_set = set(users)
    #记录用户id的索引id
    user_dict = {}
    user_invert_dict = {}
    n = 0
    for i in user_set:
        user_dict[i] = n
        user_invert_dict[n] = i
        n += 1
    #get invert-tabel
    topic_user_dict = dict()
    for i in xrange(len(topics)):
        if topics[i] not in topic_user_dict.keys():
            topic_user_dict[topics[i]] = dict()
        topic_user_dict[topics[i]][user_dict[users[i]]] = props[i]

    #get user relationship matrix
    C = dict()  #记录用户相关矩阵
    N = dict()  #记录每个用户兴趣总值, 所有probability相加
    for i, i_users_prop in topic_user_dict.items():
        for u_prop in i_users_prop.items():  #字典
            if u_prop[0] not in N:
                N[u_prop[0]] = 0
            N[u_prop[0]] += u_prop[1]
            for v_prop in i_users_prop.items():
                if u_prop[0] == v_prop[0]:
                    continue
                if u_prop[0] not in C:
                    C[u_prop[0]] = {}
                if v_prop[0] not in C[u_prop[0]]:
                    C[u_prop[0]][v_prop[0]] = 0
                C[u_prop[0]][v_prop[0]] += min(u_prop[1], v_prop[1])  #取最小的probability作为相似值

    #calcute final similirity matrix W based on user matrix
    W = dict()
    for u, related_users in C.items():
        for v, cuv in related_users.items():
            if user_invert_dict[u] not in W:
                W[user_invert_dict[u]] = dict()
            if user_invert_dict[v] not in W[user_invert_dict[u]]:
                W[user_invert_dict[u]][user_invert_dict[v]] = 0
            W[user_invert_dict[u]][user_invert_dict[v]] = cuv / math.sqrt(N[u] * N[v])

    return W


if __name__ == '__main__':
    #coll_click()
    coll_user_topics()





