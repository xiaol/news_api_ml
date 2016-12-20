# -*- coding: utf-8 -*-
# @Time    : 16/11/30 下午2:24
# @Author  : liulei
# @Brief   : 
# @File    : topic_modle.py
# @Software: PyCharm Community Edition
# Download data if you haven't already
import json

import graphlab as gl
import os
import topic_model_doc_process
from util import doc_process

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
#gl.set_runtime_config('GRAPHLAB_DEFAULT_NUM_PYLAMBDA_WORKERS', 64)

g_channel_model_dict = {}
import datetime
data_dir = real_dir_path + '/data/'
model_dir = real_dir_path + '/models/'

model_v = ''

save_model_sql = "insert into topic_models (model_v, ch_name, topic_id, topic_words) VALUES (%s, %s, %s, %s)"
def save_model_to_db(model, ch_name):
    global model_v
    model_create_time = datetime.datetime.now()
    #model 版本以时间字符串
    model_v = model_create_time.strftime('%Y%m%d%H%M%S')
    sf = model.get_topics(num_words=20, output_type='topic_words')

    conn, cursor = doc_process.get_postgredb()
    for i in xrange(0, len(sf)):
        try:
            keys_words_jsonb = json.dumps(sf[i]['words'])
            cursor.execute(save_model_sql, [model_v, ch_name, str(i), keys_words_jsonb])
            conn.commit()
        except Exception:
            print 'save model to db error'
    conn.close()


def create_model_proc(csv_file, model_save_dir=None):
    docs = gl.SFrame.read_csv(data_dir+csv_file, header=False)
    docs = gl.text_analytics.count_words(docs['X1'])
    docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
    model = gl.topic_model.create(docs, num_iterations=100, num_burnin=100, num_topics=10000)
    g_channel_model_dict[csv_file] = model
    save_model_to_db(model, csv_file)
    #save model
    if model_save_dir:
        model.save(model_save_dir+'/'+csv_file)


def create_models():
    global model_dir
    model_create_time = datetime.datetime.now()
    time_str = model_create_time.strftime('%Y-%m-%d-%H-%M-%S')
    model_path = model_dir + time_str
    if not os.path.exists(model_path):
        os.mkdir(model_path)

    from topic_model_doc_process import channel_for_topic
    for chanl in channel_for_topic:
        create_model_proc(chanl, model_path)
    print 'create models finished!!'


def load_models(models_dir):
    print 'load_models()'
    global g_channel_model_dict
    if len(g_channel_model_dict) != 0:
        g_channel_model_dict.clear()
    models_files = os.listdir(models_dir)
    for mf in models_files:
        print '    load ' + mf
        print models_dir
        g_channel_model_dict[mf] = gl.load_model(models_dir + '/'+ mf)


def get_newest_model_dir():
    global model_dir
    models_dir = real_dir_path + '/models'
    models = os.listdir(models_dir)
    ms = {}
    for m in models:
        ms[m] = m.replace('-', '')
    ms_sort = sorted(ms.items(), key=lambda x:x[1])
    return model_dir + ms_sort.pop()[0]


def lda_predict_core(nid):
    global g_channel_model_dict
    if len(g_channel_model_dict) == 0:
        load_models(get_newest_model_dir())

    words_list, chanl_name = topic_model_doc_process.get_words_on_nid(nid)
    for k in g_channel_model_dict.keys():
        print type(k)
        print k
    if chanl_name not in g_channel_model_dict.keys():
        print 'Error: channel name is not in models' + '---- ' + chanl_name
        return '', []
    s = ''
    for i in words_list:
        s += i + ' '
    ws = gl.SArray([s,])
    docs = gl.SFrame(data={'X1':ws})
    docs = gl.text_analytics.count_words(docs['X1'])
    docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)

    #预测得分最高的topic
    #pred = g_channel_model_dict[chanl_name].predict(docs)
    #print pred
    #print '%s' % str(sf[pred[0]]['words']).decode('string_escape')

    print '=================================='
    pred2 = g_channel_model_dict[chanl_name].predict(docs,
                                                     output_type='probability')
    return chanl_name, pred2


def lda_predict(nid):
    global g_channel_model_dict
    chanl_name, pred = lda_predict_core(nid)
    sf = g_channel_model_dict[chanl_name].get_topics(num_words=20,
                                                     output_type='topic_words')
    num_dict = {}
    num = 0
    for i in pred[0]:
        num_dict[i] = num
        num += 1
    probility = sorted(num_dict.items(), key=lambda d: d[0], reverse=True)
    i = 0
    res = {}
    while i < 3 and i < len(probility) and probility[i][0] > 0.1:
        res[probility[i][0]] = sf[probility[i][1]]['words']
        i += 1
    return res


news_topic_sql = "insert into news_topic (nid, model_v, ch_name, topic_id, probability) VALUES (%s,  %s, %s, %s, %s)"
def lda_predict_and_save(nid):
    global model_v
    print type(nid)
    print nid
    ch_name, pred = lda_predict_core(nid)
    if len(pred) == 0:
        return

    num_dict = {}
    num = 0
    for i in pred[0]:
        num_dict[i] = num
        num += 1
    probility = sorted(num_dict.items(), key=lambda d: d[0], reverse=True)
    i = 0
    to_save = {}
    while i < 3 and i < len(probility) and probility[i][0] > 0.1:
        to_save[probility[i][1]] = probility[i][0]
        i += 1

    conn, cursor = doc_process.get_postgredb()
    for item in to_save.items():
        cursor.execute(news_topic_sql, [nid, model_v, ch_name, item[0], item[1]])
    conn.commit()
    conn.close()


user_topic_sql = 'select * from usertopics where uid = %s and model_v = %s and ch_name=%s'
user_topic_insert_sql = 'insert into usertopics (uid, model_v, ch_name, topics) VALUES ({0}, {1}, {2}, {3})'
#收集用户topic
#nids_info: 包含nid号及nid被点击时间
def coll_user_topics(uid, nids_info):
    import datetime
    from datetime import timedelta
    global g_channel_model_dict, model_v
    conn, cursor = doc_process.get_postgredb()

    for nid_info in nids_info:
        nid = nid_info[0]
        valid_time = nid_info[1] + timedelta(days=30) #有效时间定为30天
        ch_name, pred = lda_predict_core(nid)
        if len(pred) == 0:
            continue

        cursor.execute(user_topic_sql, [uid, model_v, ch_name])
        rows = cursor.fetchall()
        new_user = False
        if len(rows) == 0: #此版本的model下没有记录该用户的点击行为
            new_user = True

        num_dict = {}
        num = 0 #标记topic的index
        for i in pred[0]:  #pred[0]是property值
            num_dict[i] = num
            num += 1
        probility = sorted(num_dict.items(), key=lambda d: d[0], reverse=True)
        i = 0
        to_save = {}
        while i < 3 and i < len(probility) and probility[i][0] > 0.1:
            to_save[probility[i][1]] = probility[i][0]
            i += 1

        if new_user: #插入新数据
            user_topics = {}
            for item in to_save.items():
                user_topics[item[0]] = (item[1], valid_time)
            cursor.execute(user_topic_insert_sql.format(nid, model_v, ch_name, json.dumps(user_topics)))
        else: #update user-topics
            user_topics = {}
            for r in rows:
                user_topics = r[3]  #已有的topics字典
                break
            for item in to_save.items():
                if item[0] in user_topics.keys():
                    user_topics[item[0]][0] += item[1]
                    user_topics[item[0]][1] = valid_time
                else:
                    user_topics[item[0]][0] = item[1]
                    user_topics[item[0]][1] = valid_time
            cursor.execute(user_topic_insert_sql.format(nid, model_v, ch_name, json.dumps(user_topics)))


user_click_sql = "select uid, nid, max(ctime) ctime from newsrecommendclick  where CURRENT_DATE - INTEGER '10' <= DATE(ctime) group by uid,nid"
def get_user_topics():
    conn, cursor = doc_process.get_postgredb()
    cursor.execute(user_click_sql)
    rows = cursor.fetchall()
    user_news_dict = {}
    for r in rows:
        uid = r[0]
        if uid in user_news_dict.keys():
            user_news_dict[uid].append((r[1], r[2]))
        else:
            user_news_dict[uid] = set()
            user_news_dict[uid].add((r[1], r[2]))
    for item in user_news_dict.items():
        coll_user_topics(item[0], item[1])


#取十万条新闻加入队列做预测,并保存至数据库
channle_sql ='SELECT a.nid FROM newslist_v2 a \
RIGHT OUTER JOIN (select * from channellist_v2 where cname in ({0})) c \
ON \
a.chid=c.id ORDER BY nid DESC LIMIT {1}'
#search_sql = 'select nid from newslist_v2 ordered by nid  DESC limit "%d" '
def produce_news_topic_manual(num):
    conn, cursor = doc_process.get_postgredb()
    channels = ', '.join("\'" + ch+"\'" for ch in topic_model_doc_process.channel_for_topic)
    print channels
    #cursor.execute(channle_sql, [channels, num])
    print channle_sql.format(channels, num)
    cursor.execute(channle_sql.format(channels, num))
    rows = cursor.fetchall()
    import redis_lda
    print len(rows)
    for r in rows:
        redis_lda.produce_nid(r[0])






