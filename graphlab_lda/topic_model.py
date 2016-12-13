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
    #if model_save_dir:
    #    model.save(model_save_dir+'/'+csv_file)


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
    global g_channel_model_dict, model_dir
    if len(g_channel_model_dict) != 0:
        g_channel_model_dict.clear()
    models_files = os.listdir(models_dir)
    for mf in models_files:
        g_channel_model_dict[models_files] = gl.load_model(model_dir + mf)

def lda_predict_core(nid):
    global g_channel_model_dict
    words_list, chanl_name = topic_model_doc_process.get_words_on_nid(nid)
    if chanl_name not in g_channel_model_dict.keys():
        print 'Error: channel name is not in models'
        return
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
    ch_name, pred = lda_predict_core(nid)

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





