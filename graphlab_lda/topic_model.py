# -*- coding: utf-8 -*-
# @Time    : 16/11/30 下午2:24
# @Author  : liulei
# @Brief   : 
# @File    : topic_modle.py
# @Software: PyCharm Community Edition
# Download data if you haven't already
import graphlab as gl
import os
import topic_model_doc_process

real_dir_path = os.path.split(os.path.realpath(__file__))[0]

g_channel_model_dict = {}
import datetime
data_dir = real_dir_path + '/data_50000/'
model_dir = real_dir_path + '/models/'
def create_model_proc(csv_file, model_save_dir=None):
    docs = gl.SFrame.read_csv(data_dir+csv_file, header=False)
    docs = gl.text_analytics.count_words(docs['X1'])
    docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
    model = gl.topic_model.create(docs, num_iterations=100, num_burnin=50, num_topics=10000)
    g_channel_model_dict[csv_file] = model
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

def lda_predict(nid):
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
    sf = g_channel_model_dict[chanl_name].get_topics(num_words=20,
                                                     output_type='topic_words')

    #预测得分最高的topic
    #pred = g_channel_model_dict[chanl_name].predict(docs)
    #print pred
    #print '%s' % str(sf[pred[0]]['words']).decode('string_escape')

    print '=================================='
    pred2 = g_channel_model_dict[chanl_name].predict(docs,
                                                     output_type='probability')
    num_dict = {}
    num = 0
    for i in pred2[0]:
        num_dict[i] = num
        num += 1
    probility = sorted(num_dict.items(), key=lambda d: d[0], reverse=True)
    i = 0
    res = []
    while i < 3 and i < len(probility) and probility[i][0] > 0.1:
        print probility
        print '%s' % str(sf[probility[i][1]]['words']).decode('string_escape')
        res.append(sf[probility[i][1]]['words'])
        i += 1
    return res





