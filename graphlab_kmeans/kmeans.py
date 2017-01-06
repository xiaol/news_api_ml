# -*- coding: utf-8 -*-
# @Time    : 17/1/4 上午10:10
# @Author  : liulei
# @Brief   : 使用kmeans 算法,为频道内推荐提供比主题模型更粗颗粒度的数据
# @File    : kmeans.py
# @Software: PyCharm Community Edition

import os
import datetime
import graphlab as gl
from util import doc_process

#添加日志
import logging
real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger = logging.getLogger(__name__)
handler = logging.FileHandler(os.path.join(real_dir_path, '../log/kmeans/kmeans.log'), 'w')
formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

#定义全局变量
data_dir = os.path.join(real_dir_path, '..', 'graphlab_lda', 'data')
real_dir_path = os.path.split(os.path.realpath(__file__))[0]
kmeans_model_save_dir = real_dir_path + '/' + 'models/'
if not os.path.exists(kmeans_model_save_dir):
    os.mkdir(kmeans_model_save_dir)
g_channel_kmeans_model_dict = {}
chnl_k_dict = {'体育':12}


def create_model_proc(chname, model_save_dir=None):
    if chname not in chnl_k_dict.keys():
        return
    global g_channle_kmeans_model_dict, data_dir
    logger.info('create kmeans model for {}'.format(chname))
    docs = gl.SFrame.read_csv(os.path.join(data_dir, chname), header=False)
    docs = gl.text_analytics.count_words(docs['X1'])
    model = gl.kmeans.create(gl.SFrame(docs), num_clusters=chnl_k_dict[chname])
    g_channel_kmeans_model_dict[chname] = model
    #save_model_to_db(model, chname)
    #save model to file
    if model_save_dir:
        model.save(model_save_dir+'/'+chname)

def create_kmeans_models():
    global kmeans_model_save_dir, g_channle_kmeans_model_dict
    model_create_time = datetime.datetime.now()
    time_str = model_create_time.strftime('%Y-%m-%d-%H-%M-%S')
    model_path = kmeans_model_save_dir + time_str
    if not os.path.exists(model_path):
        os.mkdir(model_path)
        logger.info('create kmeans models {}'.format(time_str))
    for chanl in chnl_k_dict.keys():
        create_model_proc(chanl, model_path)
    print 'create models finished!!'


def get_newest_model_dir():
    global kmeans_model_save_dir, kmeans_model_save_dir
    models = os.listdir(kmeans_model_save_dir)
    ms = {}
    for m in models:
        ms[m] = m.replace('-', '')
    ms_sort = sorted(ms.items(), key=lambda x:x[1])
    return kmeans_model_save_dir + ms_sort.pop()[0]


def load_models(models_dir):
    print 'load_models()'
    global g_channel_kmeans_model_dict, model_v
    import os
    model_v = os.path.split(models_dir)[1]
    if len(g_channel_kmeans_model_dict) != 0:
        g_channel_kmeans_model_dict.clear()
    models_files = os.listdir(models_dir)
    for mf in models_files:
        print '    load ' + mf
        print models_dir
        g_channel_kmeans_model_dict[mf] = gl.load_model(models_dir + '/'+ mf)


model_v = os.path.split(get_newest_model_dir())[1]
def load_newest_models():
    load_models(get_newest_model_dir())

###############################################################################
#@brief  :预测新数据
#@input  :
###############################################################################
nid_sql = 'select a.title, a.content, c.cname \
from (select * from newslist_v2 where nid=%s) a \
inner join channellist_v2 c on a."chid"=c."id"'
from graphlab_lda.topic_model_doc_process import channel_for_topic
def kmeans_predict(nid_list):
    global g_channel_kmeans_model_dict
    if len(g_channel_kmeans_model_dict) == 0:
        load_newest_models()
    nid_info = {}
    for nid in nid_list:
        conn, cursor = doc_process.get_postgredb()
        cursor.execute(nid_sql, [nid])
        row = cursor.fetchone()
        title = row[0]
        content_list = row[1]
        chanl_name = row[2]

        if chanl_name not in g_channel_kmeans_model_dict:
            continue

        txt = ''
        for content in content_list:
            if 'txt' in content.keys():
                txt += content['txt'].encode('utf-8')
        total_txt = title + txt
        word_list = doc_process.filter_html_stopwords_pos(total_txt, remove_num=True, remove_single_word=True)
        nid_info[nid] = [chanl_name, ' '.join(word_list)]
        cursor.close()
        conn.close()

    for chname in g_channel_kmeans_model_dict.keys():
        nids = []
        doc_list = []
        for nid in nid_info.keys():
            if nid_info[nid][0] == chname:
                nids.append(nid)
                doc_list.append(nid_info[nid][1])

        ws = gl.SArray(doc_list)
        docs = gl.SFrame(data={'X1': ws})
        docs = gl.text_analytics.count_words(docs['X1'])
        docs = gl.SFrame(docs)
        pred = g_channel_kmeans_model_dict[chname].predict(docs, output_type = 'cluster_id')
        if len(nids) != len(pred):
            print 'len(nids) != len(pred)'
            return
        clstid_nid_dict = {}
        for i in xrange(0, len(pred)):
            if pred[i] not in clstid_nid_dict.keys():
                clstid_nid_dict[i] = []
                clstid_nid_dict[i].append(nids[i])
            else:
                clstid_nid_dict[i].append(nids[i])
        print clstid_nid_dict










