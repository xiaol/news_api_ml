# -*- coding: utf-8 -*-
# @Time    : 17/1/4 上午10:10
# @Author  : liulei
# @Brief   : 使用kmeans 算法,为频道内推荐提供比主题模型更粗颗粒度的数据
# @File    : kmeans.py
# @Software: PyCharm Community Edition

import os
import datetime
import json
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


def create_model_proc(chname, model_save_dir=None):
    global g_channle_kmeans_model_dict, data_dir
    logger.info('create kmeans model for {}'.format(chname))
    docs = gl.SFrame.read_csv(os.path.join(data_dir, chname), header=False)
    docs = gl.text_analytics.count_words(docs['X1'])
    #docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
    model = gl.kmeans.create(docs, num_clusters=10)
    g_channel_kmeans_model_dict[chname] = model
    #save_model_to_db(model, chname)
    #save model
    #if model_save_dir:
    #    model.save(model_save_dir+'/'+chname)

def create_kmeans_models():
    global kmeans_model_save_dir, g_channle_kmeans_model_dict
    model_create_time = datetime.datetime.now()
    time_str = model_create_time.strftime('%Y-%m-%d-%H-%M-%S')
    model_path = kmeans_model_save_dir + time_str
    if not os.path.exists(model_path):
        os.mkdir(model_path)
        logger.info('create kmeans models {}'.format(time_str))
    #from topic_model_doc_process import channel_for_topic
    channel_for_topic = ['体育']
    for chanl in channel_for_topic:
        create_model_proc(chanl, model_path)
    print 'create models finished!!'

