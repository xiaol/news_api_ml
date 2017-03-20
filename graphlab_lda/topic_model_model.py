# -*- coding: utf-8 -*-
# @Time    : 17/3/16 下午5:22
# @Author  : liulei
# @Brief   : 创建model。 进程9987
# @File    : model_create.py
# @Software: PyCharm Community Edition
################################################################################
# 创建模型流程
#           1. 获取新闻
#           2. 训练模型
#           3. 保存模型
################################################################################

import os
import datetime
import traceback
from util.logger import Logger
from data_process import DocProcess
import graphlab as gl
from data_process import get_news_words
from util.doc_process import get_postgredb

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger_9987 = Logger('process9987',  os.path.join(real_dir_path,  'log/log_9987.txt'))

data_dir = os.path.join(real_dir_path, 'data')
model_base_path = os.path.join('/root/ossfs', 'topic_models')  #模型保存路径
model_version = ''  #模型版本
model_instance = None
insert_sql = "insert into news_topic_v2 (nid, model_v, topic_id, probability, ctime) values(%s, %s, %s, %s, %s)"

class TopicModel(object):
    '''topic model class for train/load model'''
    def __init__(self, data_path=None, model_save_path=None):
        self.data_path = data_path
        self.version = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        if model_save_path:
            self.save_path = os.path.join(model_save_path, self.version)
        self.model = None


    def create(self):
        logger_9987.info('TopicModel::create begin ...')
        docs_sframe = gl.SFrame.read_csv(self.data_path, header=False)
        tfidf_encoder = gl.feature_engineering.TFIDF('X1', min_document_frequency=5/docs_sframe['X1'].size())
        tfidf_encoder = tfidf_encoder.fit(docs_sframe)
        tfidf_dict = tfidf_encoder.transform(docs_sframe)
        #docs = gl.text_analytics.count_words(docs['X1'])
        self.model = gl.topic_model.create(tfidf_dict, num_iterations=10, num_burnin=10, num_topics=100)
        del docs_sframe
        del tfidf_dict
        logger_9987.info('TopicModel::create finished!')


    def create_and_save(self):
        self.create()
        self.model.save(self.save_path)


    def load(self, model_path):
        self.version = os.path.split(model_path)[-1]
        print self.version
        self.model = gl.load_model(model_path)


    def predict(self, nid_list):
        logger_9987.info('predict {}'.format(nid_list))
        t0 = datetime.datetime.now()
        nid_words_dict = get_news_words(nid_list)
        nids = []
        doc_list = []
        for item in nid_words_dict.items():
            nids.append(item[0])
            doc_list.append(item[1])
        ws = gl.SArray(doc_list)
        docs = gl.SFrame(data={'X1':ws})
        tfidf_encoder = gl.feature_engineering.TFIDF('X1')
        tfidf_encoder = tfidf_encoder.fit(docs)
        tfidf_dict = tfidf_encoder.transform(docs)
        pred = self.model.predict(tfidf_dict,
                                  output_type='probability',
                                  num_burnin=30)
        #pred保存的是每个doc在所有主题上的概率值
        props_list = [] #所有文档的主题-概率对儿
        for doc_index in xrange(len(pred)):  #取每个doc的分布
            doc_props = pred[doc_index]
            index_val_dict = {}
            for k in xrange(len(doc_props)):
                index_val_dict[k] = doc_props[k] #{ topic1:0.3, topic2:0.2, ...}
            sort_prop = sorted(index_val_dict.items(), key=lambda d: d[1], reverse=True)
            props = [] #本文档的主题-概率对儿 # [(5, 0.3), (3, 0.2), ...]
            for i in xrange(3):
                if sort_prop[i][1] > 0.1:
                    props.append(sort_prop[i])
            props_list.append(props)   # [ [(5, 0.3), (3, 0.2)..], ....  ]
        #入库
        insert_list = []
        str_time = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        for n in xrange(len(nids)):
            for m in xrange(len(props_list[n])):
                topic_id = props_list[n][m][0]
                prop = props_list[n][m][1]
                insert_list.append((nids[n], self.version, topic_id, prop, str_time))
                sf = self.model.get_topics(num_words=20,
                                           output_type='topic_words')
                print '{} topic words: '
                print '    {}'.format(sf[topic_id]['words'])
        conn, cursor = get_postgredb()
        cursor.executemany(insert_sql, insert_list)
        conn.commit()
        conn.close()
        t1 = datetime.datetime.now()
        logger_9987.info('prediction takes {}'.format((t1 - t0).total_seconds()))


#获取一个文件夹下最新版的文件夹
def get_newest_dir(dir):
    #models_dir = real_dir_path + '/models'
    models = os.listdir(dir)
    ms = {}
    for m in models:
        ms[m] = m.replace('-', '')
    ms_sort = sorted(ms.items(), key=lambda x:x[1])
    #return model_dir + ms_sort.pop()[0]
    return os.path.join(dir,  ms_sort.pop()[0])

def load_topic_model(dir):
    return gl.load_model(dir)

def create_topic_model():
    try:
        global model_instance
        data_path = '/root/workspace/news_api_ml/graphlab_lda/data/2017-03-17-15-02-05/data.txt'
        #data_path = os.path.join(get_newest_dir(data_dir), 'data.txt')
        model_instance = TopicModel(data_path, model_base_path)
        model_instance.create_and_save()

        mod_l = TopicModel()
        mod_l.load(get_newest_dir(model_base_path))
        nids = [5459927, 13274670]
        mod_l.predict(nids)
        print 'create model finished!'
    except:
        print 'exception !!!!'
        logger_9987.exception(traceback.format_exc())

