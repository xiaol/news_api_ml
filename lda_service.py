# -*- coding: utf-8 -*-
# @Time    : 16/12/1 上午11:10
# @Author  : liulei
# @Brief   : 
# @File    : lda_service.py
# @Software: PyCharm Community Edition
import json
import sys

import tornado
from tornado import web
from tornado import httpserver
from tornado import ioloop

from graphlab_lda import topic_model_doc_process
from graphlab_lda import topic_model

#topic_model_doc_process.coll_news_for_channles()

#topic_model.create_model('体育')
#topic_model.create_models()
#topic_model.lda_predict(6897344)

class CollNews(tornado.web.RequestHandler):
    def get(self):
        num_per_chanl = int(self.get_argument('num'))
        topic_model_doc_process.coll_news_for_channles(num_per_chanl)




class CreateModels(tornado.web.RequestHandler):
    def get(self):
        topic_model.create_models()


class PredictOnNid(tornado.web.RequestHandler):
    def get(self):
        nid = int(self.get_argument('nid'))
        res = topic_model.lda_predict(nid)
        self.write(json.dumps(res))

#对外提供的接口。新闻入库后就执行
class PredictNewsTopic(tornado.web.RequestHandler):
    def get(self):
        nid = int(self.get_argument('nid'))
        from graphlab_lda import redis_lda
        redis_lda.produce_nid(nid)


class PredictOnNidAndSave(tornado.web.RequestHandler):
    def get(self):
        nid = int(self.get_argument('nid'))
        topic_model.lda_predict_and_save(nid)


class LoadModels(tornado.web.RequestHandler):
    def get(self):
        models_dir = self.get_argument('dir')
        topic_model.load_models(models_dir)


#手动添加一些新闻进行预测。用于topic model启动时使用
class ProuceNewsTopicManual(tornado.web.RequestHandler):
    def get(self):
        num = self.get_argument('num')
        topic_model.produce_news_topic_manual(num)


#根据用户的点击预测其感兴趣的话题,入库
class CollectUserTopic(tornado.web.RequestHandler):
    def get(self):
        topic_model.get_user_topics()

class ProduceApplication(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/topic_model/predict_news_topic", PredictNewsTopic),
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)


#用于手工的一些接口
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/topic_model/coll_news", CollNews),
            ("/topic_model/create_models", CreateModels),
            ("/topic_model/predict_on_nid", PredictOnNid),
            ("/topic_model/get_topic_on_nid", PredictOnNidAndSave),
            ("/topic_model/load_models", LoadModels),
            ("/topic_model/produce_news_topic_manual", ProuceNewsTopicManual),
            ("/topic_model/get_user_topic", CollectUserTopic),
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)

if __name__ == '__main__':
    port = int(sys.argv[1])
    if port == 9988:  #新闻入库后将nid加入到队列中,对外提供的接口
        http_server = tornado.httpserver.HTTPServer(ProduceApplication())
        http_server.listen(port)
    elif port == 9989:
        http_server = tornado.httpserver.HTTPServer(Application())
        http_server.listen(port)
    elif port == 9990:
        from graphlab_lda import redis_lda
        redis_lda.consume_nid()  #消费队列数据

    tornado.ioloop.IOLoop.instance().start()
