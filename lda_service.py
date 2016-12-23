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
import traceback

from graphlab_lda import topic_model_doc_process
from graphlab_lda import topic_model


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

#预测单次点击事件
class PredictOneClick(tornado.web.RequestHandler):
    def get(self):
        try:
            uid = int(self.get_argument('uid'))
            nid = int(self.get_argument('nid'))
            ctime = self.get_argument('ctime')
            topic_model.predict_user_topic_core(uid, nid, ctime)
        except :
            traceback.print_exc()



#对外提供的接口。对用户点击行为进行预测. 数据写入队列
class PredictUserTopic(tornado.web.RequestHandler):
    def post(self):
        clicks = json.loads(self.get_body_argument('clicks'))
        #uid = int(self.get_argument('uid'))
        #nid = int(self.get_argument('nid'))
        #ctime = self.get_argument('ctime')
        from graphlab_lda import redis_lda
        for click in clicks:
            redis_lda.produce_user_click(click[0], click[1], click[2])


class PredictOnNidAndSave(tornado.web.RequestHandler):
    def get(self):
        try:
            nid = int(self.get_argument('nid'))
            topic_model.lda_predict_and_save(nid)
        except:
            traceback.print_exc()


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


class ProduceNewsApplication(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/topic_model/predict_news_topic", PredictNewsTopic), #放入新闻队列
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)


class ProduceClickEventApplication(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/topic_model/predict_clicks", PredictUserTopic),#可以将批量点击放入队列
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)



class ConsumeClickEventApplication(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/topic_model/predict_one_click", PredictOneClick), #处理单次点击
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
            #("/topic_model/get_topic_on_nid", PredictOnNidAndSave), #消费新闻
            ("/topic_model/load_models", LoadModels),
            ("/topic_model/produce_news_topic_manual", ProuceNewsTopicManual),
            ("/topic_model/get_user_topic", CollectUserTopic),
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)

if __name__ == '__main__':
    port = int(sys.argv[1])
    if port == 9987:  #新闻入库后将nid加入到队列中,对外提供的接口
        http_server = tornado.httpserver.HTTPServer(ProduceNewsApplication())
        http_server.listen(port)
    elif port == 9989 or port == 9988: #包含手工的一些接口和新闻的消费逻辑
        http_server = tornado.httpserver.HTTPServer(Application())
        http_server.listen(port)
    elif port == 9990:#消费新闻队列数据
        from graphlab_lda import redis_lda
        topic_model.load_newest_models()
        redis_lda.consume_nid()
    elif port == 9984: #用户点击事件入队列
        from graphlab_lda.timed_task import get_clicks_5s
        ioloop.PeriodicCallback(get_clicks_5s, 300000).start() #定时从点击表中取
        http_server = tornado.httpserver.HTTPServer(ProduceClickEventApplication())
        http_server.listen(port) #同时提供手工处理端口
    elif port == 9985: #消费用户点击逻辑进程。
        from graphlab_lda import redis_lda
        topic_model.load_newest_models()
        redis_lda.consume_user_click()
    tornado.ioloop.IOLoop.instance().start()





