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

class CreateModels(tornado.web.RequestHandler):
    def get(self):
        topic_model.create_models()


class PredictOnNid(tornado.web.RequestHandler):
    def get(self):
        nid = int(self.get_argument('nid'))
        res = topic_model.lda_predict(nid)
        self.write(json.dumps(res))


class LoadModels(tornado.web.RequestHandler):
    def get(self):
        models_dir = self.get_argument('dir')
        topic_model.load_models(models_dir)


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/topic_model/create_models", CreateModels),
            ("/topic_model/predict_on_nid", PredictOnNid),
            ("/topic_model/load_models", LoadModels),
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)

if __name__ == '__main__':
    port = sys.argv[1]
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(port)
    tornado.ioloop.IOLoop.instance().start()
