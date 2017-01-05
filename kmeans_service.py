# -*- coding: utf-8 -*-
# @Time    : 17/1/4 下午3:22
# @Author  : liulei
# @Brief   : 
# @File    : kmeans_service.py.py
# @Software: PyCharm Community Edition

import sys
import tornado
from tornado import web
from tornado import httpserver

class CreateKmeansModel(tornado.web.RequestHandler):
    def get(self):
        from graphlab_kmeans import kmeans
        kmeans.create_kmeans_models()

class PredictKmeans(tornado.web.RequestHandler):
    def get(self):
        nids = self.get_arguments('nid')
        from graphlab_kmeans import kmeans
        res = kmeans.kmeans_predict(nids)
        self.write(res)



class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/kmeans/createmodel", CreateKmeansModel),
            ("/kmeans/predict_nids", PredictKmeans),
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)


if __name__ == '__main__':
    port = int(sys.argv[1])
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(port) #同时提供手工处理端口

    tornado.ioloop.IOLoop.instance().start()
