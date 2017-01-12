# -*- coding: utf-8 -*-
# @Time    : 17/1/4 下午3:22
# @Author  : liulei
# @Brief   : 
# @File    : kmeans_service.py.py
# @Software: PyCharm Community Edition

import json
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
        self.write(json.dumps(res))


class PredictClick(tornado.web.RequestHandler):
    def get(self):
        uid = 1
        nid = 10682265
        time_str = '2016-12-20 07:11:53'
        from graphlab_kmeans import kmeans
        kmeans.predict_click((uid, nid, time_str))



class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/kmeans/createmodel", CreateKmeansModel),
            ("/kmeans/predict_nids", PredictKmeans),
            ("/kmeans/predict_click", PredictClick),
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)


if __name__ == '__main__':
    port = int(sys.argv[1])
    if port == 9100:
        http_server = tornado.httpserver.HTTPServer(Application())
        http_server.listen(port) #同时提供手工处理端口
    elif port == 9980:
        pass

    tornado.ioloop.IOLoop.instance().start()
