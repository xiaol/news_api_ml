# -*- coding: utf-8 -*-
# @Time    : 16/12/23 下午3:23
# @Author  : liulei
# @Brief   : 在各个具体服务之上的服务
# @File    : base_service.py
# @Software: PyCharm Community Edition
import sys
import tornado
import tornado.ioloop
import tornado.web
from base_service import produce_consume_nid

class PushNewsIntoQueue(tornado.web.RequestHandler):
    def get(self):
        nid = self.get_argument('nid')
        produce_consume_nid.push_nid_to_queue(nid)


class StayAliveApp(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/news_queue/produce_nid", PushNewsIntoQueue),
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)


if __name__ == '__main__':
    port = int(sys.argv[1])
    http_server = tornado.httpserver.HTTPServer(StayAliveApp())
    http_server.listen(port)
