# -*- coding: utf-8 -*-
# @Time    : 16/8/2 下午5:27
# @Author  : liulei
# @Brief   : 
# @File    : startService.py
# @Software: PyCharm Community Edition

import json
import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.httpserver
import tornado.netutil
import tornado.gen
import sys
import operator
import classification.match_name as match_name
from classification import DocPreProcess
from classification import FeatureSelection
from classification import FeatureWeight
from svm_module import SVMClassify

class FetchContent(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        type = self.get_argument('type', None)
        txt = self.get_argument('txt', None)
        name_instance = match_name.NameFactory(type)
        if name_instance:
            name = yield name_instance.getArticalTypeList(txt)
            if name:
                ret = {'bSuccess': True, 'name': name}
                self.write(json.dumps(ret))
            else:
                ret = {'bSuccess': False, 'msg': 'Can not get any {0} name from the txt'.format(type)}
                self.write(json.dumps(ret))

class NewsClassifyOnNid(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def post(self):
        text = self.get_argument('text', None)
        #text = DocPreProcess.getTextOfNewsNid(nid)
        res = yield SVMClassify.svmPredictOneText(text)
        self.write(json.dumps(res))

class NewsClassifyOnSrcid(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def post(self):
        srcid = self.get_argument('srcid', None)
        category = self.get_argument('category', None)
        #nids = self.get_arguments('nids')
        nids = self.get_body_arguments('nids')
        #texts = self.get_arguments('texts')
        texts = self.get_body_arguments('texts')
        ret = yield SVMClassify.svmPredictNews(srcid, nids, texts, category)
        self.write(json.dumps(ret))

#按照chennel id. 主要用於测试自媒体、点集、奇闻
class NewsClassifyOnChid(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def post(self):
        chid = self.get_argument('chid', None)
        #nids = self.get_arguments('nids')
        nids = self.get_body_arguments('nids')
        #texts = self.get_arguments('texts')
        texts = self.get_body_arguments('texts')
        ret = yield SVMClassify.svmPredictNews(chid, nids, texts)
        self.write(json.dumps(ret))

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/ml/fetchContent", FetchContent),
            (r"/ml/newsClassifyOnNid", NewsClassifyOnNid),
            (r"/ml/newsClassifyOnSrcid", NewsClassifyOnSrcid),
            (r"/ml/newsClassifyOnChid", NewsClassifyOnChid)
        ]
        settings = {}
        tornado.web.Application.__init__(self, handlers, **settings)

if __name__ == "__main__":
    #DocPreProcess.docPreProcess()
    #FeatureSelection.featureSelect()
    #FeatureWeight.featureWeight()
    SVMClassify.getModel()
    #SVMClassify.svmPredictOnSrcid(3874)

    port = sys.argv[1]
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(port)
    tornado.ioloop.IOLoop.instance().start()