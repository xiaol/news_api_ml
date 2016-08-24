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
import sys
import operator
import classification.match_name as match_name
from classification import DocPreProcess
from classification import FeatureSelection
from classification import FeatureWeight
from svm_module import SVMClassify

class FetchContent(tornado.web.RequestHandler):
    def get(self):
        type = self.get_argument('type', None)
        txt = self.get_argument('txt', None)
        name_instance = match_name.NameFactory(type)
        if name_instance:
            name_dict = name_instance.getArticalTypeList(txt)
            name_list = list(name_dict)
            sorted_names = sorted(name_list, key=operator.itemgetter(1), reverse=True)
            if sorted_names:
                ret = {'bSuccess': True, 'name': sorted_names[0]}
                self.write(json.dumps(ret))
            else:
                ret = {'bSuccess': False, 'msg': 'Can not get any {0} name from the txt'.format(type)}
                self.write(json.dumps(ret))

class NewsClassifyOnNid(tornado.web.RequestHandler):
    def get(self):
        text = self.get_argument('text', None)
        #text = DocPreProcess.getTextOfNewsNid(nid)
        res = SVMClassify.svmPredictOneText(text)
        self.write(json.dumps(res))

class NewsClassifyOnSrcid(tornado.web.RequestHandler):
    def get(self):
        srcid = self.get_argument('srcid', None)
        category = self.get_argument('category', None)
        ret = SVMClassify.svmPredictOnSrcid(srcid, category)
        self.write(json.dumps(ret))

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/ml/fetchContent", FetchContent),
            (r"/ml/newsClassifyOnNid", NewsClassifyOnNid),
            (r"/ml/newsClassifyOnSrcid", NewsClassifyOnSrcid)
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