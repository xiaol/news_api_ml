# -*- coding: utf-8 -*-
# @Time    : 16/8/2 下午5:27
# @Author  : liulei
# @Brief   : 
# @File    : startService.py
# @Software: PyCharm Community Edition
import json
import os

import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.httpserver
import tornado.netutil
import tornado.gen
import sys
from ads_remove import AdsExtract
from ads_remove import redis_ads

#计算所有微信号的广告段落
class NewsAdsExtract(tornado.web.RequestHandler):
    def get(self):
        print 'extract'
        dict_wechat = AdsExtract.get_wechat_news()
        AdsExtract.extract_ads(dict_wechat)

#获取广告再次提取后,广告发生变化的公众号名称
class GetModifiedWechatNames(tornado.web.RequestHandler):
    def get(self):
        page_num = json.loads(self.get_argument('page', None))
        page_size = json.loads(self.get_argument('pageSize', None))
        wechat_names_list = list(AdsExtract.modify_dict)
        print len(wechat_names_list)
        print wechat_names_list
        res = {}
        res['total_num'] = len(wechat_names_list)
        if len(wechat_names_list) >= (page_num - 1) * page_size:
            end_num = min(page_num*page_size-1, len(wechat_names_list) -1)
            res['list'] = wechat_names_list[(page_num - 1)*page_size : end_num + 1]
        self.write(json.dumps(res))

#读取微信号名称列表
class GetWechatNames(tornado.web.RequestHandler):
    def get(self):
        page_num = json.loads(self.get_argument('page', None))
        page_size = json.loads(self.get_argument('pageSize', None))
        wechat_names_list = AdsExtract.get_checked_name()
        res = {}
        res['total_num'] = len(wechat_names_list)
        if len(wechat_names_list) >= (page_num - 1) * page_size:
            end_num = min(page_num*page_size-1, len(wechat_names_list) -1)
            res['list'] = wechat_names_list[(page_num - 1)*page_size : end_num + 1]
        self.write(json.dumps(res))

#读取一个公众号的广告结果
class GetAdsOfOneWechat(tornado.web.RequestHandler):
    def get(self):
        name = self.get_argument('name', None)
        content = {}
        content['name']=name
        content['list'] = AdsExtract.get_ads_of_one_wechat(name)
        self.write(json.dumps(content))

#人工修改数据时的响应
class ModifyNewsAdsResults(tornado.web.RequestHandler):
    def get(self):
        modify_type = self.get_argument('type') #'add'或者'delete'
        modify_data = self.get_argument('para') #段落号:段落内容
        print 'modify ads'
        AdsExtract.modify_ads_results(modify_type, modify_data)

#保存手工干预的结果
class SaveAdsModify(tornado.web.RequestHandler):
    def get(self):
        AdsExtract.save_ads_modify()
        #清空保存了修改微信号的set
        AdsExtract.modify_dict.clear()
#用于广告的检测
class NewsAdsExtractOnnid(tornado.web.RequestHandler):
    def get(self):
        nid = self.get_argument('nid',None)
        pname, content_list = AdsExtract.get_para_on_nid(nid)
        ret = AdsExtract.get_ads_paras(pname, content_list)
        t = ''
        head_ads = ''
        tail_ads = ''
        if len(ret) != 0:
            t += nid.encode('utf-8') + '广告:'
            for item in ret.items():
                if int(item[0].encode('utf-8')) >= 0:
                    head_ads += '    新闻开头广告: ' + item[1].encode('utf-8')
                else:
                    tail_ads += '    新闻结尾广告: ' + item[1].encode('utf-8')
        else:
            t = nid.encode('utf-8') + '没有检测到广告'
        self.render('ads_nid.html', title = str(nid.encode('utf-8')), head = t, head_ads = head_ads, tail_ads = tail_ads)

#去除广告的对外的接口, 放入redis队列
class RemoveAdsOnnid(tornado.web.RequestHandler):
    def get(self):
        nid = self.get_argument('nid', None)
        redis_ads.produce_nid(nid)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/news_process/NewsAdsExtract", NewsAdsExtract),
            (r"/news_process/NewsAdsExtractOnnid", NewsAdsExtractOnnid),
            (r"/news_process/GetAdsOfOneWechat", GetAdsOfOneWechat),
            (r"/news_process/GetWechatNames", GetWechatNames),
            (r"/news_process/ModifyNewsAdsResults", ModifyNewsAdsResults),
            (r"/news_process/SaveAdsModify", SaveAdsModify),
            (r"/news_process/GetModifiedWechatNames", GetModifiedWechatNames),
            (r"/news_process/RemoveAdsOnnid", RemoveAdsOnnid),
        ]
        settings = {
            "template_path": "wechat-show",
            "static_path": os.path.join(os.path.dirname(__file__), "wechat-show"),
        }

        tornado.web.Application.__init__(self, handlers, **settings)

if __name__ == "__main__":
    port = sys.argv[1]
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(port)

    #9996,9997用于向队列中生产; 9998用于消费;9999用于人工干预
    #检测队列
    if port == 9998:
        AdsExtract.read_data()
        redis_ads.consume_process()
    tornado.ioloop.IOLoop.instance().start()

