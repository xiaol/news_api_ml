# -*- coding: utf-8 -*-
# @Time    : 16/12/23 下午3:25
# @Author  : liulei
# @Brief   : 
# @File    : produce_consume_nid.py
# @Software: PyCharm Community Edition

import requests
def push_nid_to_queue(nid):
    #lda_model处理
    lda_url = "http://120.55.88.11:9986/topic_model/predict_news_topic"
    data = {}
    data['nid'] = nid
    requests.get(url=lda_url, params=data)

    #去除广告服务
    ads_url = "http://120.55.88.11/news_process/RemoveAdsOnnid"
    ads_data = {}
    ads_data['nid'] = nid
    requests.get(url=ads_url, params=ads_data)


