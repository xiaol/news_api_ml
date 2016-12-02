# -*- coding: utf-8 -*-
# @Time    : 16/12/1 上午11:10
# @Author  : liulei
# @Brief   : 
# @File    : lda_service.py
# @Software: PyCharm Community Edition
from graphlab_lda import topic_model_doc_process
from graphlab_lda import topic_model

#topic_model_doc_process.coll_news_for_channles()

topic_model.create_model('graphlab_lda/data/体育')
topic_model.lda_predict(6897344)


