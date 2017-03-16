# -*- coding: utf-8 -*-
# @Time    : 17/3/16 下午5:22
# @Author  : liulei
# @Brief   : 创建model。 进程9987
# @File    : model_create.py
# @Software: PyCharm Community Edition
################################################################################
# 创建模型流程
#           1. 获取新闻
#           2. 训练模型
#           3. 保存模型
################################################################################

import os
import datetime
from util.logger import Logger

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger_9987 = Logger('process9987',  os.path.join(real_dir_path,  'log/log_9987.txt'))

model_dir = os.path.join('/root/ossfs', 'topic_models')  #模型保存路径
model_version = ''  #模型版本


class DocProcess(object):
    '''collect docs for training model'''



class TopicModel(object):
    '''topic model class for train/load model'''
    def __init__(self,):
        self.create_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')








