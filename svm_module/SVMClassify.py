# -*- coding: utf-8 -*-
# @Time    : 16/8/10 下午2:33
# @Author  : liulei
# @Brief   : 
# @File    : SVMClassify.py
# @Software: PyCharm Community Edition
import os
import datetime, calendar
import traceback

import re
import string

import sys
from sklearn.datasets import load_svmlight_file
#from sklearn.grid_search import GridSearchCV
from sklearn.model_selection import GridSearchCV
from sklearn.svm import SVC
from classification.DocPreProcess import strProcess
from classification.FeatureWeight import feature_list, feature_dict
from classification.DocPreProcess import category_list
from classification.DocPreProcess import category_name_id_dict
from classification.DocPreProcess import logger
from classification.FeatureWeight import train_svm_file
from classification.FeatureWeight import idf_file
import tornado
import tornado.gen
param_grid = {'C': [10, 100, 1000], 'gamma': [0.001, 0.0001]}
clf = GridSearchCV(SVC(kernel='rbf'), param_grid)
svm_file = './result/svm_predict.txt'

n_feature = 0
def getModel():
    global clf
    global n_feature
    logger.info('Begin to train...')
    X_train, y_train = load_svmlight_file(train_svm_file)
    n_feature = X_train.shape[1]
    start_time = datetime.datetime.now()
    clf.fit(X_train, y_train)
    end_time = datetime.datetime.now()
    logger.info('train done in {0}s'.format((end_time - start_time).total_seconds()))
    #X_test, y_test = load_svmlight_file('test.svm', n_features=X_train.shape[1])#没有指定n_features时出现X_test.shape[1] != X_train.shape[1]
    #pred = clf.predict(X_test)
    #from sklearn.metrics import accuracy_score
    #print accuracy_score(y_test, pred)

def getIdfOfTrain():
    idf = {}
    if not os.path.exists(idf_file):
        logger.error('no idf.txt')
        return idf
    else:
        f = open(idf_file, 'r')
        lines = f.readlines()
        for line in lines:
            line2 = re.split(' ', line)
            idf[line2[0]] = string.atof(line2[1])
        f.close()
    return idf

def writeSvmFile(text, file_path, idf_val):
    try:
        svm_file = open(file_path, 'a')
        words = strProcess(text)
        svm_file.write(str(-1) + ' ')
        checked = []
        data = dict()
        for w in words:
            w_utf = w.encode('utf-8')
            if feature_dict.get(w_utf) and w_utf not in checked:
                checked.append(w_utf)
                feature_count = words.count(w)
                currTf = float(feature_count)/len(words)
                val = idf_val[w_utf]*currTf
                data[int(feature_dict[w_utf])] = str(val)
        data_list = sorted(data.iteritems(), key=lambda d:d[0])
        for item in data_list:
            svm_file.write(str(item[0]) + ':' + item[1] + ' ')
        svm_file.write('\n')
        svm_file.close()
    except IOError as e:
        logger.error('I/O error({0}):{1}'.format(e.errno, e.strerror))
    except Exception, e:
        logger.error(traceback.format_exc())
        logger.error('Unexpected error when writeSvmFile:{0}'.format(sys.exc_info()[0]))

#预测单个文本
#@tornado.gen.coroutine
def svmPredictOneText(text):
    if text =='' or text.isspace():
        return {'res': False, 'category': ''}
    predict_file = open(svm_file, 'w')
    predict_file.close()
    idf_val = getIdfOfTrain()
    writeSvmFile(text, svm_file, idf_val)
    X_pre, y_pre = load_svmlight_file(svm_file, n_features=n_feature)
    pred = clf.predict(X_pre)
    if pred and int(pred[0]):
        return {'bSuccess': True, 'category': category_list[int(pred[0])]}
    else:
        return {'bSuccess': False, 'category': ''}

#预测多个文本
def svmPredictTexts(texts):
    predict_file = open(svm_file, 'w')
    predict_file.close()
    idf_val = getIdfOfTrain()
    for text in texts:
        writeSvmFile(text, svm_file, idf_val)
    logger.info('write svm file to predict done!')
    X_pre, y_pre = load_svmlight_file(svm_file, n_features=n_feature)
    pred = clf.predict(X_pre)
    return pred

sql = 'SELECT a.nid, a.title, a.content, b.cname  \
FROM \
(SELECT * from newslist_v2 where srid={0}) a \
INNER JOIN channellist_v2 b \
on \
a.chid = b.id '
#根据srcid从数据库中去数据进行预测
#@tornado.gen.coroutine
def svmPredictNews(nids, texts, _id = 0, category='all'):
    start_time = datetime.datetime.now()
    logger.info('svmPredictOnSrcid begin...')
    if not category:
        category = 'all'
    if category and category != 'all' and (category not in category_list):
        logger.error('unkown category:{0}'.format(category))
        return {'bSuccess':False, 'message':'{0} is not a known category.'.format(category)}
    pred = svmPredictTexts(texts)
    today = str(datetime.datetime.now())[0:10]
    time = str(datetime.datetime.now())[11:19]
    result_file_name = './result/' + today + '.' + time + '.' + str(_id)
    logger.info('result file is ' + result_file_name)
    result_file = open(result_file_name, 'w')
    cates_dict = {}
    for i in range(len(category_list)):
        cates_dict[category_list[i]] = []
    for i in range(len(texts)):
        result_file.write(category_list[int(pred[i])] + '-----' + texts[i] + '\n')
        cates_dict[category_list[int(pred[i])]].append(nids[i])
    result_file.close()
    end_time = datetime.datetime.now()
    t = (end_time - start_time).total_seconds()
    logger.info('---predict news of srcid:{0} done, in {1}s!--------'.format(str(_id), t))
    if category == 'all':
        return {'bSuccess': True, 'nids': cates_dict}
    else:
        return {'bSuccess': True, 'nids': cates_dict[category]}


#返回格式不同
def svmPredictNews2(nids, texts):
    logger.info('svmPredictOnSrcid begin...')
    pred = svmPredictTexts(texts)
    nid_cate_list = []
    for i in range(len(texts)):
        nid_cate_list.append({"nid": nids[i], 'chid': category_name_id_dict[category_list[int(pred[i])]]})
    logger.info('---predict news done!--------')
    return {'bSuccess': True, 'result': nid_cate_list}

