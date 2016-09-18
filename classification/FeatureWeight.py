# -*- coding: utf-8 -*-
# @Time    : 16/8/10 上午11:41
# @Author  : liulei
# @Brief   : 采用TF-IDF方法计算特征的权重
# @File    : FeatureWeight.py
# @Software: PyCharm Community Edition

import math
import os
from time import time
from DocPreProcess import DOC_NUM
from DocPreProcess import TRAIN_DOC
from DocPreProcess import category_list
from DocPreProcess import logger
from DocPreProcess import idf_file
from FeatureSelection import svm_feature_file
from FeatureSelection import textCutBasePath

TestDocCount = DOC_NUM - TRAIN_DOC #作為test的文档数目
train_svm_file = './result/train.svm'
test_svm_file = './result/test.svm'
idf_file = './result/idf.txt'

#feature_dict是为了后面的快速查询, feature_list是为了提供给训练的接口
def readFeature(featureName):
    featureFile = open(featureName, 'r')
    featureContent = featureFile.read().split('\n')
    featureFile.close()
    feature_list = list()
    feature_dict = dict()
    for eachfeature in featureContent:
        eachfeature = eachfeature.split(" ")
        if (len(eachfeature)==2):
            feature_list.append(eachfeature[1])
            feature_dict[eachfeature[1]] = eachfeature[0]
    return feature_list, feature_dict


# 读取所有类别的训练样本到字典中,每个文档是一个list
def readFileToList(textCutBasePath, classCode, documentCount):
    dic = dict()
    for eachclass in classCode:
        currClassPath = textCutBasePath + eachclass + "/"
        n = len(os.listdir(currClassPath))
        eachclasslist = list()
        for i in range(n):
            eachfile = open(currClassPath+str(i)+".cut")
            eachfilecontent = eachfile.read()
            eachfilewords = eachfilecontent.split(" ")
            eachclasslist.append(eachfilewords)
        dic[eachclass] = eachclasslist
    return dic

# 读取所有类别的训练样本到字典中,每个文档是一个list
def readTestFileToList(textCutBasePath, classCode, documentCount, testDocCount):
    dic = dict()
    for eachclass in classCode:
        currClassPath = textCutBasePath + eachclass + "/"
        eachclasslist = list()
        for i in range(documentCount, documentCount+testDocCount):
            eachfile = open(currClassPath+str(i)+".cut")
            eachfilecontent = eachfile.read()
            eachfilewords = eachfilecontent.split(" ")
            eachclasslist.append(eachfilewords)
        dic[eachclass] = eachclasslist
    return dic

# 计算特征的逆文档频率
def featureIDF(dic, feature, dffilename):
    dffile = open(dffilename, "w")
    dffile.close()
    dffile = open(dffilename, "a")
    totaldoccount = 0
    idffeature = dict()
    dffeature = dict()
    for eachfeature in feature:
        docfeature = 0
        for key in dic:
            totaldoccount = totaldoccount + len(dic[key])
            classfiles = dic[key]
            for eachfile in classfiles:
                if eachfeature in eachfile:
                    docfeature = docfeature + 1
        # 计算特征的逆文档频率
        featurevalue = math.log(float(totaldoccount)/(docfeature+1))
        dffeature[eachfeature] = docfeature
        # 写入文件，特征的文档频率
        dffile.write(eachfeature + " " + str(docfeature)+"\n")
        # print(eachfeature+" "+str(docfeature))
        idffeature[eachfeature] = featurevalue
    dffile.close()
    return idffeature

# 计算Feature's TF-IDF 值
def TFIDFCal(feature, dic, idffeature, filename):
    file = open(filename, 'w')
    file.close()
    file = open(filename, 'a')
    for key in dic:
        classFiles = dic[key]
        # 谨记字典的键是无序的
        classid = category_list.index(key)
        for eachfile in classFiles:
            # 对每个文件进行特征向量转化
            file.write(str(classid)+" ")
            for i in range(len(feature)):
                if feature[i] in eachfile:
                    currentfeature = feature[i]
                    featurecount = eachfile.count(feature[i])
                    tf = float(featurecount)/(len(eachfile))
                    # 计算逆文档频率
                    featurevalue = idffeature[currentfeature]*tf
                    file.write(str(i+1)+":"+str(featurevalue) + " ")
            file.write("\n")

def writeIdfToFile(idffeature_dict):
    file0 = open(idf_file, 'w')
    for key in idffeature_dict.keys():
        file0.write(key + ' ' + str(idffeature_dict[key]) + '\n')
    file0.close()

feature_list, feature_dict = readFeature(svm_feature_file)#读取所有的特征词

def featureWeight():
    global feature_list
    logger.info('featureWeight being...')
    dic = readFileToList(textCutBasePath, category_list, TRAIN_DOC)
    idffeature = featureIDF(dic, feature_list, "dffeature.txt")
    writeIdfToFile(idffeature)
    #train 数据
    TFIDFCal(feature_list, dic, idffeature, train_svm_file)
    logger.info('featureWeight done!')
    #test数据
    test_dic = readTestFileToList(textCutBasePath, category_list, TRAIN_DOC, TestDocCount)
    TFIDFCal(feature_list, test_dic, idffeature, test_svm_file)





