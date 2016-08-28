# -*- coding: utf-8 -*-
# @Time    : 16/8/8 下午3:48
# @Author  : liulei
# @Brief   : 使用卡方检验的方法提取特征词
# @File    : FeatureSelection.py.py
# @Software: PyCharm Community Edition

from DocPreProcess import *
FEATURE_NUM = 1000  #每个类别的特征词数目
# 对卡方检验所需的 a b c d 进行计算
# a：在这个分类下包含这个词的文档数量
# b：不在该分类下包含这个词的文档数量
# c：在这个分类下不包含这个词的文档数量
# d：不在该分类下，且不包含这个词的文档数量
def calChi(a, b, c, d):
    if ((a+c) * (a+b) * (b+d)*(c+d)) == 0:
        return 0;
    else:
        return float(pow((a*d - b*c), 2)) / float((a+c) * (a+b) * (b+d) * (c+d))

#分词后的文件的路径
textCutBasePath = './NewsFileCut/'
svm_feature_file = './result/SVMFeature.txt'
if not os.path.exists(svm_feature_file):
    f = open(svm_feature_file, 'w')
    f.close()

#构建每个类别的最初词向量。set内包含所有特征词
#每个类别下的文档集合用list<set>表示,每个set表示一个文档,整体用一个dict表示
def buildItemSets():
    termDic = dict()
    termClassDic = dict()
    for eachclass in category_list:
        currClassPath = textCutBasePath + eachclass + '/'
        eachClassWordSets = set()
        eachClassWordList = list()
        count = len(os.listdir(currClassPath))
        for i in range(count):
            eachDocPath = currClassPath + str(i) + '.cut'
            eachFileObj = open(eachDocPath, 'r')
            eachFileContent = eachFileObj.read()
            eachFileWords = eachFileContent.split(' ')
            eachFileSet = set()
            for eachword in eachFileWords:
                if len(eachword) == 0:
                    continue
                eachFileSet.add(eachword)
                eachClassWordSets.add(eachword)
            eachClassWordList.append(eachFileSet)
        termDic[eachclass] = eachClassWordSets
        termClassDic[eachclass] = eachClassWordList

    return termDic, termClassDic


#计算卡方,选取特征词
#K为每个类别的特征词数目
def featureSelection(termDic, termClassDic, K):
    termCountDic = dict()
    for key in termDic:
        classWordSets = termDic[key]
        classTermCountDic = dict()
        for eachword in classWordSets:
            a = 0
            b = 0
            c = 0
            d = 0
            for eachclass in termClassDic:
                if eachclass == key: #在这个类别下处理
                    for eachdocset in termClassDic[eachclass]:
                        if eachword in eachdocset:
                            a += 1
                        else:
                            c += 1
                else: #类别外处理
                    #print len(termClassDic[eachclass])
                    for eachdocset in termClassDic[eachclass]:
                        if eachword in eachdocset:
                            b += 1
                        else:
                            d += 1
            eachwordcount = calChi(a, b, c, d)
            classTermCountDic[eachword] = eachwordcount
        #排序后取前K个
        #排序后返回的是元组的列表
        sortedClassTermCountDic = sorted(classTermCountDic.items(), key=lambda d:d[1], reverse=True)
        subDic = dict()
        n = min(len(sortedClassTermCountDic), K)
        for i in range(n):
            subDic[sortedClassTermCountDic[i][0]] = sortedClassTermCountDic[i][1]
        termCountDic[key] = subDic
    return termCountDic

def writeFeatureToFile(termCounDic, fileName):
    featureSet = set()
    for key in termCounDic:
        for eachkey in termCounDic[key]:
            featureSet.add(eachkey)
    count = 1
    file = open(fileName, 'w')
    for feature in featureSet:
        #判断feature不为空
        stripfeature = feature.strip(' ')
        if len(stripfeature) > 0 and feature != ' ' and (not feature.isspace()) and (not feature.isdigit()):
            file.write(str(count) + ' ' + feature + '\n')
            count += 1
    file.close()

def featureSelect():
    logger.info('featureSelect begin...')
    termDic, termClassDic = buildItemSets()
    termCountDic = featureSelection(termDic, termClassDic, FEATURE_NUM)
    writeFeatureToFile(termCountDic, svm_feature_file)
    logger.info('featureSelect done!')

#调用buildItemSets,创造train数据
#每个类别取前200个文件; 每个类别取1000个特征词
#termDic, termClassDic = buildItemSets(200)
#termCountDic = featureSelection(termDic, termClassDic, 1000)
#writeFeatureToFile(termCountDic, 'SVMFeature.txt')



