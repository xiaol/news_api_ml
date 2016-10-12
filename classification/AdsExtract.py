# -*- coding: utf-8 -*-
# @Time    : 16/10/9 下午2:41
# @Author  : liulei
# @Brief   : 
# @File    : AdsExtract.py.py
# @Software: PyCharm Community Edition
import os

import jieba
#定义两个句子内容相同
def is_sentenses_same(s1, s2):
    if len(s1) > 1.5 * len(s2) or len(s2) > 1.5* len(s1):
        return False
    s1_words = jieba.cut(s1)
    s2_words = jieba.cut(s2)
    set1 = set()
    set2 = set()
    for w in s1_words:
        set1.add(w)
    for w in s2_words:
        set2.add(w)
    l1 = len(set1)
    l2 = len(set2)
    n = len(set1 & set2)
    if float(n) > float(l1 + l2)/2 * 0.8:
        return True
    else:
        return False

#求两篇新闻相同的段落
def get_same_paras(paras1, paras2):
    same_para_info = {}
    N1 = len(paras1)
    N2 = len(paras2)
    n1 = 0
    same_num = 0
    for p1 in paras1:
        n2 = 0
        bP1Finished = False
        p1_len = len(p1)
        if p1_len == 0:
            continue
        for p2 in paras2:
            if len(p2)==0 or (p1_len > 1.5 * len(p2) or len(p2) > 1.5* p1_len) or ((p1[0] not in p2) and (p2[0] not in p1)):
                n2 += 1
                continue
            if is_sentenses_same(p1, p2):
                if n2 < 5 and n1 < 5:
                    if n2 == n1:
                        same_para_info[str(n1)] = p1
                    else:
                        same_para_info[str(n1)] = p1
                        same_para_info[str(n2)] = p2
                elif n1 - N1 == n2 - N2: #位于文章结尾
                    same_para_info[str(n1 - N1)] = p1
                else:
                    same_para_info[str(n1 - N1)] = p1
                    same_para_info[str(n2 - N2)] = p2
                same_num += 1
                bP1Finished = True
            n2 += 1
            if bP1Finished:  #如果P1已经与p2匹配,则不再比较p1与p2后面的句子
                break
        n1 += 1
    #重复度<90%才认为是不同的文章,否则认定为内容相同
    #if float(same_num) < float(N1 + N2)/2 * 0.9:
    if (float(same_num) < float(N1) * 0.9) and (float(same_num) < float(N2) * 0.9):
        return False, same_para_info    #不是同一篇文章,返回相同的段落
    else:
        return True, dict()   #相同文章

#求两篇新闻相同的段落
def get_same_paras2(paras1, paras2):
    same_para_info = {}
    N1 = len(paras1)
    N2 = len(paras2)
    n1 = 0
    same_num = 0
    #判断开头是否一样
    for i in xrange(min(N1, N2)):
        if is_sentenses_same(paras1[i], paras2[i]):
            same_para_info[str(i)] = paras1[i]
            same_num += 1
        else:
            break
    #判断结尾是否一直
    for i in xrange(-1, 0-min(N1, N2), -1):
        if is_sentenses_same(paras1[i], paras2[i]):
            same_para_info[str(i)] = paras1[i]
            same_num += 1
        else:
            break
    #重复度<90%才认为是不同的文章,否则认定为内容相同
    #if float(same_num) < float(N1 + N2)/2 * 0.9:
    if (float(same_num) < float(N1) * 0.9) and (float(same_num) < float(N2) * 0.9):
        return False, same_para_info    #不是同一篇文章,返回相同的段落
    else:
        return True, dict()   #相同文章

#结果汇总
from multiprocessing import Process, Lock, Manager, Pool
mylock = Lock()
out_dict = Manager().dict()
def coll_result(key, val):
    global out_dict
    mylock.acquire()
    out_dict[key] = val
    mylock.release()


#根据公众号的文章提取公众号的广告, 子进程
#news_dict 格式: {'果壳':[段落1, 段落2,...],
#                      [段落1, 段落2,...]...,
#                 '美国咖': ...... }
#output: {'果壳':[(0, 段落0的内容), (-1, 段落-1的内容)...],
#         '美国咖'...... }
def extract_ads_proc(name, news):
    print 'pid = ', str(os.getpid())
    num = len(news)
    i = 0
    ads_dict = {}
    while i < len(news):
        k = i + 1
        while k < len(news):
            bSameNews, same_dict = get_same_paras2(news[i], news[k])
            if bSameNews:
                del news[k]
                continue
            for item in same_dict.items():
                el = item[0] + '\t|' + item[1]
                if el not in ads_dict:
                    ads_dict[el] = 1
                else:
                    ads_dict[el] += 1
            k += 1
        i += 1
    sorted_dict = sorted(ads_dict.items(), key=lambda d:d[1], reverse=True)

    tmp_list = []
    for i in sorted_dict:
        if float(i[1]) > float(num/3):
            para = i[0].split('\t|')
            tmp_list.append( (int(para[0]), para[1]) )
    sorted_list = sorted(tmp_list, key=lambda d:d[0])
    ads_list = []
    for para in sorted_list:
        para_num = int(para[0])
        para_text = para[1]
        ads_list.append( (para_num, para_text))

    coll_result(name, ads_list)

real_path = os.path.split(os.path.realpath(__file__))[0] #文件所在路径
ads_data_file = real_path + '/../result/ads_data.txt'
def extract_ads(news_dict):
    global out_dict
    pool = Pool(30)
    for item in news_dict.items():
        pool.apply_async(extract_ads_proc, (item[0], item[1]))
    pool.close()
    pool.join()
    ret = {}
    for item in out_dict.items():
        ret[item[0]] = item[1]
    import json
    f = open(ads_data_file, 'w')
    f.write(json.dumps(ret))
    f.close()
    return



