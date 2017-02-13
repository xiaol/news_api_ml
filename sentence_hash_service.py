# -*- coding: utf-8 -*-
# @Time    : 17/2/13 下午4:02
# @Author  : liulei
# @Brief   : 
# @File    : sentence_hash_service.py
# @Software: PyCharm Community Edition
import sys
from multi_viewpoint import sentence_hash
from util.doc_process import filter_html_stopwords_pos

if __name__ == '__main__':
    #port = sys.argv[1]
    #sentence_hash.cal_sentence_hash_on_nid(11995300)
    sentence_hash.coll_sentence_hash()
    '''
    s1 = "一年来，汪梦云和她的团队走过了澳大利亚、印度、意大利、斐济等26个国家，其旅行视频全网播放超过一亿，单期播放在200-300万。"
    s2 = "2014年，还在浙江电视台做实习主持人的她将旅行视频上传到网络，无心之举却吸引来众多关注。"

    s1 = filter_html_stopwords_pos(s1, remove_num=True, remove_single_word=True)
    s2 = filter_html_stopwords_pos(s2, remove_num=True, remove_single_word=True)
    print type(s1)
    for s in s1:
        print s
    from sim_hash import sim_hash
    h1 = sim_hash.simhash(s1, hashbits=128)
    h2 = sim_hash.simhash(s2, hashbits=128)
    print h1.similarity(h2)
    '''

