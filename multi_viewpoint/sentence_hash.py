# -*- coding: utf-8 -*-
# @Time    : 17/2/13 上午10:09
# @Author  : liulei
# @Brief   : 
# @File    : sentence_hash.py
# @Software: PyCharm Community Edition
#from util  import doc_process
from util.doc_process import get_postgredb
from util.doc_process import Cut
from util.doc_process import filter_html_stopwords_pos
from util.doc_process import get_sentences_on_nid

from sim_hash import sim_hash
import datetime
import os
import logging
import traceback
from multiprocessing import Process
from multiprocessing import Pool

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(real_dir_path + '/../log/multi_vp/log.txt')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
def get_nid_sentence(nid):
    get_sentences_on_nid(nid)


#从64位取字段,建立索引
def get_4_segments(hash_bits):
    #fir = h.__long__() & 0b11111100000000000000000000000000000000000000000000000011111111
    #sec = h.__long__() & 0b00000011111111000000000000000000000000000000001111111100000000
    #thi = h.__long__() & 0b00000000000000111111110000000000000000111111110000000000000000
    #fou = h.__long__() & 0b00000000000000000000001111111111111111000000000000000000000000
    fir = hash_bits & 0b11000000000000000000000000111111110000000000000000000000001111
    sec = hash_bits & 0b00111100000000000000001111000000001111000000000000000011110000
    thi = hash_bits & 0b00000011110000000011110000000000000000111100000000111100000000
    fou = hash_bits & 0b00000000001111111100000000000000000000000011111111000000000000
    return fir, sec, thi, fou



#insert_sentence_hash = "insert into news_sentence_hash (nid, sentence, hash_val, ctime) VALUES({0}, '{1}', '{2}', '{3}')"
insert_sentence_hash = "insert into news_sentence_hash (nid, sentence, sentence_id, hash_val, first_16, second_16, third_16, fourth_16, ctime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
#query_sen_sql = "select nid, sentence, hash_val from news_sentence_hash"
query_sen_sql = "select nid, sentence, hash_val from news_sentence_hash where first_16=%s or second_16=%s or third_16=%s or fourth_16=%s"
#insert_same_sentence = "insert into news_same_sentence_map (nid1, nid2, sentence1, sentence2, ctime) VALUES ('{0}', '{1}', '{2}', '{3}', '{4}')"
insert_same_sentence = "insert into news_same_sentence_map (nid1, nid2, sentence1, sentence2, ctime) VALUES (%s, %s, %s, %s, %s)"
def cal_sentence_hash_on_nid(nid, same_t=3):
    try:
        sentences_list = get_sentences_on_nid(nid)
        same_news = []
        n = 0
        conn, cursor = get_postgredb()
        for s in (su.encode('utf-8') for su in sentences_list):  #计算每一段话的hash值
            s_list = filter_html_stopwords_pos(s)
            n += 1
            if len(s) < 30: #10个汉字
                continue
            h = sim_hash.simhash(s_list)
            #s_fir, s_sec, s_thi, s_fou = get_4_segments(h.__long__())
            fir, sec, thi, fou = get_4_segments(h.__long__())
            t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(query_sen_sql, (str(fir), str(sec), str(thi), str(fou)))
            rows = cursor.fetchall()  #所有可能相同的段落
            #检查是否有相同的段落
            for r in rows:
                if r[0] in same_news:
                    continue
                l1 = float(len(s))
                l2 = float(len(r[1]))
                if l1 > 1.5 * l2 or l2 > 1.5 * l1:
                    continue
                if h.hamming_distance_with_val(long(r[2])) <= same_t:
                    nid1 = r[0]
                    #先检查两篇新闻是否是相同的, 若相同则忽略。 同样利用simhash计算
                    if sim_hash.is_news_same(nid, nid1):
                        same_news.append(nid1)
                        continue
                    cursor.execute(insert_same_sentence, (nid, nid1, s, r[1], t))
            #插入库
            cursor.execute(insert_sentence_hash, (nid, s, n, h.__str__(), fir, sec, thi, fou, t))
            conn.commit()
        cursor.close()
        conn.close()
    except:
        cursor.close()
        conn.close()
        logger.exception(traceback.format_exc())


s_nid_sql = "select distinct nid from news_sentence_hash "
def get_exist_nids():
    conn, cursor = get_postgredb()
    cursor.execute(s_nid_sql)
    rows = cursor.fetchall()
    nid_set = set()
    for r in rows:
        nid_set.add(r[0])
    conn.close()
    return nid_set

################################################################################
#获取新闻句子,句子以分词list形式给出,方便后面直接算hash
#@input ---- nid_set
#@output --- nid_sentenct_dict  格式:
#    {'11111': [['i', 'love', 'you'], ["好", "就", "这样"], ...],
#     '22222': [['举例', '说明'], ['今天', '天气', '不错']...]
#      ...
#    }
################################################################################
get_sent_sql = "select nid, title, content from newslist_v2 where nid in %s"
def get_nids_sentences(nid_set):
    nid_tuple = tuple(nid_set)
    conn, cursor = get_postgredb()
    cursor.execute(get_sent_sql, (nid_tuple, ))
    rows = cursor.fetchall()
    nid_sentences_dict = {}
    for r in rows:
        nid = r[0]
        nid_sentences_dict[nid] = []
        content_list = r[2]
        for content in content_list:
                if "txt" in content.keys():
                    sents = Cut(content['txt'])
                    for i in sents:
                        if len(i) > 20:  #20个汉字, i 是unicode, len代表汉字个数
                            nid_sentences_dict[nid].append(i)
                        #wl = filter_html_stopwords_pos(i)
                        #if len(wl) > 5:   #文本词数量<5, 不计算hash
                        #    nid_sentences_dict[nid].append(wl)
    conn.close()
    return nid_sentences_dict


################################################################################
#@brief: 计算子进程
################################################################################
def cal_process(nid_set, same_t=3):
    logger.info('there are {} news to calulate'.format(len(nid_set)))
    nid_sents_dict = get_nids_sentences(nid_set)
    try:
        i = 0
        t0 = datetime.datetime.now()
        conn, cursor = get_postgredb()
        for item in nid_sents_dict.items(): #每条新闻
            i += 1
            n = 0
            same_news = []
            nid = item[0]
            sents = item[1]
            for s in sents:  #每个句子
                n +=1
                str_no_html, wl = filter_html_stopwords_pos(s, True, True, True)
                if len(wl) < 5:
                    continue
                h = sim_hash.simhash(wl)
                fir, sec, thi, fou = get_4_segments(h.__long__())
                t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                #将段落入库
                cursor.execute(insert_sentence_hash, (nid, str_no_html, n, h.__str__(), fir, sec, thi, fou, t))

                #检查是否有相同的段落
                cursor.execute(query_sen_sql, (str(fir), str(sec), str(thi), str(fou)))
                rows = cursor.fetchall()  #所有可能相同的段落
                for r in rows:
                    sen = r[1].decode('utf-8')
                    if r[0] in same_news or len(sen) < 20: # r[1]是utf-8类型。
                        continue
                    l1 = float(len(str_no_html))
                    l2 = float(len(sen))
                    if l1 > 1.5 * l2 or l2 > 1.5 * l1:
                        continue
                    if h.hamming_distance_with_val(long(r[2])) <= same_t:
                        nid1 = r[0]
                        #除了检查hash值,还要检查相同词组
                        wl2 = filter_html_stopwords_pos(sen, True, True)
                        set1 = set(wl)
                        set2 = set(wl2)
                        set_same = set1 & set2
                        l1 = float(len(set1))
                        l2 = float(len(set2))
                        l3 = float(len(set_same))
                        if l3 < min(l1, l2) * 0.6:  #相同比例要达到0.6
                            continue

                        #先检查两篇新闻是否是相同的, 若相同则忽略。 同样利用simhash计算
                        if sim_hash.is_news_same(nid, nid1, 4):
                            same_news.append(nid1)
                            continue
                        cursor.execute(insert_same_sentence, (nid, nid1, str_no_html, sen, t))
                conn.commit()
            if i % 100 == 0:
                t1 = datetime.datetime.now()
                logger.info('{0} finished! Latest 100 news takes {1}s'.format(i, (t1 - t0).total_seconds()))
                t0 = t1
        cursor.close()
        conn.close()
        del nid_sents_dict
    except:
        cursor.close()
        conn.close()
        logger.exception(traceback.format_exc())



'''
def cal_process(nid_set):
    logger.info('there are {} news to calulate'.format(len(nid_set)))
    i = 0
    t0 = datetime.datetime.now()
    for n in nid_set:
        i += 1
        cal_sentence_hash_on_nid(n)
        if i % 100 == 0:
            t1 = datetime.datetime.now()
            logger.info('{0} finished! Latest 100 news takes {1}s'.format(i, (t1 - t0).total_seconds()))
            t0 = t1
'''


cal_sql = "select nid from newslist_v2 limit %s offset %s"
cal_sql2 ='SELECT a.nid \
FROM newslist_v2 a \
RIGHT OUTER JOIN (select * from channellist_v2 where "cname" not in %s) c \
ON \
a."chid" =c."id"  LIMIT %s offset %s'
ignore_cname = ("美女", "帅哥", "搞笑", "趣图")

def coll_sentence_hash():
    logger.info("Begin to collect sentence...")
    exist_set = get_exist_nids()
    limit = 10000
    offset = 10000
    pool = Pool(25)
    while True:
        conn, cursor = get_postgredb()
        cursor.execute(cal_sql2, (ignore_cname, limit, offset))
        rows = cursor.fetchall()
        conn.close()
        offset += limit
        if len(rows) == 0:
            break
        all_set = set()
        for r in rows:
            all_set.add(r[0])
        need_to_cal_set = all_set - exist_set
        if len(need_to_cal_set) == 0:
            continue
        pool.apply_async(cal_process, args=(need_to_cal_set, ))

    pool.close()
    pool.join()

    logger.info("Congratulations! Finish to collect sentences.")




