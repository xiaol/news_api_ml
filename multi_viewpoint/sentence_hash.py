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
from util.doc_process import filter_tags
from util.doc_process import filter_url
from util.doc_process import get_sentences_on_nid

from sim_hash import sim_hash
import datetime
import os
import logging
import traceback
from multiprocessing import Process
from multiprocessing import Pool
import jieba

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
    fir2 = hash_bits & 0b11000000001111000000000000000011110000000000001111000000000000
    sec2 = hash_bits & 0b00111100000000111100000000000000001111000000000000111100000000
    thi2 = hash_bits & 0b00000011110000000011110000000000000000111100000000000011110000
    fou2 = hash_bits & 0b00000000001111000000001111000000000000000011110000000000001111
    return fir, sec, thi, fou, fir2, sec2, thi2, fou2



#insert_sentence_hash = "insert into news_sentence_hash (nid, sentence, hash_val, ctime) VALUES({0}, '{1}', '{2}', '{3}')"
insert_sentence_hash = "insert into news_sentence_hash (nid, sentence, sentence_id, hash_val, first_16, second_16, third_16, fourth_16, ctime, first2_16, second2_16, third2_16, fourth2_16) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
#query_sen_sql = "select nid, sentence, hash_val from news_sentence_hash"
#query_sen_sql = "select nid, sentence, hash_val from news_sentence_hash where first_16=%s or second_16=%s or third_16=%s or fourth_16=%s"
query_sen_sql = "select nid, hash_val from news_sentence_hash where (first_16=%s or second_16=%s or third_16=%s or fourth_16=%s) and (first2_16=%s or second2_16=%s or third2_16=%s or fourth2_16=%s) "
#insert_same_sentence = "insert into news_same_sentence_map (nid1, nid2, sentence1, sentence2, ctime) VALUES ('{0}', '{1}', '{2}', '{3}', '{4}')"
insert_same_sentence = "insert into news_same_sentence_map (nid1, nid2, sentence1, sentence2, ctime) VALUES (%s, %s, %s, %s, %s)"
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
get_sent_sql = "select nid, title, content, state from newslist_v2 where nid in %s"
def get_nids_sentences(nid_set):
    nid_tuple = tuple(nid_set)
    conn, cursor = get_postgredb()
    cursor.execute(get_sent_sql, (nid_tuple, ))
    rows = cursor.fetchall()
    nid_sentences_dict = {}
    for r in rows:
        if r[3] != 0: #已被下线
            logger.info('{} has been offline.'.format(r[0]))
            continue
        nid = r[0]
        nid_sentences_dict[nid] = []
        content_list = r[2]
        for content in content_list:
                if "txt" in content.keys():
                    #str_no_tags = filter_tags(content['txt'])
                    nid_sentences_dict[nid].extend(Cut(filter_tags(content['txt'])))
                    #for i in sents:
                        #if len(i) > 20:  #20个汉字, i 是unicode, len代表汉字个数
                    #    nid_sentences_dict[nid].append(i) #计算所有段落。 计算重复句子时再筛选掉字数少的句子; 去除广告时,对字数不做要求
                        #wl = filter_html_stopwords_pos(i)
                        #if len(wl) > 5:   #文本词数量<5, 不计算hash
                        #    nid_sentences_dict[nid].append(wl)
    conn.close()
    return nid_sentences_dict


################################################################################
#@brief : 读取相同的新闻
################################################################################
same_sql = "select nid, same_nid from news_simhash_map where (nid in %s) or (same_nid in %s) "
def get_relate_same_news(nid_set):
    conn, cursor = get_postgredb()
    nid_tuple = tuple(nid_set)
    cursor.execute(same_sql, (nid_tuple, nid_tuple))
    same_dict = {}
    rows = cursor.fetchall()
    for r in rows:
        if r[0] not in same_dict.keys():
            same_dict[r[0]] = []
        if r[1] not in same_dict.keys():
            same_dict[r[1]] = []
        same_dict[r[0]].append(r[1])
        same_dict[r[1]].append(r[0])

    conn.close()
    return same_dict


################################################################################
#@brief: 计算子进程
################################################################################
def cal_process(nid_set, same_t=3):
    logger.info('there are {} news to calulate'.format(len(nid_set)))
    nid_sents_dict = get_nids_sentences(nid_set)
    same_dict = get_relate_same_news(nid_set)
    try:
        i = 0
        #t0 = datetime.datetime.now()
        conn, cursor = get_postgredb()
        for item in nid_sents_dict.items(): #每条新闻
            i += 1
            n = 0
            nid = item[0]
            sents = item[1]
            t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for s in sents:  #每个句子
                n += 1
                print n
                str_no_html, wl = filter_html_stopwords_pos(s, True, True, True, False)
                h = sim_hash.simhash(wl)
                fir, sec, thi, fou, fir2, sec2, thi2, fou2 = get_4_segments(h.__long__())

                #检查是否有相同的段落
                #if len(wl) < 5: #小于20个汉字, 不判断句子重复  #2.22再次修改: 不做长度限制做重复判断; 真正判断相关观点时再判断; 广告去除也需要短句子
                #    continue
                print fir, sec, thi, fou, fir2, sec2, thi2, fou2
                cursor.execute(query_sen_sql, (str(fir), str(sec), str(thi), str(fou), str(fir2), str(sec2), str(thi2), str(fou2)))
                rows = cursor.fetchall()  #所有可能相同的段落
                for r in rows:
                    #距离过大或者是同一篇新闻
                    if h.hamming_distance_with_val(long(r[1])) > same_t or (nid in same_dict.keys() and r[0] in same_dict[nid]):
                        continue
                    same_sql = "select sentence from news_sentence_hash where nid=%s and hash_val=%s"
                    cursor.execute(same_sql, (r[0], r[1]))
                    rs = cursor.fetchall()
                    for r2 in rs:
                        sen = r2[0].decode('utf-8')
                        cursor.execute(insert_same_sentence, (nid, r[0], str_no_html, sen, t))

                    '''
                    sen = r[1].decode('utf-8')
                    sen_without_html = filter_tags(sen)
                    if len(str_no_html) > len(sen_without_html) * 1.5 or len(sen_without_html) > len(str_no_html) * 1.5:
                        continue
                    #除了检查hash值,还要检查相同词组
                    wl1 = jieba.cut(str_no_html)
                    wl2 = jieba.cut(sen_without_html)
                    set1 = set(wl1)
                    set2 = set(wl2)
                    set_same = set1 & set2
                    l1 = float(len(set1))
                    l2 = float(len(set2))
                    l3 = float(len(set_same))
                    if l3 < max(l1, l2) * 0.6:  #相同比例要达到0.6
                        continue

                    #先检查两篇新闻是否是相同的, 若相同则忽略。 同样利用simhash计算
                    nid1 = r[0]
                    sql = "select * from news_simhash_map where (nid = %s and same_nid = %s) or (nid = %s and same_nid = %s) "
                    cursor.execute(sql, (nid, nid1, nid1, nid))
                    if len(cursor.fetchall()) > 0:
                        continue
                    if sim_hash.is_news_same(nid, nid1, 3):
                        in_sql = 'insert into news_simhash_map (nid, same_nid) values(%s, %s)'
                        cursor.execute(in_sql, (nid, nid1))
                        continue
                    cursor.execute(insert_same_sentence, (nid, nid1, str_no_html, sen, t))
                    '''

                #将所有段落入库
                cursor.execute(insert_sentence_hash, (nid, str_no_html, n, h.__str__(), fir, sec, thi, fou, t, fir2, sec2, thi2, fou2))
                conn.commit()
            #if i % 100 == 0:
                #t1 = datetime.datetime.now()
                #logger.info('{0} finished! Latest 100 news takes {1}s'.format(i, (t1 - t0).total_seconds()))
                #t0 = t1
        cursor.close()
        conn.close()
        del nid_sents_dict
    except:
        cursor.close()
        conn.close()
        logger.exception(traceback.format_exc())


#供即时计算
def coll_sentence_hash_time(nid_list):
    nid_set = set(nid_list)
    cal_process(nid_set)
    logger.info("Congratulations! Finish to collect sentences.")



cal_sql = "select nid from newslist_v2 limit %s offset %s"
cal_sql2 ='SELECT a.nid \
FROM newslist_v2 a \
RIGHT OUTER JOIN (select * from channellist_v2 where "cname" not in %s) c \
ON \
a."chid" =c."id" where a.state=0 LIMIT %s offset %s'
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




