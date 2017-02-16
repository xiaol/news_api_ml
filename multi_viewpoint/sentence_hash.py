# -*- coding: utf-8 -*-
# @Time    : 17/2/13 上午10:09
# @Author  : liulei
# @Brief   : 
# @File    : sentence_hash.py
# @Software: PyCharm Community Edition
from util  import doc_process
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
    doc_process.get_sentences_on_nid(nid)


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
        sentences_list = doc_process.get_sentences_on_nid(nid)
        same_news = []
        n = 0
        conn, cursor = doc_process.get_postgredb()
        for s in (su.encode('utf-8') for su in sentences_list):  #计算每一段话的hash值
            s_list = doc_process.filter_tags(s)
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
                if h.hamming_distance_with_val(long(r[2])) > same_t:
                    nid1 = r[0]
                    #先检查两篇新闻是否是相同的, 若相同则忽略。 同样利用simhash计算
                    if sim_hash.is_news_same(nid, nid1, same_t):
                        same_news.append(nid1)
                        continue
                    cursor.execute(insert_same_sentence, (nid, nid1, s, r[1], t))
                    break
            #插入库
            cursor.execute(insert_sentence_hash, (nid, s, n, h.__str__(), fir, sec, thi, fou, t))
            conn.commit()
        cursor.close()
        conn.close()
    except:
        cursor.close()
        conn.close()
        logger.exception(traceback.format_exc())


def cal_sentence_hash_nid_list(nid_list):
    for n in nid_list:
        cal_sentence_hash_on_nid(n)

s_nid_sql = "select distinct nid from news_sentence_hash "
def get_exist_nids():
    conn, cursor = doc_process.get_postgredb()
    cursor.execute(cal_sql)
    rows = cursor.fetchall()
    nid_set = set()
    for r in rows:
        nid_set.add(r[0])
    conn.close()


################################################################################
#@brief: 计算子进程
################################################################################
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


cal_sql = 'select nid from newslist_v2 limit %s offset %s'
def coll_sentence_hash():
    logger.info("Begin to collect sentence...")
    exist_set = get_exist_nids()
    limit = 5
    offset = 5
    pool = Pool(5)
    i = 0
    while True:
        conn, cursor = doc_process.get_postgredb()
        cursor.execute(cal_sql, (limit, offset))
        rows = cursor.fetchall()
        conn.close()
        if len(rows) == 0:
            break
        all_set = set()
        for r in rows:
            all_set.add(r[0])
        need_to_cal_set = all_set - exist_set
        pool.apply_async(cal_process, args=(need_to_cal_set,))
        i += len(rows)
        if i > 100:
            logger.info('100 finished!!')
            break

    pool.close()
    pool.join()

    logger.info("Congratulations! Finish to collect sentences.")




