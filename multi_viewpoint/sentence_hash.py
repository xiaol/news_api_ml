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


insert_sentence_hash = "insert into news_sentence_hash (nid, sentence, hash_val, ctime) VALUES({0}, '{1}', '{2}', '{3}')"
insert_sentence_hash = "insert into news_sentence_hash (nid, sentence, hash_val, ctime) VALUES(%s,  %s, %s, %s)"
query_sen_sql = "select nid, sentence, hash_val from news_sentence_hash"
insert_same_sentence = "insert into news_same_sentence_map (nid1, nid2, sentence1, sentence2, ctime) VALUES ('{0}', '{1}', '{2}', '{3}', '{4}')"
def cal_sentence_hash_on_nid(nid, same_t=0.95):
    sentences_list = doc_process.get_sentences_on_nid(nid)
    n = 0
    conn1, cursor1 = doc_process.get_postgredb()
    cursor1.execute(query_sen_sql)
    rows = cursor1.fetchall()
    for s in (su.encode('utf-8') for su in sentences_list):
        s_list = doc_process.filter_html_stopwords_pos(s, remove_num=True, remove_single_word=True)
        n += 1
        if len(s) < 30: #10个汉字
            continue
        h = sim_hash.simhash(s_list, hashbits=64)
        hv = h.__str__()
        t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn, cursor = doc_process.get_postgredb()
        #检查是否有相同的段落
        for r in rows:
            l1 = float(len(s))
            l2 = float(len(r[1]))
            if l1 > 1.5 * l2 or l2 > 1.5 * l1:
                continue
            if h.similarity_with_val(int(r[2])) > same_t:
                nid1 = r[0]
                #先检查两篇新闻是否是相同的, 若相同则忽略。 同样利用simhash计算
                #if sim_hash.is_news_same(nid, nid1, same_t):
                #    continue
                cursor.execute(insert_same_sentence.format(nid, nid1, s, r[1], t))
                break

        #插入库
        #cursor.execute(insert_sentence_hash.format(nid, s, hv, t))
        cursor.execute(insert_sentence_hash, (nid, s, hv, t))
        conn.commit()
        cursor.close()
        conn.close()
        conn1.close()


cal_sql = 'select nid from newslist_v2'
def coll_sentence_hash():
    conn, cursor = doc_process.get_postgredb()
    cursor.execute(cal_sql)
    rows = cursor.fetchall()
    logger.info('there are {} news to calulate'.format(str(len(rows))))
    i = 0
    for n in rows:
        i += 1
        if i % 1 == 0:
            logger.info('{} finished!'.format(i * 100))
        cal_sentence_hash_on_nid(n[0])



