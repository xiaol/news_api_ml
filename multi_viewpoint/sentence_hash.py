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
from util.doc_process import get_sentences_on_nid
from bs4 import BeautifulSoup

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
    return str(fir), str(sec), str(thi), str(fou), str(fir2), str(sec2), str(thi2), str(fou2)



#insert_sentence_hash = "insert into news_sentence_hash (nid, sentence, hash_val, ctime) VALUES({0}, '{1}', '{2}', '{3}')"
insert_sentence_hash = "insert into news_sentence_hash (nid, sentence, sentence_id, hash_val, first_16, second_16, third_16, fourth_16, ctime, first2_16, second2_16, third2_16, fourth2_16) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
#query_sen_sql = "select nid, sentence, hash_val from news_sentence_hash"
#query_sen_sql = "select nid, sentence, hash_val from news_sentence_hash where first_16=%s or second_16=%s or third_16=%s or fourth_16=%s"
query_sen_sql = "select nid, hash_val from news_sentence_hash where (first_16=%s or second_16=%s or third_16=%s or fourth_16=%s) and (first2_16=%s or second2_16=%s or third2_16=%s or fourth2_16=%s) group by nid, hash_val"
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
get_sent_sql = "select nid, title, content, state, pname from newslist_v2 where nid in %s"
def get_nids_sentences(nid_set):
    nid_tuple = tuple(nid_set)
    conn, cursor = get_postgredb()
    cursor.execute(get_sent_sql, (nid_tuple, ))
    rows = cursor.fetchall()
    nid_sentences_dict = {}
    nid_para_links_dict = {}
    nid_pname_dict = {}
    for r in rows:
        if r[3] != 0: #已被下线
            logger.info('{} has been offline.'.format(r[0]))
            continue
        nid = r[0]
        nid_sentences_dict[nid] = {}
        nid_para_links_dict[nid] = {}
        nid_pname_dict[nid] = r[4]
        content_list = r[2]
        pi = 0
        for content in content_list:
                if "txt" in content.keys():
                    pi += 1
                    #str_no_tags = filter_tags(content['txt'])
                    #nid_sentences_dict[nid].extend(Cut(filter_tags(content['txt'])))
                    soup = BeautifulSoup(content['txt'], 'lxml')
                    pi_sents = []
                    for link in soup.find_all('a'):
                        pi_sents.append(link)  #记录每一段的链接
                    nid_sentences_dict[nid][pi] = Cut(soup.text)
                    nid_para_links_dict[nid][pi] = pi_sents
                    #nid_sentences_dict[nid].extend(Cut(get_paragraph_text(content['txt'])))
                    #for i in sents:
                        #if len(i) > 20:  #20个汉字, i 是unicode, len代表汉字个数
                    #    nid_sentences_dict[nid].append(i) #计算所有段落。 计算重复句子时再筛选掉字数少的句子; 去除广告时,对字数不做要求
                        #wl = filter_html_stopwords_pos(i)
                        #if len(wl) > 5:   #文本词数量<5, 不计算hash
                        #    nid_sentences_dict[nid].append(wl)
    conn.close()
    return nid_sentences_dict, nid_para_links_dict, nid_pname_dict


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
#@brief: 检查句子是否是广告   如果已经被判定是广告,则不再判断有无重复
################################################################################
check_ads_sql = "select ads_sentence, hash_val, special_pname from news_ads_sentence where " \
                "(first_16=%s or second_16=%s or third_16=%s or four_16=%s) and" \
                "(first2_16=%s or second2_16=%s or third2_16=%s or four2_16=%s) "
def is_sentence_ads(hash_val, fir_16, sec_16, thi_16, fou_16, fir2_16, sec2_16, thi2_16, fou2_16, pname):
    conn, cursor = get_postgredb()
    cursor.execute(check_ads_sql, (fir_16, sec_16, thi_16, fou_16, fir2_16, sec2_16, thi2_16, fou2_16))
    rows = cursor.fetchall()
    for r in rows:
        if hash_val.hamming_distance_with_val(long(r[1])) <= 3:
            exist = False
            if r[2]:
                spnames = r[2].split(',')
                if len(spnames) == 0 or (pname in spnames):
                    exist = True
            else:
                exist = True
            if exist:
                conn.close()
                return True
    conn.close()
    return False


get_pname = "select pname, chid, ctime, nid from newslist_v2 where nid in %s"
same_sql2 = "select sentence from news_sentence_hash where nid=%s and hash_val=%s"
ads_insert = "insert into news_ads_sentence (ads_sentence, hash_val, ctime, first_16, second_16, third_16, four_16, first2_16, second2_16, third2_16, four2_16, nids, state, special_pname)" \
             "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
multo_vp_insert_sql = "insert into news_multi_vp (nid1, sentence1, nid2, sentence2, ctime) values (%s, %s, %s, %s, %s)"
################################################################################
#@brief: 计算子进程
################################################################################
def cal_process(nid_set, same_t=3):
    logger.info('there are {} news to calulate'.format(len(nid_set)))
    nid_sents_dict, nid_para_links_dict, nid_pname_dict = get_nids_sentences(nid_set)
    same_dict = get_relate_same_news(nid_set)
    try:
        for item in nid_sents_dict.items(): #每条新闻
            n = 0
            nid = item[0]
            logger.info('--- consume :{}'.format(nid))
            t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            #sents = []
            para_sent_dict = item[1]
            #for para in para_sent_list:
            #for pn in xrange(0, len(para_sent_dict)):
            for pa in para_sent_dict.items():
                para_num = pa[0]
                sents = pa[1]
                conn, cursor = get_postgredb()
                sen_len = len(sents)
                for s in sents:  #每个句子
                    n += 1
                    str_no_html, wl = filter_html_stopwords_pos(s, False, True, True, False)
                    if len(wl) == 0 or len(str_no_html) <= 2: #去除一个字的句子,因为有很多是特殊字符
                        continue
                    h = sim_hash.simhash(wl)
                    fir, sec, thi, fou, fir2, sec2, thi2, fou2 = get_4_segments(h.__long__())
                    if is_sentence_ads(h, fir, sec, thi, fou, fir2, sec2, thi2, fou2, nid_pname_dict[nid]):  #在广告db内
                        #  删除广告句子
                        logger.info('find ads of {0}  : {1} '.format(nid, str_no_html.encode("utf-8")))
                        continue
                    cursor.execute(query_sen_sql, (str(fir), str(sec), str(thi), str(fou), str(fir2), str(sec2), str(thi2), str(fou2)))
                    #print cursor.mogrify(query_sen_sql, (str(fir), str(sec), str(thi), str(fou), str(fir2), str(sec2), str(thi2), str(fou2)))
                    rows = cursor.fetchall()  #所有可能相同的段落
                    if len(rows) == 0:
                        continue

                    same_sentence_sql_para = []
                    nids_for_ads = set()
                    for r in rows:
                        #if len(nids_for_ads) >= 15:
                            #break
                        #距离过大或者是同一篇新闻
                        if h.hamming_distance_with_val(long(r[1])) > same_t or (nid in same_dict.keys() and r[0] in same_dict[nid]) or nid == r[0]:
                            continue
                        cursor.execute(same_sql2, (r[0], r[1]))
                        rs = cursor.fetchall()
                        for r2 in rs:
                            sen = r2[0].decode('utf-8')
                            sen_without_html = filter_tags(sen)
                            if len(sen) == 1 or len(sen_without_html) > len(str_no_html)*1.5 or len(str_no_html) > len(sen_without_html)*1.5:
                                continue
                            wl1 = jieba.cut(str_no_html)
                            set1 = set(wl1)
                            l1 = len(set1)
                            wl2 = jieba.cut(sen_without_html)
                            set2 = set(wl2)
                            set_same = set1 & set2
                            l2 = len(set2)
                            l3 = len(set_same)
                            if l3 < max(l1, l2) * 0.6:  #相同比例要达到0.6
                                continue
                            nids_for_ads.add(str(r[0]))
                            same_sentence_sql_para.append((nid, r[0], str_no_html, sen, t))
                            #cursor.execute(insert_same_sentence, (nid, r[0], str_no_html, sen, t))
                            #print cursor.mogrify(insert_same_sentence, (nid, r[0], str_no_html, sen_without_html, t))
                    if len(nids_for_ads) == 0:
                        cursor.execute(insert_sentence_hash, (nid, str_no_html, n, h.__str__(), fir, sec, thi, fou, t, fir2, sec2, thi2, fou2))
                        conn.commit()
                        continue

                    is_new_ads = False
                    not_ads_but_ignore = False   #不是广告,但需要忽略计算重复
                    PNAME_T = 3
                    nid_pn = {}
                    pname_set = set()
                    chid_set = set()
                    ctime_list = []
                    #print cursor.mogrify(get_pname, (tuple(nids_for_ads),))
                    cursor.execute(get_pname, (tuple(nids_for_ads),))
                    rows2 = cursor.fetchall()
                    for rk in rows2:
                        pname_set.add(rk[0])
                        chid_set.add(rk[1])
                        ctime_list.append(rk[2])
                        nid_pn[rk[3]] = rk[0]
                    if len(nids_for_ads) >= 20:
                        #先处理同源潜在广告
                        if len(pname_set) <= PNAME_T or (len(pname_set) > 5 and len(chid_set) < 4):
                            if n > sen_len * .2 and n < sen_len * .8:
                                min_time = ctime_list[0]
                                max_time = ctime_list[0]
                                for kkk in xrange(1, len(ctime_list)):
                                    if ctime_list[kkk] > max_time:
                                        max_time = ctime_list[kkk]
                                    if ctime_list[kkk] < min_time:
                                        min_time = ctime_list[kkk]
                                if (max_time - min_time).days > 2:  #不是三天内的热点新闻
                                    print (max_time - min_time).days
                                    print str_no_html
                                    is_new_ads = True
                            '''
                            nid_links = nid_para_links_dict[nid]
                            sum_own_links = 0  #有链接的段落数
                            for kk in xrange(para_num, len(nid_links)):
                                if len(nid_links[kk]):
                                    sum_own_links += 1
                            if sum_own_links > (len(nid_links) - para_num) * 0.8: #后面的链接很多,认为是广告
                                is_new_ads = True
                        elif len(pname_set) > 5 and len(chid_set) < 4:   #来自多个源, 看是否集中在几个频道,如果是,则认为是广告
                            #需要判断这些新闻入库时间不集中在3天内,否则可能不是广告
                            min_time = ctime_list[0]
                            max_time = ctime_list[0]
                            for kkk in xrange(1, len(ctime_list)):
                                if ctime_list[kkk] > max_time:
                                    max_time = ctime_list[kkk]
                                if ctime_list[kkk] < min_time:
                                    min_time = ctime_list[kkk]
                            if (max_time - min_time).days > 2:  #不是三天内的热点新闻
                                is_new_ads = True
                             '''
                        else:
                            not_ads_but_ignore = True
                    nids_str = ','.join(nids_for_ads)
                    if is_new_ads:
                        if len(pname_set) <= PNAME_T:
                            pname_str = ','.join(pname_set)
                        else:
                            pname_str = ""
                        cursor.execute(ads_insert, (str_no_html, h.__str__(), t, fir, sec, thi, fou, fir2, sec2, thi2, fou2, nids_str, 0, pname_str))
                        logger.info('find new ads : {0}'.format(str_no_html.encode("utf-8")))
                    else:
                        #if len(same_sentence_sql_para) < 5:  #检测出过多的相同句子,又不是广告, 可能是误判, 不处理
                        if not_ads_but_ignore:  #相同的句子过多,认为是误判, 加入广告数据库,但state=1,即不是真广告,这样可以在下次碰到时减少计算
                            cursor.execute(ads_insert, (str_no_html, h.__str__(), t, fir, sec, thi, fou, fir2, sec2, thi2, fou2, nids_str, 1, "" ))
                        else:
                            cursor.executemany(insert_same_sentence, same_sentence_sql_para)
                    #多放观点  1. 句子长度>30.  2 不同源
                    if len(str_no_html) > 30:
                        for same in same_sentence_sql_para:
                            nn = same[1]  #nid
                            if nid_pname_dict[nid] != nid_pn[nn]:
                                logger.info(same[1])
                                logger.info(same[2])
                                cursor.execute(multo_vp_insert_sql, (str(same[0]), same[2], str(same[1]), same[3], t) )
                                logger.info('get multi viewpoint :{}'.format(str_no_html))

                    #将所有段落入库
                    cursor.execute(insert_sentence_hash, (nid, str_no_html, n, h.__str__(), fir, sec, thi, fou, t, fir2, sec2, thi2, fou2))
                    conn.commit()
                cursor.close()
                conn.close()

        del nid_sents_dict
        del nid_para_links_dict
    except:
        logger.exception(traceback.format_exc())


#供即时计算
def coll_sentence_hash_time(nid_list):
    #nid_set = set(nid_list)
    # arr是被分割的list，n是每个chunk中含n元素。
    small_list = [nid_list[i:i + 20] for i in range(0, len(nid_list), 20)]
    pool = Pool(20)
    for nid_set in small_list:
        pool.apply_async(cal_process, args=(set(nid_set), ))

    pool.close()
    pool.join()
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




