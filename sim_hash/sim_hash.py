# -*- coding: utf-8 -*-
# @Time    : 17/2/4 下午2:46
# @Author  : liulei
# @Brief   : 
# @File    : sim_hash.py
# @Software: PyCharm Community Edition

from util import doc_process
import traceback
import logging
import datetime
import os
import requests
from util.simhash import simhash, get_4_segments
from util.logger import Logger

real_dir_path = os.path.split(os.path.realpath(__file__))[0]

logger = Logger('sim_hash', os.path.join(real_dir_path, 'log/log.txt'))

###########################################################################
#@brief :计算新闻的hash值.
#@input  : nid, int或str类型都可以
###########################################################################
def get_news_hash(nid_list):
    try:
        nid_hash_dict = {}
        for nid in nid_list:
            words_list = doc_process.get_words_on_nid(nid)
            h = simhash(words_list)
            nid_hash_dict[nid] = h.__long__()
        return nid_hash_dict
    except:
        logger.error(traceback.format_exc())
        return {}


################################################################################
#@brief: 查看是否有重复。  相似度达到一定值就认为是相同
#@input:
#         news_simhash   ---  新闻的simhash对象
#        check_interval --- 检查数据时间范围,例如检查三天内的数据, default: 999999
#        threshold      ---  相同的判断阈值
#@output: list  --- 相同的nid
################################################################################
hash_sql = "select ns.nid, hash_val from news_simhash ns inner join newslist_v2 nv on ns.nid=nv.nid where (ns.ctime > now() - interval '{0} day') and nv.state=0 " \
           "and (first_16='{1}' or second_16='{2}' or third_16='{3}' or fourth_16='{4}') and (first2_16='{5}' or second2_16='{6}' or third2_16='{7}' or fourth2_16='{8}') "
def get_news_interval(h, interval = 9999):
    fir, sec, thi, fou, fir2, sec2, thi2, fou2 = get_4_segments(h.__long__())
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(hash_sql.format(interval, fir, sec, thi, fou, fir2, sec2, thi2, fou2))
    rows = cursor.fetchall()
    nid_hv_list = []
    for r in rows:
        nid_hv_list.append((r[0], r[1]))
    conn.close()
    return nid_hv_list

def get_same_news(news_simhash, check_list, threshold = 3):
    try:
        same_list = []
        for r in check_list:
            hv = r[1]
            dis = news_simhash.hamming_distance_with_val(int(hv))
            if dis <= threshold:  #存在相同的新闻
                same_list.append(r[0])
                break
        return same_list
    except:
        logger.error(traceback.format_exc())


#去除广告的原则
#得分越小, 越需要删除
def goal_to_del(contents, coment_num):
    img_num = 0
    para_num = 0
    for con in contents:
        para_num += 1
        if 'img' in con.keys():
            img_num += 1

    return img_num + para_num + 0.3*coment_num


################################################################################
#@brief : 删除重复的新闻
################################################################################
#get_comment_num_sql = 'select nid, comment, content from newslist_v2 where nid in ({0}, {1})'
get_comment_num_sql = 'select nv.nid, nv.comment, ni.content from newslist_v2 nv inner join info_news ni on nv.nid=ni.nid where nv.nid in ({0}, {1})'
recommend_sql = "select nid, rtime from newsrecommendlist where rtime > now() - interval '1 day' and nid in (%s, %s)"
#offonline_sql = 'update newslist_v2 set state=1 where nid={0}'
url = "http://114.55.142.40:9001/news_delete"
def del_nid_of_fewer_comment(nid, n):
    try:
        conn, cursor = doc_process.get_postgredb_query()
        #先判断新闻是否已经被手工推荐。有则删除没有被手工推荐的新闻
        cursor.execute(recommend_sql, (nid, n))
        rs = cursor.fetchall()
        if len(rs) == 1: #一个被手工上线
            for r in rs:
                rnid = r[0]
            if rnid == n:
                del_nid = nid
                stay_nid = n
            else:
                del_nid = n
                stay_nid = nid
            #cursor.execute(offonline_sql.format(del_nid))
            #conn.commit()
            data = {}
            data['nid'] = del_nid
            response = requests.post(url, data=data)
            cursor.close()
            conn.close()
            logger.info('{0} has been recommended, so offline {1}'.format(stay_nid, del_nid))
            return

        cursor.execute(get_comment_num_sql.format(nid, n))
        rows = cursor.fetchall()
        nid_goal = []
        for r in rows:
            nid_goal.append((r[0], goal_to_del(r[2], r[1])))
        sorted_goal = sorted(nid_goal, key=lambda goal:goal[1])
        del_nid = sorted_goal[0][0]

        data = {}
        data['nid'] = del_nid
        response = requests.post(url, data=data)
        cursor.close()
        conn.close()
        logger.info('{0} vs {1},  offline {2}'.format(nid, n, del_nid))
    except Exception as e:
        logger.error(traceback.format_exc())


################################################################################
#@brief : 计算新闻hash值,并且检测是否是重复新闻。如果重复,则删除该新闻
################################################################################
insert_same_sql = 'insert into news_simhash_map (nid, same_nid) VALUES ({0}, {1})'
insert_news_simhash_sql = "insert into news_simhash (nid, hash_val, ctime, first_16, second_16, third_16, fourth_16, first2_16, second2_16, third2_16, fourth2_16) " \
                          "VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}')"
def cal_and_check_news_hash(nid_list):
    try:
        logger.info('begin to calculate simhash of {}'.format(' '.join(str(m) for m in nid_list)))
        t0 = datetime.datetime.now()
        conn, cursor = doc_process.get_postgredb()
        for nid in nid_list:
            words_list = doc_process.get_words_on_nid(nid)
            h = simhash(words_list)
            check_list = get_news_interval(h, 2)
            same_list = get_same_news(h, check_list)
            if len(same_list) > 0: #已经存在相同的新闻
                for n in same_list:
                    if n != nid:
                        cursor.execute(insert_same_sql.format(nid, n))
                        del_nid_of_fewer_comment(nid, n)
            #else: #没有相同的新闻,将nid添加到news_hash
            t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fir, sec, thi, fou, fir2, sec2, thi2, fou2 = get_4_segments(h.__long__())
            cursor.execute(insert_news_simhash_sql.format(nid, h.__str__(), t, fir, sec, thi, fou, fir2, sec2, thi2, fou2))
            conn.commit()
        cursor.close()
        conn.close()
        t1 = datetime.datetime.now()
        logger.info('finish to calculate simhash. it takes {} s'.format(str((t1 - t0).total_seconds())))
    except:
        logger.error(traceback.format_exc())


def is_news_same(nid1, nid2, same_t=3):
    try:
        w1 = doc_process.get_words_on_nid(nid1)
        w2 = doc_process.get_words_on_nid(nid2)
        h1 = simhash(w1)
        h2 = simhash(w2)
        #print h1.hamming_distance(h2)
        if h1.hamming_distance(h2) > same_t:
            return False
        return True
    except:
        raise

import jieba
if __name__ == '__main__':

    nid_list = [11952459, 11952414]
    #print is_news_same(11952760, 11963937, 3)
    print is_news_same(3235506, 3375849, 4)
    print is_news_same(3211559, 3212267, 4)
    print is_news_same(3248729, 3247245, 4)
    print is_news_same(12119757, 12119757, 4)
    print is_news_same(15134277, 15135383, 4)
    #cal_and_check_news_hash(nid_list)
    #w1 = doc_process.get_words_on_nid(11580728)
    #w2 = doc_process.get_words_on_nid(11603489)
    #h1 = simhash(w1)
    #h2 = simhash(w2)
    #print 100 * h2.similarity(h1)
    #print h1.hamming_distance(h2), "bits differ out of", h1.hashbits




