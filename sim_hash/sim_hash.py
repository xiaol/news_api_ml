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

real_dir_path = os.path.split(os.path.realpath(__file__))[0]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(real_dir_path + '/../log/sim_hash/log.txt')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class simhash():
    def __init__(self, tokens='', hashbits=128):
        self.hashbits = hashbits
        self.hash = self.simhash(tokens)

    def __str__(self):
        return str(self.hash)

    def __long__(self):
        return long(self.hash)

    def __float__(self):
        return float(self.hash)

    def simhash(self, tokens):
        # Returns a Charikar simhash with appropriate bitlength
        v = [0] * self.hashbits

        for t in [self._string_hash(x) for x in tokens]:
            bitmask = 0
            # print (t)
            for i in range(self.hashbits):
                bitmask = 1 << i
                # print(t,bitmask, t & bitmask)
                if t & bitmask:
                    v[i] += 1  # 查看当前bit位是否为1，是的话则将该位+1
                else:
                    v[i] += -1  # 否则得话，该位减1

        fingerprint = 0
        for i in range(self.hashbits):
            if v[i] >= 0:
                fingerprint += 1 << i
                # 整个文档的fingerprint为最终各个位大于等于0的位的和
        return fingerprint

    #计算一个词的hash值。 一个汉字utf-8中使用三个字节表示
    def _string_hash(self, v):
        # A variable-length version of Python's builtin hash
        if v == "":
            return 0
        else:
            x = ord(v[0]) << 7   #第一个字节左移7位
            m = 1000003
            mask = 2 ** self.hashbits - 1
            for c in v:
                x = ((x * m) ^ ord(c)) & mask
            x ^= len(v)
            if x == -1:
                x = -2
            return x

    #与另一个simhash类比较
    def hamming_distance(self, other_hash):
        x = (self.hash ^ other_hash.hash) & ((1 << self.hashbits) - 1)
        tot = 0
        while x:
            tot += 1
            x &= x - 1
        return tot

    #与一个hash值比较
    def hamming_distance_with_val(self, other_hash_val):
        x = (self.hash ^ other_hash_val) & ((1 << self.hashbits) - 1)
        tot = 0
        while x:
            tot += 1
            x &= x - 1
        return tot

    def similarity(self, other_hash):
        '''
        a = float(self.hash)
        b = float(other_hash.hash)
        if a > b: return b / a
        return a / b
        '''
        b = self.hashbits
        return float(b - self.hamming_distance(other_hash)) / b

    def similarity_with_val(self, other_hash_val):
        b = self.hashbits
        return float(b - self.hamming_distance_with_val(other_hash_val)) / b


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
hash_sql = "select nid, hash_val, ctime from news_simhash where ctime > now() - interval '{0} day'"
def get_same_news(news_simhash, check_interval=999999, threshold = 0.95):
    try:
        conn, cursor = doc_process.get_postgredb()
        cursor.execute(hash_sql.format(check_interval))
        rows = cursor.fetchall()
        same_list = []
        for r in rows:
            hv = r[1]
            print news_simhash.similarity_with_val(int(hv))
            if news_simhash.similarity_with_val(int(hv)) >= threshold:  #存在相同的新闻
                same_list.append(r[0])
                break
        cursor.close()
        conn.close()
        return same_list
    except:
        cursor.close()
        conn.close()
        logger.error(traceback.format_exc())


################################################################################
#@brief : 删除重复的新闻
################################################################################
del_sql = 'delete from newslist_v2 where nid={0}'
def del_nid(nid):
    try:
        conn, cursor = doc_process.get_postgredb()
        cursor.execute(del_sql.format(nid))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info('delete {}'.format(nid))
    except Exception as e:
        logger.error(traceback.format_exc())


################################################################################
#@brief : 计算新闻hash值,并且检测是否是重复新闻。如果重复,则删除该新闻
################################################################################
insert_same_sql = 'insert into news_simhash_map (nid, same_nid) VALUES ({0}, {1})'
insert_news_simhash_sql = "insert into news_simhash (nid, hash_val, ctime) VALUES('{0}', '{1}', '{2}')"
def cal_and_check_news_hash(nid_list):
    try:
        logger.info('begin to calculate simhash of {}'.format(' '.join(str(m) for m in nid_list)))
        t0 = datetime.datetime.now()
        for nid in nid_list:
            words_list = doc_process.get_words_on_nid(nid)
            h = simhash(words_list)
            same_list = get_same_news(h, 2)
            conn, cursor = doc_process.get_postgredb()
            if len(same_list) > 0: #已经存在相同的新闻
                for n in same_list:
                    if n != nid:
                        cursor.execute(insert_same_sql.format(nid, n))
                        del_nid(nid)
            else: #没有相同的新闻,将nid添加到news_hash
                t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute(insert_news_simhash_sql.format(nid, h.__str__(), t))
            conn.commit()
            cursor.close()
            conn.close()
        t1 = datetime.datetime.now()
        logger.info('finish to calculate simhash. it takes {} s'.format(str((t1 - t0).total_seconds())))
    except:
        logger.error(traceback.format_exc())


import jieba
if __name__ == '__main__':

    nid_list = [11584438, 11576416]
    cal_and_check_news_hash(nid_list)
    #w1 = doc_process.get_words_on_nid(11580728)
    #w2 = doc_process.get_words_on_nid(11603489)
    #h1 = simhash(w1)
    #h2 = simhash(w2)
    #print 100 * h2.similarity(h1)
    #print h1.hamming_distance(h2), "bits differ out of", h1.hashbits




