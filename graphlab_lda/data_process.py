# -*- coding: utf-8 -*-
# @Time    : 17/3/16 下午6:22
# @Author  : liulei
# @Brief   : 
# @File    : data_process.py
# @Software: PyCharm Community Edition
import os
import datetime
from util import doc_process
from util.logger import Logger
import traceback
import pandas as pd
import jieba
import jieba.analyse

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger = Logger('data_process', os.path.join(real_dir_path,  'log/data_process.txt'))
time_str = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
save_path = ''
doc_num_per_chnl = 50
doc_min_len = 100
csv_columns = ('nid', 'doc')


#需要
channel_for_topic = ['科技', '外媒', '社会', '财经', '体育', '汽车', '国际', '时尚', '探索', '科学',
                     '娱乐', '养生', '育儿', '股票', '互联网', '美食', '健康', '影视', '军事', '历史',
                     '故事', '旅游', '美文', '萌宠', '游戏', '点集', '自媒体', '奇闻']
excluded_chnl = ['美女', '视频', '趣图', '搞笑']


channle_sql ='SELECT a.title, a.content, a.nid \
FROM newslist_v2 a \
INNER JOIN (select * from channellist_v2 where "cname"=%s) c \
ON \
a."chid"=c."id" ORDER BY nid desc LIMIT %s'

news_word_sql = "select nid, title, content from newslist_v2 where nid in ({})"

def get_news_words(nid_list):
    conn, cursor = doc_process.get_postgredb_query()
    nids_str = ','.join([str(i) for i in nid_list])
    cursor.execute(news_word_sql.format(nids_str))
    rows = cursor.fetchall()
    conn.close()
    nid_words_dict = {}
    for r in rows:
        nid = r[0]
        title = r[1]
        paragraphs = r[2]
        txt = ''
        for para in paragraphs:
            if 'txt' in para.keys():
                txt += para['txt']
        total_txt = title + txt.encode('utf-8')
        word_list = doc_process.filter_html_stopwords_pos(total_txt, remove_num=True, remove_single_word=True)
        nid_words_dict[nid] = ' '.join(word_list)
    return nid_words_dict



def coll_news_proc(save_dir, chnl, doc_num_per_chnl, csv_path):
    try:
        logger.info('    start to collect {} ......'.format(chnl))
        #f = open(os.path.join(save_dir, chnl), 'w') #定义频道文件
        conn, cursor = doc_process.get_postgredb_query()
        cursor.execute(channle_sql, (chnl, doc_num_per_chnl))
        logger.info('        finish to query {} '. format(chnl))
        rows = cursor.fetchall()
        print len(rows)
        df = pd.DataFrame(columns=csv_columns)
        for row in rows:
            title = row[0]
            content_list = row[1]
            txt = ''
            for content in content_list:
                if 'txt' in content.keys():
                    txt += content['txt'].encode('utf-8')
            total_txt = title + txt
            data = {'nid':[row[2]], 'doc':[total_txt]}
            df_local = pd.DataFrame(data, columns=csv_columns)
            df = df.append(df_local, ignore_index=True)
            '''
            total_list = doc_process.filter_html_stopwords_pos(total_txt, remove_num=True, remove_single_word=True)
            if len(total_list) < doc_min_len:  #字数太少则丢弃
                continue
            #根据tfidf进行二次筛选
            total_list = doc_process.jieba_extract_keywords(' '.join(total_list), min(50, len(total_list)/5))
            for w in total_list:
                f.write(w.encode('utf-8') + ' ')
            f.write('\n')
            #f.write(' '.join(total_list).encode('utf-8') + '\n')
            del content_list
            '''
        df.to_csv(csv_path, index=False)
        cursor.close()
        conn.close()
        #f.close()
        logger.info('    finished to collect {} ......'.format(chnl))
    except:
        traceback.print_exc()
        logger.exception(traceback.format_exc())


def clear_doc(txt, features):
    wds = txt.split()
    logger.info('len is {}'.format(len(wds)))
    wds = [w for w in wds if w.decode('utf-8') in features]
    logger.info('after len is {}'.format(len(wds)))
    if len(wds) < 20:
        return ''
    return ' '.join(wds)

def doc_preprocess(csv_path, save_path):
    raw_df = pd.read_csv(csv_path)
    df = raw_df['doc'] # Series
    df = df.apply(doc_process.cut_pos_ltp)
    #tfidf 筛选
    from sklearn.feature_extraction.text import TfidfVectorizer
    tfidf_vec = TfidfVectorizer(max_df=0.001, min_df=0.00001)
    tfidf = tfidf_vec.fit_transform(df)
    features = tfidf_vec.get_feature_names()
    print 'feature num = ' + str(len(features))
    logger.info('len of feature = '.format(len(features)))
    df = df.apply(clear_doc, args=(features,))
    ds = df.values
    ds = [i for i in ds if len(i) != 0]
    df = pd.DataFrame({'nid': raw_df['nid'], 'doc': ds}, columns=csv_columns)
    df.to_csv(save_path, index=False)


class DocProcess(object):
    '''collect docs for training model'''
    def __init__(self, doc_num_per_chnl, doc_min_len):
        self.doc_num_per_chnl = doc_num_per_chnl
        self.doc_min_len = doc_min_len
        str_time = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        self.save_dir = os.path.join(real_dir_path, 'data', str_time)
        self.data_file = os.path.join(self.save_dir, 'raw.csv')
        if not os.path.exists(self.save_dir):
            os.mkdir(self.save_dir)
        with open(self.data_file, 'w') as f: #定义总文件
            pass

    def coll_news_handler(self):
        logger.info("coll_news_handler begin ...!")
        t0 = datetime.datetime.now()
        from multiprocessing import Pool
        pool = Pool(30)
        from util.doc_process import join_csv
        chnl_file = []
        for chanl in channel_for_topic:
            path = os.path.join(self.save_dir, chanl+'.csv')
            chnl_file.append(path)
            pool.apply_async(coll_news_proc, args=(self.save_dir, chanl, self.doc_num_per_chnl, path))
        pool.close()
        pool.join()
        join_csv(chnl_file, self.data_file, csv_columns)
        doc_preprocess(self.data_file, os.path.join(self.save_dir, 'data_after_process.csv'))
        t1 = datetime.datetime.now()
        logger.info("coll_news_handler finished!, it takes {}s".format((t1 - t0).total_seconds()))


def coll_news():
    try:
        dp = DocProcess(doc_num_per_chnl, doc_min_len)
        dp.coll_news_handler()
        print 'collect news finished!'
        logger.info('collect news finished!')
    except:
        logger.error(traceback.format_exc())












