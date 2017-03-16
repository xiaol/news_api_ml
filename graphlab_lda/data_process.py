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

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger = Logger('data_process', os.path.join(real_dir_path,  'log/data_process.txt'))
time_str = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
save_path = os.path.join(real_dir_path, time_str)
doc_num_per_chnl = 5
doc_min_len = 100


#需要
channel_for_topic = ['科技', '外媒', '社会', '财经', '体育', '汽车', '国际', '时尚', '探索', '科学',
                     '娱乐', '养生', '育儿', '股票', '互联网', '美食', '健康', '影视', '军事', '历史',
                     '故事', '旅游', '美文', '萌宠', '游戏', '点集', '自媒体', '奇闻', '奇点']
excluded_chnl = ['美女', '视频', '趣图', '搞笑']


channle_sql ='SELECT a.title, a.content \
FROM newslist_v2 a \
INNER JOIN (select * from channellist_v2 where "cname"=%s) c \
ON \
a."chid"=c."id" ORDER BY nid desc LIMIT %s'


def coll_news_proc(save_dir, chnl, doc_num_per_chnl, doc_min_len):
    try:
        logger.info('    start to collect {} ......'.format(chnl))
        f = open(os.path.join(save_dir, chnl), 'w+') #定义频道文件
        f.write('aaaaaaaaaaa')
        print '^^^^^^^^^^^^^^^^^^^^^^^^'
        print os.path.join(save_dir, chnl)
        conn, cursor = doc_process.get_postgredb_query()
        cursor.execute(channle_sql, [chnl, doc_num_per_chnl])
        rows = cursor.fetchall()
        for row in rows:
            title = row[0]
            content_list = row[1]
            txt = ''
            for content in content_list:
                if 'txt' in content.keys():
                    txt += content['txt'].encode('utf-8')
            total_txt = title + txt
            total_list = doc_process.filter_html_stopwords_pos(total_txt, remove_num=True, remove_single_word=True)
            if len(total_list) < doc_min_len:  #字数太少则丢弃
                continue
            #根据tfidf进行二次筛选
            total_list = doc_process.jieba_extract_keywords(' '.join(total_list), min(50, len(total_list)/5))
            f.write(' '.join(total_list).encode('utf-8') + '\n')
            del content_list
            del total_list
        cursor.close()
        conn.close()
        f.close()
        print 'fffffffff' + chnl
    except:
        print 'eeeeeeeeeeeeeeee'
        print traceback.format_exc()

doc_num_per_chnl = doc_num_per_chnl
doc_min_len = doc_min_len
str_time = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
save_dir = os.path.join(real_dir_path, 'data', str_time)
if not os.path.exists(save_dir):
    os.mkdir(save_dir)
with open(save_dir + '/data.txt', 'w') as f: #定义总文件
    pass
def coll_news_handler(save_dir, doc_num_per_chnl, doc_min_len):
    try:
        logger.info("coll_news_handler begin ...!")
        t0 = datetime.datetime.now()
        import multiprocessing as mp
        from multiprocessing import Pool
        pool = Pool(20)
        from util.doc_process import join_file
        procs = []
        chnl_file = []
        for chanl in channel_for_topic:
            chnl_file.append(os.path.join(save_dir, chanl))
            pool.apply_async(coll_news_proc, args=(save_dir, chanl, doc_num_per_chnl, doc_min_len))
            #coll_proc = mp.Process(target=self.coll_news_proc, args=(chanl,))
            #coll_proc.start()
            #procs.append(coll_proc)
        pool.close()
        pool.join()
        #for i in procs:
        #    i.join()
        join_file(chnl_file, os.path.join(save_dir, '/data.txt'))
        t1 = datetime.datetime.now()
        logger.info("coll_news_handler finished!, it takes {}s".format((t1 - t0).total_seconds()))
    except:
        traceback.print_exc()


class DocProcess(object):
    '''collect docs for training model'''
    def __init__(self, doc_num_per_chnl, doc_min_len):
        self.doc_num_per_chnl = doc_num_per_chnl
        self.doc_min_len = doc_min_len
        self.str_time = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        self.save_dir = os.path.join(real_dir_path, 'data', self.str_time)
        if not os.path.exists(self.save_dir):
            os.mkdir(self.save_dir)
        with open(self.save_dir + '/data.txt', 'w') as f: #定义总文件
            pass


    def coll_news_handler(self):
        logger.info("coll_news_handler begin ...!")
        t0 = datetime.datetime.now()
        import multiprocessing as mp
        from multiprocessing import Pool
        pool = Pool(20)
        from util.doc_process import join_file
        procs = []
        chnl_file = []
        for chanl in channel_for_topic:
            chnl_file.append(os.path.join(self.save_dir, chanl))
            pool.apply_async(coll_news_proc, args=(self.save_dir, chanl, self.doc_num_per_chnl, self.doc_min_len))
            #coll_proc = mp.Process(target=self.coll_news_proc, args=(chanl,))
            #coll_proc.start()
            #procs.append(coll_proc)
        pool.close()
        pool.join()
        #for i in procs:
        #    i.join()
        join_file(chnl_file, os.path.join(self.save_dir, '/data.txt'))
        t1 = datetime.datetime.now()
        logger.info("coll_news_handler finished!, it takes {}s".format((t1 - t0).total_seconds()))


def coll_news():
    coll_news_handler(save_dir, doc_num_per_chnl, doc_min_len)
    print '-----------finish------'












