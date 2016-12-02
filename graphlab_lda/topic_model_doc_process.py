# -*- coding: utf-8 -*-
# @Time    : 16/12/1 下午5:23
# @Author  : liulei
# @Brief   : 
# @File    : topic_model_doc_process.py
# @Software: PyCharm Community Edition
import os
from util import doc_process
channle_sql ='SELECT a.title,a.content, a.nid, c.cname \
FROM newslist_v2_back a \
RIGHT OUTER JOIN (select * from channellist_v2 where "cname"=%s) c \
ON \
a."chid"=c."id" LIMIT %s'

real_dir_path = os.path.split(os.path.realpath(__file__))[0] #文件所在路径

#准备新闻数据
def collectNews(category, news_num, min_len=100):
    print category
    conn, cursor = doc_process.get_postgredb()
    cursor.execute(channle_sql, [category, news_num])
    rows = cursor.fetchall()
    with open(real_dir_path+'/data/'+category, 'w') as f:
        f.close()
    for row in rows:
        title = row[0]
        content_list = row[1]
        txt = ''
        for content in content_list:
            if 'txt' in content.keys():
                txt += content['txt']
        txt_list = doc_process.filter_html_stopwords_pos(txt, remove_num=True, remove_single_word=True)
        title_list = doc_process.filter_html_stopwords_pos(title, remove_num=True, remove_single_word=True)
        if len(txt_list) + len(title_list) < min_len:
            continue
        with open(real_dir_path+'/data/'+category, 'a') as f:
            for w in title_list:
                f.write(w.encode('utf-8') + ' ')
            f.write(' ')
            for w in txt_list:
                f.write(w.encode('utf-8') + ' ')
            f.write('\n')

    conn.close()


channel_for_topic = ['科技', '外媒', '社会', '财经', '体育', '汽车', '国际', '时尚', '探索', '科学',
                     '娱乐', '养生', '育儿', '股票', '互联网', '美食', '健康', '影视', '军事', '历史',
                     '故事', '旅游', '美文', '萌宠', '游戏']
import multiprocessing as mp
def coll_news_for_channles():
    for chanl in channel_for_topic:
        coll_proc = mp.Process(target=collectNews, args=(chanl, 3000, 100))
        coll_proc.start()



