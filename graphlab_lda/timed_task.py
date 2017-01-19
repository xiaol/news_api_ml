# -*- coding: utf-8 -*-
# @Time    : 16/12/23 上午10:37
# @Author  : liulei
# @Brief   : 定时任务
# @File    : timed_task.py
# @Software: PyCharm Community Edition

from util.doc_process import get_postgredb
from redis_lda import produce_user_click
from topic_model_doc_process import channel_for_topic
from graphlab_kmeans.kmeans import chnl_k_dict

#定义取用户点击的循环周期
period = 30
#click_sql = "select uid, nid, ctime from newsrecommendclick where ctime > now() - INTERVAL '5 minute'"
click_sql = "select c.uid, c.nid, c.ctime from newsrecommendclick c \
inner join newslist_v2 nl  on c.nid=nl.nid \
INNER JOIN channellist_v2 cl on nl.chid = cl.id \
where cname in ({0}) and c.ctime > now() - INTERVAL '{1} second' "
#and \
#(c.uid, c.nid) not in ( \
#select c2.uid, c2.nid from newsrecommendclick c2 \
#where c2.ctime < now() - interval ' second' and c2.ctime > now() - INTERVAL '1 day') "

channels = ', '.join("\'" + ch+"\'" for ch in chnl_k_dict.keys())
def get_clicks_5m():
    conn, cursor = get_postgredb()
    cursor.execute(click_sql.format(channels, period))
    rows = cursor.fetchall()
    for r in rows:
        ctime_str = r[2].strftime('%Y-%m-%d %H:%M:%S')
        produce_user_click(r[0], r[1], ctime_str)
    cursor.close()
    conn.close()


