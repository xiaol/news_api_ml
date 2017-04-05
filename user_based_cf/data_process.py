# -*- coding: utf-8 -*-
# @Time    : 17/4/5 下午1:56
# @Author  : liulei
# @Brief   : 协同过滤数据处理; user-based cf算法。 首先收集用户点击行为; 用户相似度矩阵每隔一两天就需要更新
# @File    : data_process.py
# @Software: PyCharm Community Edition
import os
import traceback
from util import logger
import pandas as pd
from util.doc_process import get_postgredb_query
import datetime

real_dir_path = os.path.split(os.path.realpath(__file__))[0] #文件所在路径
log_cf = logger.Logger('log_cf', os.path.join(real_dir_path, 'log', 'log.txt'))

click_query_sql = "select uid, nid, ctime from newsrecommendclick where ctime > now() - interval '%s day'"
#收集用户一段时间内的的点击行为
def coll_click():
    try:
        conn, cursor = get_postgredb_query()
        cursor.execute(click_query_sql, (30,))
        rows = cursor.fetchall()
        user_ids = []
        nid_ids = []
        click_time = []
        for r in rows:
            user_ids.append(r[0])
            nid_ids.append(r[1])
            click_time.append(r[2])

        df = pd.DataFrame({'uid':user_ids, 'nid':nid_ids, 'ctime':click_time}, columns=('uid', 'nid', 'ctime'))
        time_str = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        data_save_path = os.path.join(real_dir_path, 'data', time_str)
        df.to_csv(data_save_path, index=False)
        conn.close()
    except:
        log_cf.exception(traceback.format_exc())
        conn.close()



if __name__ == '__main__':
    coll_click()





