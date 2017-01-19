# -*- coding: utf-8 -*-
# @Time    : 17/1/4 上午10:10
# @Author  : liulei
# @Brief   : 使用kmeans 算法,为频道内推荐提供比主题模型更粗颗粒度的数据
# @File    : kmeans.py
# @Software: PyCharm Community Edition

import os
import datetime
import graphlab as gl
from util import doc_process
from multiprocessing import Process

#添加日志
import logging
real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger = logging.getLogger(__name__)
handler = logging.FileHandler(os.path.join(real_dir_path, '../log/kmeans/kmeans.log'), 'w')
formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

#定义全局变量
data_dir = os.path.join(real_dir_path, '..', 'graphlab_lda', 'data')
real_dir_path = os.path.split(os.path.realpath(__file__))[0]
kmeans_model_save_dir = real_dir_path + '/' + 'models/'
if not os.path.exists(kmeans_model_save_dir):
    os.mkdir(kmeans_model_save_dir)
g_channel_kmeans_model_dict = {}
#chnl_k_dict = {'体育':20, '娱乐':10, '社会':10, '科技':12, '国际':5}
#chnl_k_dict = {'体育':20}
#chnl_k_dict = {'娱乐':20, '社会':20, '国际':10}
#chnl_k_dict = {'体育':20, '娱乐':20, '社会':20, '科技':20, '国际':10}
chnl_k_dict = {'财经':20, '股票':10, '故事':20, '互联网':20, '健康':30, '军事':20,
               '科学':20, '历史':30, '旅游':20, '美食':20, '美文':20, '萌宠':20,
               '汽车':30, '时尚':30, '探索':30, '外媒':30, '养生':30, '影视':30,
               '游戏':40, '育儿':20}


def get_newest_model_dir():
    global kmeans_model_save_dir, kmeans_model_save_dir
    models = os.listdir(kmeans_model_save_dir)
    ms = {}
    for m in models:
        ms[m] = m.replace('-', '')
    ms_sort = sorted(ms.items(), key=lambda x:x[1])
    return kmeans_model_save_dir + ms_sort.pop()[0]

#初始化模型版本号为最新的保存的模型
model_v = os.path.split(get_newest_model_dir())[1]
def create_model_proc(chname, model_save_dir=None):
    if chname not in chnl_k_dict.keys():
        return
    global g_channle_kmeans_model_dict, data_dir
    print '******************{}'.format(chname)
    docs = gl.SFrame.read_csv(os.path.join(data_dir, chname), header=False)
    trim_sa = gl.text_analytics.trim_rare_words(docs['X1'], threshold=5, to_lower=False)
    docs = gl.text_analytics.count_words(trim_sa)
    model = gl.kmeans.create(gl.SFrame(docs), num_clusters=chnl_k_dict[chname],
                             max_iterations=200)
    print 'create kmeans model for {} finish'.format(chname)
    g_channel_kmeans_model_dict[chname] = model
    #save model to file
    if model_save_dir:
        model.save(model_save_dir+'/'+chname)

def create_kmeans_models():
    global kmeans_model_save_dir, g_channle_kmeans_model_dict, model_v
    model_create_time = datetime.datetime.now()
    time_str = model_create_time.strftime('%Y-%m-%d-%H-%M-%S')
    model_v = kmeans_model_save_dir + time_str
    if not os.path.exists(model_v):
        os.mkdir(model_v)
        logger.info('create kmeans models {}'.format(time_str))

    t0 = datetime.datetime.now()
    for chanl in chnl_k_dict.keys():
        create_model_proc(chanl, model_v) #只能单进程。 gl的数据结构(SFrame等)无法通过传递给子进程
    t1 = datetime.datetime.now()
    time_cost = (t1 - t0).seconds
    print 'create models finished!! it cost ' + str(time_cost) + '\'s'


def load_models(models_dir):
    print 'load_models()'
    global g_channel_kmeans_model_dict, model_v
    import os
    model_v = os.path.split(models_dir)[1]
    if len(g_channel_kmeans_model_dict) != 0:
        g_channel_kmeans_model_dict.clear()
    models_files = os.listdir(models_dir)
    for mf in models_files:
        print '    load ' + mf
        print models_dir
        g_channel_kmeans_model_dict[mf] = gl.load_model(models_dir + '/'+ mf)


def load_newest_models():
    load_models(get_newest_model_dir())

###############################################################################
#@brief  :预测新数据
#@input  :
###############################################################################
nid_sql = 'select a.title, a.content, c.cname \
from (select * from newslist_v2 where nid=%s) a \
inner join channellist_v2 c on a."chid"=c."id"'

chname_id_dict = {}
def get_chname_id_dict():
    global chname_id_dict
    chname_id_sql = "select id, cname from channellist_v2"
    conn, cursor = doc_process.get_postgredb()
    cursor.execute(chname_id_sql)
    rows = cursor.fetchall()
    for r in rows:
        chname_id_dict[r[1]] = r[0]
    cursor.close()
    conn.close()


insert_sql = "insert into news_kmeans  (nid, model_v, ch_name, cluster_id, chid, ctime) VALUES ({0}, '{1}', '{2}', {3}, {4}, '{5}')"
def kmeans_predict(nid_list):
    global g_channel_kmeans_model_dict, chname_id_dict
    print "****************************************************"  + model_v
    if len(g_channel_kmeans_model_dict) == 0:
        load_newest_models()
    if (len(chname_id_dict)) == 0:
        get_chname_id_dict()
    nid_info = {}
    for nid in nid_list:
        conn, cursor = doc_process.get_postgredb()
        cursor.execute(nid_sql, [nid])
        row = cursor.fetchone()
        if not row:
            print 'Error: do not get info of nid: ' + str(nid)
            continue
        title = row[0]
        content_list = row[1]
        chanl_name = row[2]

        if chanl_name not in g_channel_kmeans_model_dict:
            continue

        txt = ''
        for content in content_list:
            if 'txt' in content.keys():
                txt += content['txt'].encode('utf-8')
        total_txt = title + txt
        word_list = doc_process.filter_html_stopwords_pos(total_txt, remove_num=True, remove_single_word=True)
        nid_info[nid] = [chanl_name, ' '.join(word_list)]
        cursor.close()
        conn.close()

    #clstid_nid_dict = {}
    for chname in g_channel_kmeans_model_dict.keys():
        print 'predict ---- ' + chname
        nids = []
        doc_list = []
        for nid in nid_info.keys():
            if nid_info[nid][0] == chname:
                nids.append(nid)
                doc_list.append(nid_info[nid][1])

        print 'news num of ' + chname + ' is ' + str(len(chname))
        if len(nids) == 0:
            continue
        ws = gl.SArray(doc_list)
        docs = gl.SFrame(data={'X1': ws})
        docs = gl.text_analytics.count_words(docs['X1'])
        docs = gl.SFrame(docs)
        pred = g_channel_kmeans_model_dict[chname].predict(docs, output_type = 'cluster_id')
        if len(nids) != len(pred):
            print 'len(nids) != len(pred)'
            return
        conn, cursor = doc_process.get_postgredb()
        for i in xrange(0, len(pred)):
            #入库
            now = datetime.datetime.now()
            time_str = now.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(insert_sql.format(nids[i], model_v, chname, pred[i], chname_id_dict[chname], time_str))

            #if pred[i] not in clstid_nid_dict.keys():
            #    clstid_nid_dict[pred[i]] = []
            #    clstid_nid_dict[pred[i]].append(nids[i])
            #else:
            #    clstid_nid_dict[pred[i]].append(nids[i])
        conn.commit()
        cursor.close()
        conn.close()
    #print clstid_nid_dict
    #return clstid_nid_dict


#nt_sql = "select ch_name, cluster_id from news_kmeans where nid = {0} and model_v = '{1}' "
nt_sql = "select ch_name, cluster_id, model_v from news_kmeans where nid = {0}"
ut_sql = "select times from user_kmeans_cluster where uid = {0} and model_v = '{1}' and ch_name = '{2}' and cluster_id ='{3}' "
ut_update_sql = "update user_kmeans_cluster set times='{0}', create_time = '{1}', fail_time='{2}' where " \
                "uid='{3}' and model_v = '{4}' and ch_name = '{5}' and cluster_id='{6}'"
user_topic_insert_sql = "insert into user_kmeans_cluster (uid, model_v, ch_name, cluster_id, times, create_time, fail_time, chid) " \
                        "VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}')"
################################################################################
#@brief : 添加用户点击
################################################################################
from datetime import timedelta
def predict_click(click_info):
    global chname_id_dict
    if (len(chname_id_dict)) == 0:
        get_chname_id_dict()
    uid = click_info[0]
    nid = click_info[1]
    time_str = click_info[2]
    print 'consume ' + str(uid) + ' ' + str(nid) + ' '+time_str
    ctime = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    valid_time = ctime + timedelta(days=15) #有效时间定为30天
    fail_time = valid_time.strftime('%Y-%m-%d %H:%M:%S')
    conn, cursor = doc_process.get_postgredb()
    cursor.execute(nt_sql.format(nid)) #获取nid可能的话题
    rows = cursor.fetchall()
    for r in rows:
        ch_name = r[0]
        cluster_id = r[1]
        local_model_v = r[2]
        conn2, cursor2 = doc_process.get_postgredb()
        cursor2.execute(ut_sql.format(uid, local_model_v, ch_name, cluster_id))
        rows2 = cursor2.fetchone()
        if rows2: #该用户已经关注过该topic_id, 更新probability即可
            times = 1 + rows2[0]
            print "update '{0}' '{1}' '{2}' '{3}' '{4}' '{5}' '{6}'".format(times, time_str, fail_time, uid, model_v, ch_name, cluster_id)
            cursor2.execute(ut_update_sql.format(times, time_str, fail_time, uid, local_model_v, ch_name, cluster_id))
        else:
            cursor2.execute(user_topic_insert_sql.format(uid, local_model_v, ch_name, cluster_id, '1', time_str, fail_time, chname_id_dict[ch_name]))
        conn2.commit()
        conn2.close()
    cursor.close()
    conn.close()







