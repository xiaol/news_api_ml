# -*- coding: utf-8 -*-
# @Time    : 17/1/4 上午10:10
# @Author  : liulei
# @Brief   : 离线更新kmeans模型版本
# @File    : kmeans.py
# @Software: PyCharm Community Edition

import os
import datetime
import graphlab as gl
from util import doc_process
from multiprocessing import Process
import traceback

from util.doc_process import cut_pos_ltp
#添加日志
from util.logger import Logger
real_dir_path = os.path.split(os.path.realpath(__file__))[0]
logger_update = Logger('kmeans_update', os.path.join(real_dir_path, 'log/update.log'))

#定义全局变量
data_dir = os.path.join(real_dir_path, 'data')
kmeans_model_save_dir = os.path.join(real_dir_path, 'models', '000')
if not os.path.exists(kmeans_model_save_dir):
    os.mkdir(kmeans_model_save_dir)
g_channel_kmeans_model_dict = {}

chnl_k_dict = {'财经':20, '股票':10, '故事':20, '互联网':20, '健康':30, '军事':20,
               '科学':20, '历史':30, '旅游':20, '美食':20, '美文':20, '萌宠':20,
               '汽车':30, '时尚':30, '探索':30, '外媒':30, '养生':30, '影视':30,
               '游戏':40, '育儿':20,
               '体育':20, '娱乐':10, '社会':10, '科技':12, '国际':5}

chnl_k_dict = {'财经':20, '股票':10, '故事':20, '互联网':20, '健康':30, '军事':20,
               '科学':20, '历史':30, '旅游':20, '美食':20, '美文':20, '萌宠':20,
               '汽车':30, '时尚':30, '探索':10, '外媒':30, '养生':30, '影视':30,
               '游戏':30, '育儿':20,'体育':20, '娱乐':10, '社会':10,'科技':12,
               '国际':5, '美女': 1, '搞笑': 1, '趣图':1, '风水玄学':10, '本地':10,
               '自媒体':20, '奇闻':20}


#chnl_newsnum_dict = {'财经':20000, '股票':10000, '故事':10000, '互联网':20000, '健康':20000, '军事':10000,
#                     '科学':10000, '历史':10000, '旅游':10000, '美食':10000, '美文':10000, '萌宠':20000,
#                     '汽车':20000, '时尚':20000, '探索':1500, '外媒':10000, '养生':20000, '影视':20000,
#                     '游戏':20000, '育儿':20000, '体育':20000, '娱乐':20000, '社会':30000, '科技':20000,
#                     '国际':20000,'美女': 100, '搞笑': 100, '趣图':100, '风水玄学':10000, '本地':20000,
#                     '自媒体':10000, '奇闻':10000}
chnl_newsnum_dict = {'财经':50, '股票':50, '美女':50}

#创建新版本模型子进程
def create_kmeans_core(chname, docs, model_save_dir):
    try:
        global g_channel_kmeans_model_dict
        #logger.info('---begin to deal with {}'.format(chname))
        print 'begin to create kmeans model for {}'.format(chname)
        trim_sa = gl.text_analytics.trim_rare_words(docs, threshold=5, to_lower=False)
        docs_trim = gl.text_analytics.count_words(trim_sa)
        model = gl.kmeans.create(gl.SFrame(docs_trim),
                                 num_clusters=chnl_k_dict[chname],
                                 max_iterations=200)
        g_channel_kmeans_model_dict[chname] = model
        #save model to file
        model.save(model_save_dir+'/'+chname)
        print 'create kmeans model for {} finish'.format(chname)
    except:
        traceback.print_exc()


#创建新版本的模型
def create_new_kmeans_model():
    try:
        t0 = datetime.datetime.now()
        global kmeans_model_save_dir,  model_v
        model_create_time = datetime.datetime.now()
        time_str = model_create_time.strftime('%Y-%m-%d-%H-%M-%S')
        data_dir_v = os.path.join(data_dir, time_str)
        model_v = os.path.join(kmeans_model_save_dir,  time_str)
        if not os.path.exists(model_v):
            os.mkdir(model_v)
            logger_update.info('create kmeans models {}'.format(time_str))
        if not os.path.exists(data_dir_v):
            os.mkdir(data_dir_v)

        from util.doc_process import coll_cut_extract
        coll_cut_extract(chnl_newsnum_dict, data_dir_v, os.path.join(data_dir_v, 'idf.txt'))
        news = gl.SFrame.read_csv(os.path.join(data_dir_v, 'cut_extract.csv'))
        chnls = news['chnl']  #SArray类型
        nids = news['nid']
        docs = news['doc']
        chnl_doc_dict = dict()
        #提取各个频道的新闻
        for i in xrange(chnls.size()):
            if chnls[i] not in chnl_doc_dict:
                print chnls[i]
                chnl_doc_dict[chnls[i]] = []
            chnl_doc_dict[chnls[i]].append(docs[i])
        #单进程训练
        for item in chnl_doc_dict.items():
            print type(item[1])
            create_kmeans_core(item[0], gl.SArray(item[1]), model_v)
        t1 = datetime.datetime.now()
        time_cost = (t1 - t0).seconds
        print 'create models finished!! it cost ' + str(time_cost) + '\'s'
    except:
        traceback.print_exc()



def get_newest_model_dir():
    global kmeans_model_save_dir
    models = os.listdir(kmeans_model_save_dir)
    if len(models) == 0:
        return ''
    ms = {}
    for m in models:
        ms[m] = m.replace('-', '')
    ms_sort = sorted(ms.items(), key=lambda x:x[1])
    return os.path.join(kmeans_model_save_dir, ms_sort.pop()[0])

#初始化模型版本号为最新的保存的模型
model_v = os.path.split(get_newest_model_dir())[1]

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
    conn, cursor = doc_process.get_postgredb_query()
    cursor.execute(chname_id_sql)
    rows = cursor.fetchall()
    for r in rows:
        chname_id_dict[r[1]] = r[0]
    cursor.close()
    conn.close()


def kmeans_predict(nid_list):
    global g_channel_kmeans_model_dict, chname_id_dict
    print "****************************************************"  + model_v
    if len(g_channel_kmeans_model_dict) == 0:
        load_newest_models()
    if (len(chname_id_dict)) == 0:
        get_chname_id_dict()
    nid_info = {}
    for nid in nid_list:
        conn, cursor = doc_process.get_postgredb_query()
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
                txt += content['txt']
        total_txt = title + txt.encode('utf-8')
        #word_list = doc_process.filter_html_stopwords_pos(total_txt, remove_num=True, remove_single_word=True)
        word_list = cut_pos_ltp(total_txt)
        nid_info[nid] = [chanl_name, ' '.join(word_list)]
        cursor.close()
        conn.close()

    clstid_nid_dict = {}
    for chname in g_channel_kmeans_model_dict.keys():
        print 'predict ---- ' + chname
        nids = []
        doc_list = []
        for nid in nid_info.keys():
            if nid_info[nid][0] == chname:
                nids.append(nid)
                doc_list.append(nid_info[nid][1])

        print 'news num of ' + chname + ' is ' + str(len(nids))
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
        for i in xrange(0, len(pred)):
            if pred[i] not in clstid_nid_dict.keys():
                clstid_nid_dict[pred[i]] = []
            clstid_nid_dict[pred[i]].append(nids[i])
    #print clstid_nid_dict
    return clstid_nid_dict








