# -*- coding: utf-8 -*-
# @Time    : 16/8/8 下午4:58
# @Author  : liulei
# @Brief   : 从postgre 获取新闻并众多文本的预处理
# @File    : DocPreProcess.py
# @Software: PyCharm Community Edition
from os.path import basename
import jieba
import re
import sys
import psycopg2
import shutil


import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('log.txt')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

reload(sys)
sys.setdefaultencoding('utf8')
import os

DOC_NUM = 100 #总数据集
TRAIN_DOC = 100 #训练集

POSTGRE_USER = 'postgres'
POSTGRE_PWD = 'ly@postgres&2015'
POSTGRE_HOST = '120.27.163.25'
POSTGRE_DBNAME = 'BDP'
POSTGRES = "postgresql://postgres:ly@postgres&2015@120.27.163.25/BDP"
def get_postgredb():
    connection = psycopg2.connect(database=POSTGRE_DBNAME, user=POSTGRE_USER, password=POSTGRE_PWD, host=POSTGRE_HOST,)
    cursor = connection.cursor()
    return connection, cursor


#category_dict = {'汽车':'automobile', '娱乐':'entertainment', '财经':'finance', '美食':'food', '游戏':'game',
#                 '健康':'health', '军事':'militry', '科技':'science', '体育':'sports', '旅游':'travel'}
category_dict = {'科技':'technology', '外媒':'MediaAbroad', '社会':'society', '财经':'finance', '体育':'sports',
                 '汽车':'aotumobile', '国际':'international', '时尚':'fashion','探索':'discovery',
                 '科学':'science', '娱乐':'entertainment', '趣图':'fannypicture', '搞笑':'fanny', '养生':'healthcare',
                 '育儿':'childbearing', '股票':'stock', '互联网':'internet',
                 '美食':'food', '健康':'health', '影视':'movie', '奇闻':'odd', '军事':'military', '历史':'history',
                 '故事':'story', '旅游':'travel', '美文':'essay',
                 '萌宠':'pet', '游戏':'games', '美女':'beauty', '广告':'Ads'}
category_list = [u'科技', u'外媒', u'社会', u'财经', u'体育', u'汽车', u'国际', u'时尚',u'探索', u'科学',
                 u'娱乐', u'趣图', u'搞笑', u'养生', u'育儿', u'股票', u'互联网', u'美食', u'健康', u'影视',
                 u'奇闻', u'军事', u'历史', u'故事', u'旅游', u'美文', u'萌宠', u'游戏', u'美女', u'广告']

#手工选出的广告新闻
ad_nids = [5663445, 5663406, 3945020, 5709777, 3961793, 5693457, 4727101, 5513815, 4851274, 5578330,
           5049199, 4692648, 5094801, 5392324, 3994872, 4695941, 5192098, 5439278, 3475114, 3785035,
           4772786, 4032398, 3507621, 4793037, 5559949, 5715116, 3971649, 3926523, 4954757, 4981211,
           5406800, 5512677, 4567062, 3914242, 4872578, 4264963, 3902868, 5577327, 3564736, 5273589,
           5556793, 3529501, 3729809, 4789485, 5116095, 5078288, 4891491, 5574459, 5023394, 3860209,
           3588747, 3784503, 3973070, 4245991, 5099896, 5281591, 5068612, 5155247, 5448065, 3620326,
           4656018, 4928678, 4977714, 5214908, 5373329, 3190273, 3213072, 3526561, 5138707, 5320321,
           3604645, 5134496, 5240866, 5466346, 4565071, 4825802, 4952450, 3887672, 5233347, 5417679,
           5487921, 4696618, 3729552, 3895200, 5011992, 5044157, 5610834, 5705926, 3127365, 3607194,
           4791877, 4032237, 5458201, 5545768, 3675194, 5239236, 5657210, 4824631, 5589556, 4042448]

ad_nids2 =[5524442, 4808083,4736576,4785403,4760140, 5796932, 5052048, 5038690,5500876, 5684128, 5463030,
           4962484, 5082874, 5710311, 5709832]

news_file_path = './NewsFile/'
news_cut_file_path = './NewsFileCut/'
idf_file = './result/idf.txt'
#ads_path = './AdsFile/'
#ads_cut_path = './AdsFileCut/'

channle_sql ='SELECT a.title,a.content, a.nid, c.cname \
FROM \
(select * from newslist_v2 where nid >= %s and nid <=%s) a \
RIGHT OUTER JOIN (select * from channellist_v2 where "cname"=%s) c \
ON \
a.chid=c."id" LIMIT 300'

#####################################广告处理部分 start
#def writeAdNidToFile():
#    ad_nids_file = open(ads_nids_file_name, 'w')
#    for item in ad_nids:
#        ad_nids_file.write(str(item))
#        ad_nids_file.write('\n')

def getAdsFile():
    #writeAdNidToFile()
    ad_path = news_file_path + '广告'
    if not os.path.exists(ad_path):
        os.mkdir(ad_path)

    ad_nids_tuple = tuple(ad_nids)
    ads_str = str(ad_nids_tuple)
    sql = "select title, content, nid from newslist_v2 where nid in {0}"
    conn, cursor = get_postgredb()
    cursor.execute(sql.format(ads_str))
    rows = cursor.fetchall()

    num = 0
    for row in rows:
        ad_file_full_path = news_file_path + '广告' + '/' + str(num) + '.txt'
        title = row[0]
        content_list = row[1]
        writeNewsToFile(ad_file_full_path, title, content_list)
        num += 1
#####################################广告处理部分 end
sql_nid = 'select nid, title, content from newslist_v2 where nid={0}'
def getTextOfNewsNid(nid):
    conn, cursor = get_postgredb()
    cursor.execute(sql_nid.format(str(nid)))
    rows = cursor.fetchall()
    text = ''
    for row in rows:
        title = row[1]
        content_list = row[2]
        content = ''
        for elems in content_list:
            if 'txt' in elems.keys():
                content += elems['txt'] + ' '
        text += title + ' '
        text += content
    return text

#将新闻的title和content的txt部分写入文件
def writeNewsToFile(filePath, title, content_list):
    file = open(filePath, 'w')
    file.write(title)
    for elems in content_list:
        if 'txt' in elems.keys():
            file.write(elems['txt'])
    file.close()

#准备新闻数据
def collectNews(start_id, end_id, category):
    conn, cursor = get_postgredb()
    cursor.execute(channle_sql, [start_id, end_id, category])
    rows = cursor.fetchall()
    num = 0
    for row in rows:
        if num >= DOC_NUM:
            break
        if row[2] in ad_nids:
            print 'get ad nid = ' + str(row[2]) + '. skip!'
            continue
        title = row[0]
        content_list = row[1]
        full_path = news_file_path + category + '/' + str(num) + '.txt'
        writeNewsToFile(full_path, title, content_list)
        num += 1
    conn.close()

##过滤HTML中的标签
# 将HTML中标签等信息去掉
# @param htmlstr HTML字符串.
def filter_tags(htmlstr):
    # 先过滤CDATA
    re_cdata = re.compile('//<!\[CDATA\[[^>]*//\]\]>', re.I)  # 匹配CDATA
    re_script = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I)  # Script
    re_style = re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.I)  # style
    re_br = re.compile('<br\s*?/?>')  # 处理换行
    re_h = re.compile('</?\w+[^>]*>')  # HTML标签
    re_comment = re.compile('<!--[^>]*-->')  # HTML注释
    s = re_cdata.sub('', htmlstr)  # 去掉CDATA
    s = re_script.sub('', s)  # 去掉SCRIPT
    s = re_style.sub('', s)  # 去掉style
    s = re_br.sub('\n', s)  # 将br转换为换行
    s = re_h.sub('', s)  # 去掉HTML 标签
    s = re_comment.sub('', s)  # 去掉HTML注释
    # 去掉多余的空行
    blank_line = re.compile('\n+')
    s = blank_line.sub('\n', s)
    s = replaceCharEntity(s)  # 替换实体
    return s

##替换常用HTML字符实体.
# 使用正常的字符替换HTML中特殊的字符实体.
# 你可以添加新的实体字符到CHAR_ENTITIES中,处理更多HTML字符实体.
# @param htmlstr HTML字符串.
def replaceCharEntity(htmlstr):
    CHAR_ENTITIES = {'nbsp': ' ', '160': ' ',
                     'lt': '<', '60': '<',
                     'gt': '>', '62': '>',
                     'amp': '&', '38': '&',
                     'quot': '"', '34': '"',}

    re_charEntity = re.compile(r'&#?(?P<name>\w+);')
    sz = re_charEntity.search(htmlstr)
    while sz:
        entity = sz.group()  # entity全称，如&gt;
        key = sz.group('name')  # 去除&;后entity,如&gt;为gt
        try:
            htmlstr = re_charEntity.sub(CHAR_ENTITIES[key], htmlstr, 1)
            sz = re_charEntity.search(htmlstr)
        except KeyError:
            # 以空串代替
            htmlstr = re_charEntity.sub('', htmlstr, 1)
            sz = re_charEntity.search(htmlstr)
    return htmlstr


def repalce(s, re_exp, repl_string):
    return re_exp.sub(repl_string, s)

#提取关键词
def strProcess(str):
    #删除html标签
    txt_no_html = filter_tags(str)
    jieba.load_userdict('./util/networds.txt')
    jieba.analyse.set_stop_words('./util/stopwords.txt')
    jieba.analyse.set_idf_path(idf_file)
    words = jieba.analyse.extract_tags(txt_no_html, 30)
    return words

#去除html标签及停用词
def strProcess2(str):
    #删除html标签
    txt_no_html = filter_tags(str)
    jieba.load_userdict('./util/networds.txt')
    words=jieba.cut(txt_no_html)
    stopwords = {}.fromkeys([line.rstrip() for line in open('./util/stopwords.txt')])
    final_words = []
    for i in words:
        if not i.isspace() and (i not in stopwords.keys()):
            #print i
            final_words.append(i)
    return final_words

def cutAndRemoveUseless(file_path, new_file_path):
    orgFile=open(file_path, 'r')
    file_name = basename(file_path)
    portion = os.path.splitext(file_name)
    new_file_name =portion[0] + '.cut'
    file_full_path = new_file_path + new_file_name
    print file_full_path
    if (os.path.exists(file_full_path)):
        return
    txtlist=orgFile.read()
    orgFile.close()
    words = strProcess(txtlist)
    result = ''
    f = open(new_file_path + new_file_name, 'w')
    for w in words:
        f.write(w + ' ')

    f.close()


#词性(POS, part of speech)筛选
# 保留: 名词，名词短语（两者为评论描述主题）
#　　　　形容词，动词，动词短语（对主题的描述）以及其他可能有实意的词
#去除: 副词，标点,拟声词等无实意词包括/x /zg /uj /ul /e /d /uz /y
def filterPOS(filePath, resultFilePath):
    f = open(filePath)
    txt = f.readlines()
    f.close()
    txtlist=[]
    POS = ['/x', '/zg', '/uj', 'ul', '/e', '/d', '/uz', '/y']
    #去除特定词性的词
    for line in txt:
        line_list2 = re.split('[ ]', line)
        line_list = line_list2[:]
        for segs in line_list2:
            for K in POS:
                if K in segs:
                    line_list.remove(segs)
                    break
                else:
                    pass
        txtlist.extend(line_list)
    f2 = open(resultFilePath, 'a')
    resultlist = txtlist[:]
    #把筛选后的词写入新文件
    for v in txtlist:
        if '/' in v:
            slope = v.index('/')
            letter = v[0:slope] + ' '
            f2.write(letter)
        else:
            f2.write(v)

#去除空行,空白字符等等
def removeSpace(inFilePath, outFilePath):
    f1 = open(inFilePath, 'r+')
    f2 = open(outFilePath, 'a')
    txt = f1.readlines()
    f1.close()
    list1=[]
    for line in txt:
        if not line.split():
            line_clean = ' '.join(line.split())
            lines = line_clean + ' ' + '\n'
            f2.write(lines)
        else:
            pass
    f2.close()

def clearDir(dirPath):
    if os.path.isdir(dirPath):
        for item in os.listdir(dirPath):
            itemsrc=os.path.join(dirPath, item)
            shutil.rmtree(itemsrc)

#收集所有类型的新闻数据,产生新闻文件
def collectEveryCatagoryNews(start_id, end_id):
    clearDir(news_file_path)
    #category = category_dict.values()
    category = category_list  #所有类别,汉字
    for cata in category:
        if cata == '广告':
            getAdsFile()
        else:
            cate_path = news_file_path + cata
            if not os.path.exists(cate_path):
                os.mkdir(cate_path)
            collectNews(start_id, end_id, cata)

#将原始的新闻数据进行预处理后保存
def preProcessNews():
    clearDir(news_cut_file_path)
    #categories = category_dict.values()
    categories = category_list
    for cate in categories:
        cate_cut_path = news_cut_file_path + cate
        if not os.path.exists(cate_cut_path):
            os.mkdir(cate_cut_path)
        cate_path = news_file_path + cate
        if not os.path.exists(cate_path):
            logger.error('Path does not exit: ' + cate_path)
        file_names = os.listdir(cate_path)
        for fn in file_names:
            file_path = cate_path + '/' + fn
            cutAndRemoveUseless(file_path, cate_cut_path + '/')

def docPreProcess():
    logger.info('docPreProcess begin...')
    start_id = 3500000
    end_id = 4600000
    collectEveryCatagoryNews(start_id, end_id)
    preProcessNews()
    logger.info('docPreProcess done!')

if __name__ == '__main__':
    os.chdir('../')
    start_id = 3500000
    end_id = 3580000
    #----- train -----
    #collectEveryCatagoryNews(start_id, end_id)
    #preProcessNews()
    #----- test -----


