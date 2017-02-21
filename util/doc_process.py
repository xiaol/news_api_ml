# -*- coding: utf-8 -*-
# @Time    : 16/12/1 上午11:15
# @Author  : liulei
# @Brief   : 
# @File    : doc_process.py
# @Software: PyCharm Community Edition
import os
import re
import jieba
import jieba.analyse
import time

##过滤HTML中的标签
# 将HTML中标签等信息去掉
# @param htmlstr HTML字符串.
import psycopg2
import traceback

sentence_delimiters = ['?', '!', ';', '？', '！', '。', '；', '……', '…', '\n']
question_delimiters = [u'?', u'？']
news_text_nid_sql = "select nid, title, content from newslist_v2 where nid={0}"



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

def filter_url(str):
    if str.find('www') < 0:
        return str
    myString_list = [item for item in str.split(" ")]
    url_list = []
    for item in myString_list:
        try:
            url_list.append(re.search("(?P<url>https?://[^\s]+)", item).group("url"))
        except:
            pass
    for i in url_list:
        str = str.replace(i, ' ')
    return str

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


def get_file_real_path():
    return os.path.realpath(__file__)


def get_file_real_dir_path():
    return os.path.split(os.path.realpath(__file__))[0]

real_dir_path = os.path.split(os.path.realpath(__file__))[0] #文件所在路径
net_words_file = real_dir_path + '/networds.txt'
stop_words_file = real_dir_path + '/stopwords.txt'
#去除html标签及停用词
def remove_html_and_stopwords(str):
    #删除html标签
    txt_no_html = filter_tags(str)
    jieba.load_userdict(net_words_file)
    words = jieba.cut(txt_no_html)  #unicode is returned
    stopwords = {}.fromkeys([line.rstrip() for line in open(stop_words_file)]) #utf-8
    stopwords_set = set(stopwords)
    final_words = []
    for w in words:
        if not w.encode('utf-8') in stopwords_set and (not w.isspace()):
            final_words.append(w)
    return final_words

def filterPOS2(org_text, pos_list):
    txtlist = []
    # 去除特定词性的词
    for w in org_text:
        if w.flag in pos_list:
            pass
        else:
            txtlist.append(w.word)
    return txtlist

POS = ['zg', 'uj', 'ul', 'e', 'd', 'uz', 'y']
#去除html标签及停用词并筛选词性
def filter_html_stopwords_pos(str, remove_num=False, remove_single_word=False, return_no_html=False, remove_html=True):
    #删除html标签
    if remove_html:
        txt_no_html = filter_tags(str)
    else:
        txt_no_html = str
    import jieba.posseg
    jieba.load_userdict(net_words_file)
    words = jieba.posseg.cut(txt_no_html)  #unicode is returned
    words_filter = filterPOS2(words, POS)
    stopwords = {}.fromkeys([line.rstrip() for line in open(stop_words_file)]) #utf-8
    stopwords_set = set(stopwords)
    final_words = []
    for w in words_filter:
        if not w.encode('utf-8') in stopwords_set and (not w.isspace()):
            final_words.append(w)
    i = 0
    if remove_num == True:
        while i < len(final_words):
            w = final_words[i]
            if w.isdigit():
                final_words.remove(w)
                continue
            else:
                i += 1
    i = 0
    if remove_single_word == True:
        while i < len(final_words):
            w = final_words[i]
            if len(w) == 1:
                final_words.remove(w)
                continue
            else:
                i += 1

    if return_no_html:
        return txt_no_html, final_words
    return final_words


#jieba提取关键词
def jieba_extract_keywords(str, K):
    #删除html标签
    txt_no_html = filter_tags(str)
    jieba.load_userdict(net_words_file)
    jieba.analyse.set_stop_words(stop_words_file)
    words = jieba.analyse.extract_tags(txt_no_html, K)
    return words

POSTGRE_USER = 'postgres'
POSTGRE_PWD = 'ly@postgres&2015'
#POSTGRE_HOST = '120.27.163.25'
POSTGRE_HOST = '10.47.54.175'
POSTGRE_DBNAME = 'BDP'
#POSTGRES = "postgresql://postgres:ly@postgres&2015@120.27.163.25:5432/BDP"
POSTGRES = "postgresql://postgres:ly@postgres&2015@10.47.54.175:5432/BDP"
def get_postgredb():
    try:
        connection = psycopg2.connect(database=POSTGRE_DBNAME, user=POSTGRE_USER, password=POSTGRE_PWD, host=POSTGRE_HOST,)
        cursor = connection.cursor()
        return connection, cursor
    except:    #出现异常,再次连接
        try:
            time.sleep(2)
            connection = psycopg2.connect(database=POSTGRE_DBNAME, user=POSTGRE_USER, password=POSTGRE_PWD, host=POSTGRE_HOST,)
            cursor = connection.cursor()
            return connection, cursor
        except:
            traceback.print_exc()
            raise



nid_sql = 'select a.title, a.content, c.cname \
from (select * from newslist_v2 where nid=%s) a \
inner join channellist_v2 c on a."chid"=c."id"'
#获取nid的段落的字符串。 去除了html,但是没有去除停用词和特殊词性的词
def get_words_on_nid(nid):
    conn, cursor = get_postgredb()
    cursor.execute(nid_sql, [nid])
    rows = cursor.fetchall()
    word_list = []
    for row in rows:
        title = row[0]  #str类型
        content_list = row[1]
        txt = ''
        for content in content_list:
            if 'txt' in content.keys():
                txt += content['txt'].encode('utf-8')
        total_txt = title + txt
        word_list = filter_html_stopwords_pos(total_txt, remove_num=True, remove_single_word=True)
        #txt_no_html = filter_tags(total_txt)
        #word_list = remove_html_and_stopwords(total_txt)
    return word_list


# 检查某字符是否分句标志符号的函数；如果是，返回True，否则返回False
def FindToken(cutlist, char):
    if char in cutlist:
        return True
    else:
        return False

        # 进行分句的核心函数


# 设置分句的标志符号；可以根据实际需要进行修改
#cutlist = "。！？".decode('utf-8')
#cutlist = "。！？.!?。!?".decode('utf-8')
cutlist = "。！？!?。!?".decode('utf-8') #不包含英文句号,因为也会被当成小数点
def Cut(lines, cutlist=cutlist):  # 参数1：引用分句标志符；参数2：被分句的文本，为一行中文字符
    l = []  # 句子列表，用于存储单个分句成功后的整句内容，为函数的返回值
    line = []  # 临时列表，用于存储捕获到分句标志符之前的每个字符，一旦发现分句符号后，就会将其内容全部赋给l，然后就会被清空

    for i in lines:  # 对函数参数2中的每一字符逐个进行检查 （本函数中，如果将if和else对换一下位置，会更好懂）
        if FindToken(cutlist, i):  # 如果当前字符是分句符号
            line.append(i)  # 将此字符放入临时列表中
            l.append(''.join(line))  # 并把当前临时列表的内容加入到句子列表中
            line = []  # 将符号列表清空，以便下次分句使用
        else:  # 如果当前字符不是分句符号，则将该字符直接放入临时列表中
            line.append(i)
    if len(line) != 0:
        l.append(''.join(line))
    return l


#获取文章段落
def get_sentences_on_nid(nid):
    conn, cursor = get_postgredb()
    cursor.execute(news_text_nid_sql.format(nid))
    rows = cursor.fetchall()
    sentences = []
    for r in rows:
        title = r[1]
        content_list = r[2]
        for elems in content_list: #段落
            if "txt" in elems.keys():
                l = Cut(filter_tags(elems['txt']))
                sentences.extend(l)

    return sentences



