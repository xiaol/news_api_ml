# -*- coding: utf-8 -*-
# @Time    : 16/11/30 下午2:24
# @Author  : liulei
# @Brief   : 
# @File    : topic_modle.py
# @Software: PyCharm Community Edition
# Download data if you haven't already
import graphlab as gl
import os
import topic_model_doc_process

real_dir_path = os.path.split(os.path.realpath(__file__))[0]
'''
wikipedia_file = real_dir_path + '/wikipedia_w0'
w0_file = real_dir_path + '/w0.csv'

if os.path.exists(wikipedia_file):
    docs = gl.SFrame(wikipedia_file)
else:
    #docs = gl.SFrame.read_csv('https://static.turi.com/datasets/wikipedia/raw/w0.csv', header=False)
    docs = gl.SFrame.read_csv(w0_file, header=False)
    docs.save(wikipedia_file)

# Remove stopwords and convert to bag of words
docs = gl.text_analytics.count_words(docs['X1'])
docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)

# Learn topic model
#model = gl.topic_model.create(docs)
model = gl.topic_model.create(docs, num_iterations=100, num_topics=50, verbose=True)
#model = gl.topic_model.create(docs, num_iterations=100, num_topics=50, verbose=False)

sf = model.get_topics(num_words=20, output_type='topic_words')
#print '------'
#print sf.num_rows()
#print sf.column_names()

#print '%s' % str(sf[0]['words']).decode('string_escape')
pred = model.predict(docs)
print pred
print '%s' % str(sf[pred[86]]['words']).decode('string_escape')

from jieba import analyse
def strProcess(str):
    str2 = ''.join(str.split())
    words = analyse.extract_tags(str2, 50)
    return words

#predict
pred_file_path = real_dir_path + '/pred.txt'
pred_file = open(pred_file_path, 'r')
pred_text = strProcess(pred_file.read())
pred_file.close()
pred_file = open(pred_file_path, 'w')
for w in pred_text:
    pred_file.write(w.encode('utf-8') + ' ')
pred_file.close()

pred_docs = gl.SFrame.read_csv(pred_file_path, header=False)
pred_docs = gl.text_analytics.count_words(pred_docs['X1'])
pred_docs = pred_docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
print '=================='
print pred_docs
pred2 = model.predict(pred_docs)
print pred2[0]
print '%s' % str(sf[pred2[0]]['words']).decode('string_escape')
'''

data_sframe_dir = real_dir_path + '/data_sframe'

g_channel_model_dict = {}

data_dir = real_dir_path + '/data/'
def create_model_proc(csv_file):
    if not os.path.exists(data_sframe_dir):
        os.mkdir(data_sframe_dir)

    docs = gl.SFrame.read_csv(data_dir+csv_file, header=False)
    docs = gl.text_analytics.count_words(docs['X1'])
    docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
    model = gl.topic_model.create(docs, num_iterations=100, num_topics=50, verbose=True)
    g_channel_model_dict[csv_file] = model


def create_models():
    from topic_model_doc_process import channel_for_topic
    for chanl in channel_for_topic:
        create_model_proc(chanl)
    print 'create models finished!!'


def lda_predict(nid):
    global g_channel_model_dict
    words_list, chanl_name = topic_model_doc_process.get_words_on_nid(nid)
    if chanl_name not in g_channel_model_dict.keys():
        print 'Error: channel name is not in models'
    s = ''
    for i in words_list:
        s += i + ' '
    ws = gl.SArray([s,])
    docs = gl.SFrame(data={'X1':ws})
    docs = gl.text_analytics.count_words(docs['X1'])
    docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
    sf = g_channel_model_dict[chanl_name].get_topics(num_words=20, output_type='topic_words')

    #预测得分最高的topic
    pred = g_channel_model_dict[chanl_name].predict(docs)
    print pred
    print '%s' % str(sf[pred[0]]['words']).decode('string_escape')

    print '=================================='
    pred2 = g_channel_model_dict[chanl_name].predict(docs, output_type='probability')
    num_dict = {}
    num = 0
    for i in pred2[0]:
        num_dict[pred2[0][num]] = num
        num += 1
    probility = sorted(num_dict.items(), key=lambda d:d[0], reverse=True)
    i = 0
    while i < 3 and i <len(probility) and probility[i][0] > 0.1:
        print '%s' % str(sf[probility[i][1]]['words']).decode('string_escape')
        i += 1





