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
pred_file = open('pred.txt', 'r')
pred_text = strProcess(pred_file.read())
pred_file.close()
pred_file = open('pred.txt', 'w')
for w in pred_text:
    pred_file.write(w.encode('utf-8') + ' ')
pred_file.close()

pred_docs = gl.SFrame.read_csv('pred.txt', header=False)
pred_docs = gl.text_analytics.count_words(pred_docs['X1'])
pred_docs = pred_docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
print pred_docs
pred2 = model.predict(pred_docs)
print pred2[0]
print '%s' % str(sf[pred2[0]]['words']).decode('string_escape')
'''

data_sframe_dir = real_dir_path + '/data_sframe'
g_channel_model_dict = {}
data_dir = real_dir_path + '/data/'
def create_model(csv_file):
    global g_channel_model_dict
    if not os.path.exists(data_sframe_dir):
        os.mkdir(data_sframe_dir)

    docs = gl.SFrame.read_csv(data_dir+csv_file, header=False)
    docs = gl.text_analytics.count_words(docs['X1'])
    docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
    model = gl.topic_model.create(docs, num_iterations=100, num_topics=50, verbose=True)
    g_channel_model_dict[csv_file] = model
    '''
    sf = model.get_topics(num_words=20, output_type='topic_words')

    pred_file = real_dir_path + '/pred.txt'
    pred_docs = gl.SFrame.read_csv(pred_file, header=False)
    pred_docs = gl.text_analytics.count_words(pred_docs['X1'])
    pred_docs = pred_docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)


    #get highest probolity
    pred2 = model.predict(dataset=pred_docs, output_type='probability')
    max_t = 0
    max_p = 0
    for i in range(0, len(pred2[0])):
        if pred2[0][i] >= max_p:
            max_t = i
            max_p = pred2[0][i]
    print max_t
    print '%s' % str(sf[max_t]['words']).decode('string_escape')
    print '-----------------'

    #predict one topic
    pred2 = model.predict(pred_docs)
    print pred2[0]
    print '%s' % str(sf[pred2[0]]['words']).decode('string_escape')
    '''

def lda_predict(nid):
    global g_channel_model_dict
    words_list, chanl_name = topic_model_doc_process.get_words_on_nid(nid)
    print chanl_name
    print words_list
    docs = gl.SFrame(data=words_list)
    docs = gl.text_analytics.count_words(docs['X1'])
    docs = docs.dict_trim_by_keys(gl.text_analytics.stopwords(), exclude=True)
    print docs
    print len(g_channel_model_dict)
    print chanl_name
    print type(chanl_name)

    for i in g_channel_model_dict.keys():
        print type(i)
        print i

    sf = g_channel_model_dict[chanl_name].get_topics(num_words=20, output_type='topic_words')

    #预测得分最高的topic
    pred = g_channel_model_dict[chanl_name].predict(docs)
    print pred
    print '%s' % str(sf[pred[0]]['words']).decode('string_escape')




