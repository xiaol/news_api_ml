# -*- coding: utf-8 -*-
# @Time    : 16/11/30 下午2:24
# @Author  : liulei
# @Brief   : 
# @File    : topic_modle.py
# @Software: PyCharm Community Edition
# Download data if you haven't already
import graphlab as gl
import os

dir_path = os.path.split(os.path.realpath(__file__))[0]
wikipedia_file = dir_path + '/wikipedia_w0'
w0_file = dir_path + '/w0.csv'

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
