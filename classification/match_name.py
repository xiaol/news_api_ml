# -*- coding: utf-8 -*-
# @Time    : 16/8/2 下午5:43
# @Author  : liulei
# @Brief   : 给定文章和文章类别,与list中的名称做匹配,返回匹配的名称
# @File    : match_name.py
# @Software: PyCharm Community Edition

import xlrd
import jieba
import jieba.analyse
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class Names(object):
    def __init__(self, type):
        self.type = type
        self.name_list = []
    #从xlsm文件读取所有名称
    def getTypeNamesList(self):
        pass
    def addName(self, name):
        self.name_list.append(name)
    #根据名称列表构造字典
    #字典中每个词的说明由词名、词频和词性（可省）构成
    def createDictForJieba(self):
        self.dict_name = './util/' + self.type + '.dict'
        try:
            with open(self.dict_name, 'wr') as newDict:
                for newName in self.name_list:
                    str = newName + ' 1'  #词频为1, 词性省略
                    newDict.write(str + '\n')
        except IOError as err:
            print('ioerror : %s' + str(err))

    #获取文章相关的名字
    def getArticalTypeList(self, txt):
        print 'super len = ' + str(len(self.name_list))
        jieba.load_userdict(self.dict_name)
        #words = jieba.cut(txt)
        words = jieba.analyse.extract_tags(txt, 5)
        word_dict = {}
        name_dict = {}
        #word_dict[words] = 1
        for word in words:
            print word
            if word not in word_dict:
                word_dict[word] = 1
            else:
                word_dict[word] += 1
        for name in self.name_list:
            if name in word_dict:
                name_dict[name] = word_dict[name]
        return name_dict


class GameNames(Names):
    def __init__(self):
        super(GameNames, self).__init__('game')
        self.getTypeNamesList()
    def getTypeNamesList(self):
        game_data = xlrd.open_workbook('./util/game_list1.xlsx')
        table = game_data.sheets()[0]
        nrows = table.nrows
        i = 0
        while( i < nrows):
            name = ''.join(table.row_values(i))
            super(GameNames, self).addName(name)
            i += 1
        #构造新字典,供jieba使用
        super(GameNames, self).createDictForJieba()

gameNames = GameNames()
def NameFactory(type):
    if type == 'game':
        return gameNames
    else:
        pass
