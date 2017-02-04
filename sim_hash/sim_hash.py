# -*- coding: utf-8 -*-
# @Time    : 17/2/4 下午2:46
# @Author  : liulei
# @Brief   : 
# @File    : sim_hash.py
# @Software: PyCharm Community Edition

from util import doc_process


class simhash():
    def __init__(self, tokens='', hashbits=128):
        self.hashbits = hashbits
        self.hash = self.simhash(tokens)

    def __str__(self):
        return str(self.hash)

    def __long__(self):
        return long(self.hash)

    def __float__(self):
        return float(self.hash)

    def simhash(self, tokens):
        # Returns a Charikar simhash with appropriate bitlength
        v = [0] * self.hashbits

        #print type(tokens)
        print len(tokens)
        for t in [self._string_hash(x) for x in tokens]:
            bitmask = 0
            # print (t)
            for i in range(self.hashbits):
                bitmask = 1 << i
                # print(t,bitmask, t & bitmask)
                if t & bitmask:
                    v[i] += 1  # 查看当前bit位是否为1，是的话则将该位+1
                else:
                    v[i] += -1  # 否则得话，该位减1

        fingerprint = 0
        for i in range(self.hashbits):
            if v[i] >= 0:
                fingerprint += 1 << i
                # 整个文档的fingerprint为最终各个位大于等于0的位的和
        return fingerprint

    def _string_hash(self, v):
        # A variable-length version of Python's builtin hash
        if v == "":
            return 0
        else:
            x = ord(v[0]) << 7
            m = 1000003
            mask = 2 ** self.hashbits - 1
            for c in v:
                x = ((x * m) ^ ord(c)) & mask
            x ^= len(v)
            if x == -1:
                x = -2
            return x

    def hamming_distance(self, other_hash):
        x = (self.hash ^ other_hash.hash) & ((1 << self.hashbits) - 1)
        tot = 0
        while x:
            tot += 1
            x &= x - 1
        return tot

    def similarity(self, other_hash):
        #a = float(self.hash)
        #b = float(other_hash.hash)
        #if a > b: return b / a
        #return a / b
        b = self.hashbits
        return float(b - self.hamming_distance(other_hash)) / b

import jieba
if __name__ == '__main__':
    # 看看哪些东西google最看重？标点？
    s = '看看哪些东西google最看重标点'
    #hash1 = simhash(jieba.cut(''.join(s.split())))
    # print("0x%x" % hash1)
    # print ("%s\t0x%x" % (s, hash1))

    s = '看一下哪些东西google最看重标点'
    #hash2 = simhash(jieba.cut(''.join(s.split())))
    # print ("%s\t[simhash = 0x%x]" % (s, hash2))

    #print '%f%% percent similarity on hash' % (100 * (hash1.similarity(hash2)))
    #print hash1.hamming_distance(hash2), "bits differ out of", hash1.hashbits

    w1 = doc_process.get_words_on_nid(11580728)
    w2 = doc_process.get_words_on_nid(11603489)
    print ' '.join(w1)
    print '------'
    print ' '.join(w2)
    print type(w1)
    h1 = simhash(w1)
    h2 = simhash(w2)
    print 100 * h2.similarity(h1)
