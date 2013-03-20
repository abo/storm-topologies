#encoding=utf-8
import storm
import sys
sys.path.append("deps/jieba/")

import jieba

class SplitSentenceBolt(storm.BasicBolt):
    def process(self, tup):
    	words = jieba.cut(tup.values[0],cut_all=False)
        for word in words:
          storm.emit([word])

SplitSentenceBolt().run()