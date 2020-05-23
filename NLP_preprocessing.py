# -*- coding: utf-8 -*-
"""
Created on Fri May 22 05:02:34 2020

@author: sandip
"""

# Tokenizing - to split the sentence into words.
# We tokenize a tweet here using nltk library
from nltk.tokenize import TweetTokenizer

text = 'This is really a coolz thing @Idol todo. 67!, sayonara. :('
tokenizer = TweetTokenizer()
print(tokenizer.tokenize(text.lower()))

# Stemming
from nltk import PorterStemmer
stemmer = PorterStemmer()
print(stemmer.stem('nicely'))

# WordNetLemmatizer
from nltk import WordNetLemmatizer
lemma = WordNetLemmatizer()
print(lemma.lemmatize('antinationalistic'))