#!/usr/bin/python

import random

words = 100000
word_len = 10
alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-'

output = open('random_words', 'w')
for x in xrange(words):
	arr = [random.choice(alphabet) for i in range(word_len)]
	word = ''.join(arr)
	output.write(word)
	output.write('\n')
