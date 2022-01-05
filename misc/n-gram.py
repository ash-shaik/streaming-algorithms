"""
n-gram is a sequence of n words. n-gram ranking is an algorithm to rank the n-grams
based on their frequency of occurrences in the input corpus.
If we want the top say 10 we get the top 10 occurring n-gram sequences.
"""

import urllib.request
import bs4 as wikiparser
import nltk
import re
import pandas as pd
from collections import defaultdict, Counter

STOP_WORDS = nltk.corpus.stopwords.words('english') + \
                 ['.', 'for', 'the', 'an', 'of', 'and', 'be', 'have']


def get_wiki_article(article_url):
    """
    Makes a request to get the article and does basic cleaning of the text
    before n-gram processing.
    :param article_url: wiki article url
    :return: article text to be processes.
    """
    wiki_article_raw = urllib.request.urlopen(article_url)

    wiki_article = wiki_article_raw.read()
    article_html = wikiparser.BeautifulSoup(wiki_article, "lxml")
    article_paragraphs = article_html.find_all('p')
    article_text = ''

    for paragraph in article_paragraphs:
        article_text += paragraph.text

    article_text = article_text.lower()
    article_text = re.sub(r'[^A-Za-z. ]', '', article_text)
    return article_text


def clean_corpus(article_text):
    """
    Common corpus cleaning operations of removing white space and stop words
    :param article_text:
    :return: clean list of words in the article.
    """
    words = re.sub(r'[^\w\s]', '', article_text).split()
    final_words = [word for word in words if word not in STOP_WORDS]
    return final_words


def gather_n_grams(article_words, n):
    """
    Tokenize and find ngrams.
    :param article_text:
    :param n: number of distinct words to group
    :return: top 10 occurring n grams.
    """
    # final_words = clean_corpus(article_text)
    return (pd.Series(nltk.ngrams(article_words, n)).value_counts())[:10]


def _ngram(article_words, n):
    """
    generate n-gram frequencies for word sequences of len n.
    :param article_words:
    :param n: number of words to group.
    :return:top 10 occurring n-grams
    Runtime : O(num_words)
    """

    ngrams_dict = defaultdict(int)

    for i in range(len(article_words) - n):
        seq = ' '.join(article_words[i: i + n])
        ngrams_dict[seq] += 1

    return dict(Counter(ngrams_dict).most_common(10))


if __name__ == '__main__':
    atext = get_wiki_article('http://en.wikipedia.org/wiki/N-gram')

    final_words = clean_corpus(atext)
    top_10_n_grams = _ngram(final_words, 3)
    # top_10_n_grams = gather_n_grams(final_words, 3)
    print(top_10_n_grams)
