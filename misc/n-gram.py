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


def gather_n_grams(article_text, n):
    """
    Tokenize and find ngrams.
    :param article_text:
    :param n: number of distinct words to group
    :return: top 10 occurring n grams.
    """
    stopword_s = nltk.corpus.stopwords.words('english') + \
                 ['.', 'for', 'the', 'an', 'of', 'and', 'be', 'have']

    words = re.sub(r'[^\w\s]', '', article_text).split()
    final_words = [word for word in words if word not in stopword_s]

    return (pd.Series(nltk.ngrams(final_words, n)).value_counts())[:10]


if __name__ == '__main__':
    atext = get_wiki_article('http://en.wikipedia.org/wiki/N-gram')
    top_10_n_gram = gather_n_grams(atext, 3)
    print(top_10_n_gram)
