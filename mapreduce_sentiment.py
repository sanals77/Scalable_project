from multiprocessing import Pool
from textblob import TextBlob
import csv

def map_function(line):
    tweet = line[-1]
    polarity = TextBlob(tweet).sentiment.polarity
    sentiment = 'positive' if polarity > 0 else 'negative' if polarity < 0 else 'neutral'
    return sentiment, 1

def reduce_function(mapped_data):
    result = {}
    for sentiment, count in mapped_data:
        result[sentiment] = result.get(sentiment, 0) + count
    return result

if __name__ == '__main__':
    with open('sampled_100k_sentiment140.csv', 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        header = next(reader)  # skip header
        lines = list(reader)

    with Pool(4) as p:
        mapped = p.map(map_function, lines[:10000])  # adjust range if needed

    result = reduce_function(mapped)
    print(result)
