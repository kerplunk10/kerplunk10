import requests
import pandas
from dateutil import parser
import json
host = 'http://18.188.56.207:9200/'
requests.get(host + '_cat/indices/enron').content


def elasticsearch_results_to_df(results):
    '''
    A function that will take the results of a requests.get 
    call to Elasticsearch and return a pandas.DataFrame object 
    with the results 
    '''
    #print("results:::",results)
    hits = results.json()['hits']['hits']
    #print("hits:::",hits)
    data = pandas.DataFrame([i['_source'] for i in hits], index = [i['_id'] for i in hits])
    #print("data:::",data)
    data['date'] = data['date'].apply(parser.parse)
    return(data)

def print_df_row(row):
    '''
    A function that will take a row of the data frame and print it out
    '''
    print('____________________')
    print('RE: %s' % row.get('subject',''))
    print('At: %s' % row.get('date',''))
    print('From: %s' % row.get('sender',''))
    print('To: %s' % row.get('recipients',''))
    print('CC: %s' % row.get('cc',''))
    print('BCC: %s' % row.get('bcc',''))
    print('Body:\n%s' % row.get('text',''))
    print('____________________')


############################################## Strategy-1 ################################################################################
# Find emails from the know list of Enron employees convicted(referred as person of interest(POI)). Extract email body, 
#tokenize it to words, clean-up and plot wordcloud to discover any intereseting keywords that can then be used to enhance the search query
# and further refine the search results. Repeat this process as many times as required futher refining the search results and ultimately
# finding the wrongdoing email
##########################################################################################################################################
# Query For a full text match in the "text" field
# Uses the "match" query: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html
#https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html

doc = {
  "size" : 10000,
  "query": {
    "bool": {  
      "must" : {
        "query_string" : {
            "query": "(text: (LJM OR manipulate OR manipulated OR blackout OR fraud OR illegal OR unethical OR immoral OR insider OR conceal OR prison OR jail OR obscure OR disguise ) AND NOT (\"news\" OR \"newspaper\" OR \"newspapers\" OR \"new york times\" OR \"washington post\" OR \"reuters\" OR \"newsletter\" OR \"newsletters\" OR \"e-newsletter\")) "
        }
      }
    }
  }
}


r=requests.get(host + 'enron/_search',
               data=json.dumps(doc), headers={'Content-Type':'application/json'})
r.raise_for_status()
print("Found %s messages matching the query, of " % r.json()['hits']['total'])
df = elasticsearch_results_to_df(r)
#print(df.head())
print("Returned %s messages" % df.shape[0])
#print_df_row(df.iloc[0])


#Extract all the words from the emails an plot word cloud to discover new keywords to further refine the search query
import nltk
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import matplotlib.pyplot as plt

def preprocess(textStr):
    textStr = textStr.lower()
    tokenizer = RegexpTokenizer(r'\w+')
    tokens = tokenizer.tokenize(textStr)
    #Remove Stopwords
    tokens_no_stopwords = [w for w in tokens if not w in stopwords.words('english')]
    #lemmatize
    lem = WordNetLemmatizer()
    tokens_lemmatized = []
    for w in tokens_no_stopwords:
        tokens_lemmatized.append(lem.lemmatize(w, "v"))
    return tokens_lemmatized

def tokenizeemails(emails):
    all_words = []
    for text in emails:
        all_words.extend(preprocess(text))
    return all_words
filtered_words = tokenizeemails(df.get('text'))
#print("Filterd Words:",filtered_words)
#all_words_freq_dist = nltk.FreqDist(filtered_words)
#print(all_words_freq_dist.most_common(15))

#from wordcloud import WordCloud

#create a single string from list
send_list=' '.join(filtered_words)
# Create and generate a word cloud image:
wordcloud = WordCloud().generate(send_list)

# Display the generated image:
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()
