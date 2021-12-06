import requests
import pandas
from dateutil import parser
host = 'http://18.188.56.207:9200/'
requests.get(host + '_cat/indices/enron').content

pandas.set_option('display.max_rows', None, "display.max_columns", None)


doc = {
    "query" : {
        "match_all" : {}
    }
}
import json
r=requests.get(host + 'enron/_search', data=json.dumps(doc), headers={'Content-Type':'application/json'})


def elasticsearch_results_to_df(results):
    '''
    A function that will take the results of a requests.get 
    call to Elasticsearch and return a pandas.DataFrame object 
    with the results 
    '''
    hits = results.json()['hits']['hits']
    data = pandas.DataFrame([i['_source'] for i in hits], index = [i['_id'] for i in hits])
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

df = elasticsearch_results_to_df(r)
#print(r) - result: <Response [200]>
#print(df)
#print(df.head(12))


#print_df_row(df.iloc[0])

doc = {
    "size": 2000,
    #"sort": "date",
    "from": 0,
    "query": {
        "query_string": {
            "query": "(sender: (jeff.skilling@enron.com OR kenneth.lay@enron.com OR ken_lay@enron.com OR klay@enron.com OR kenneth_lay@enron.com OR andrew.fastow@enron.com OR richard.causey@enron.com OR ken.rice@enron.com OR kenneth.rice@enron.com OR joe.hirko@enron.com OR michael.kopper@enron.com OR ben.glisan@enron.com OR mark.koenig@enron.com OR tim.despain@enron.com OR david.delainey@enron.com OR tim.belden@enron.com OR tbelden@enron.com OR jeff.richter@enron.com OR sherron.watkins@enron.com) OR recipients: (jeff.skilling@enron.com OR kenneth.lay@enron.com OR ken_lay@enron.com OR klay@enron.com OR kenneth_lay@enron.com OR andrew.fastow@enron.com OR richard.causey@enron.com OR ken.rice@enron.com OR kenneth.rice@enron.com OR joe.hirko@enron.com OR michael.kopper@enron.com OR ben.glisan@enron.com OR mark.koenig@enron.com OR tim.despain@enron.com OR david.delainey@enron.com OR tim.belden@enron.com OR tbelden@enron.com OR jeff.richter@enron.com OR sherron.watkins@enron.com) AND date: (2001-08-13 OR 2001-08-14 OR 2001-08-15 OR 2001-10-12 OR 2001-10-13 OR 2001-10-14 OR 2001-10-15 OR 2001-10-16 OR 2001-10-17 OR 2001-10-22 OR 2001-10-23) )"
        }
    },
}


r=requests.get(host + 'enron/_search',
               data=json.dumps(doc), headers={'Content-Type':'application/json'})
r.raise_for_status()
print("Found %s messages matching the query, of " % r.json()['hits']['total'])
df = elasticsearch_results_to_df(r)
#print(df)
#print(df.values)
print("Returned %s messages" % df.shape[0])

#print(df.tail)
#print_df_row(df.iloc[0])
counter = df.shape[0]
#print("counter:",counter)
#counter = 9
for email in range(counter):
    print_df_row(df.iloc[email])
