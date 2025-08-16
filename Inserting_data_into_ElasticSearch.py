#Import the libraries
from pandas import json_normalize
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from faker import Faker
fake = Faker()

#create a connection Elasticsearch
# es = Elasticsearch([{'host': '127.0.0.1', 'port': 9200, 'scheme': 'http'}])


es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "yI*0e2MjOOr0DF-R6_m4"),
    ca_certs="/Users/pawanjadhav/elasticsearch-9.1.0/config/certs/http_ca.crt"

)

#---------------------------------------------------------#

#create the JSON object and add the data
# doc = {"name": fake.name(), "Street": fake.street_address(), "City": fake.city(),"zip": fake.zipcode()}
# # result = es.index(index="users", document=doc)
# # print(result) #created

#Inserting data using helpers
# actions = [{
#     "_index": "users",
#     "_source": {
#         "name": fake.name(),
#         "street": fake.street_address(),
#         "city": fake.city(),
#         "zip": fake.zipcode()
#     }
#
# }
#     for x in range(998)
#  ]
#
# res_1 = helpers.bulk(es, actions)
# print(res_1)
# print(es.info())
#---------------------------------------------------------#
# Create the JSON object to send to Elasticsearch.
doc = {"query": {"match_all": {}}}

#Pass the object to Elasticsearch using the search method
result = es.search(index="users")
#Lastly,print the documents
# print(result["hits"]["hits"])
# for hit in result["hits"]["hits"]:
#     print(hit["_source"])

df = json_normalize(result["hits"]["hits"])
# print(df)

#Lucene syntax for queries: can specify field:value
res_2 = es.search(index="users", q="name:Ronald Goodman")
# print(res_2['hits']['hits'][0]['_source'])

#Get City Name
doc = {
    "query": {
        "match":
            {"city": "South Danielland"}
    }
}
res_3 = es.search(index="users", body=doc)
# print(json_normalize(res_3['hits']['hits']))

# Get Jamesberg and filter on zip so Lake Jamesberg is removed
doc = {
    "query": {
        "bool": {
            "must": [
                {"match": {"city": "South Danielland"}}
            ],
            "filter": [
                {"term": {"zip": "48450"}}
            ]
        }
    }
}
res_4 = es.search(index="users", body=doc)
for hit in res_4['hits']['hits']:
    # print(hit["_source"])

#Using Scroll to handle larger results
    doc_5 = {
        "query": {
            "match_all": {}
        }
    }
res = es.search(
    index="users",
    body=doc_5,
    scroll="20m",
    size=500
)

sid = res['_scroll_id']
size = res['hits']['total']['value']

while (size > 0):
    res = es.scroll(scroll_id=sid, scroll="20m")
    sid = res['_scroll_id']
    size = len(res['hits']['hits'])
    for doc in res['hits']['hits']:
        print(doc["_source"])
