import json
import certifi
from elasticsearch import helpers
from elasticsearch import Elasticsearch


URL="https://YOUR_ACCESS_KEY:YOUR_ACCESS_SECRET@kafka-test-1248082572.us-west-2.bonsaisearch.net:443"



def sink2elastic(doc, index, type):
    esclient = Elasticsearch([URL],use_ssl=True,verify_certs=False,ssl_show_warn=False)
    # counter
    statcnt = 0
    actions = []
    for row in doc:
        actions.append({
            "_op_type": "index",
            "_index": index,
            "_type": type,
            "_source": row
        })

    for ok, response in helpers.streaming_bulk(esclient, actions, index=index, doc_type=type,
                                               max_retries=5,
                                               raise_on_error=False, raise_on_exception=False):
        if not ok:
            statcnt+=0
            print(response)
        else:
            statcnt += 1
    return statcnt
