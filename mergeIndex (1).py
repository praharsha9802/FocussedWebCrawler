import pickle
import re
import os
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers

CRAWLED_DATA_FOLDER = "./CRAWLED_DATA_FOLDER"
PARTIAL_INDEXING_FOLDER = "info.pickle"
FINAL_INDEX_FILE = "./final_indexed_file"
STOPWORDS_FILE = "topic_keywords/stoplist.txt"


def read_data(batch_no, batch_size):
    start = (batch_no - 1) * batch_size  # docs crawled in last batch + 1
    end = batch_no * batch_size
    #data_to_be_indexed = {}
    data_to_be_indexed = dict(info[start:end])
    #while start < end:
    #    data_to_be_indexed[info[start][0]] = info[start][1]
    #while start < end:
        # print("Reading from file " + str(start))
     #   filepath = open(PARTIAL_INDEXING_FOLDER, 'rb')
      #  new_dict = pickle.load(filepath)
      #  filepath.close()
      #  start += 1

        # check if we have correct info for data to be indexed
        #err_docs = []
        #for url in new_dict:
            #if "raw_html" in new_dict[url]:
            #    if len(new_dict[url]["raw_html"]) > 10000:  # shorten length
             #       new_dict[url]["raw_html"] = new_dict[url]["raw_html"][0:1000]
            #else:
            #    err_docs.append(url)

        # print("Deleting " + str(len(err_docs)) + " due to missing data")
        #for doc in err_docs:  # delete incorrect docs
         #   del new_dict[doc]

        #data_to_be_indexed.update(new_dict)  # merging existing data with new data
    return data_to_be_indexed


class Index:
    # cloud settings
    INDEX_NAME = 'maritime_accidents'
    TYPE_NAME = 'document'
    USERNAME = 'elastic'
    PASSWORD = 'dY5DwHK0PxQ7PgNC0Za8P0Ec'
    es = Elasticsearch('https://bf7d8087ca284033b930a990073d7b4e.us-central1.gcp.cloud.es.io:9243', http_auth=(USERNAME, PASSWORD), scheme='https', port=9243)

    # localhost settings
    # ES_HOST = {"host": "localhost", "port": 9200}
    # INDEX_NAME = 'maritime_accidents'
    # TYPE_NAME = 'document'
    # es = Elasticsearch(hosts=[ES_HOST], timeout=3600)

    def delete_and_create_new_index(self):
        if not self.es.indices.exists(self.INDEX_NAME):
            print("index already exists... deleting " + self.INDEX_NAME + " index...")
            res = self.es.indices.delete(index=self.INDEX_NAME, ignore=[400, 404])
            print(" response: '%s'" % res)

            request_body = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "max_result_window": 90000,
                    "analysis": {
                        "filter": {
                            "english_stop": {
                                "type": "stop",
                                "stopwords": ["a", "about", "above", "according", "across", "after", "afterwards", "again", "against", "albeit", "all", "almost", "alone", "along", "already", "also", 
                                              "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anywhere", "apart", "are", "around", "as",
                                              "at", "av", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides",
                                              "between", "beyond", "both", "but", "by", "can", "cannot", "canst", "certain", "cf", "choose", "contrariwise", "cos", "could", "cu", "day", "do", "does", "doing", "dost", "doth", "double", "down", 
                                              "dual", "during", "each", "either", "else", "elsewhere", "enough", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "except", "excepted", "excepting", "exception",
                                              "exclude", "excluding", "exclusive", "far", "farther", "farthest", "few", "ff", "first", "for", "formerly", "forth", "forward", "from", "front", "further", "furthermore", "furthest", "get", "go", "had", "halves",
                                              "hardly", "has", "hast", "hath", "have", "he", "hence", "henceforth", "her", "here", "hereabouts", "hereafter", "hereby", "herein", "hereto", "hereupon", "hers", "herself", "him", "himself", "hindmost", "his",
                                              "hither", "hitherto", "how", "however", "howsoever", "i", "ie", "if", "in", "inasmuch", "inc", "include", "included", "including", "indeed", "indoors", "inside", "insomuch", "instead", "into", "inward", "inwards",
                                              "is", "it", "its", "itself", "just", "kind", "kg", "km", "last", "latter", "latterly", "less", "lest", "let", "like", "little", "ltd", "many", "may", "maybe", "me", "meantime", "meanwhile", "might", "moreover", "most",
                                              "mostly", "more", "mr", "mrs", "ms", "much", "must", "my", "myself", "namely", "need", "neither", "never", "nevertheless", "next", "no", "nobody", "none", "nonetheless", "noone", "nope", "nor", "not", "nothing", 
                                              "notwithstanding", "now", "nowadays", "nowhere", "of", "off", "often", "ok", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over",
                                              "own", "per", "perhaps", "plenty", "provide", "quite", "rather", "really", "round", "said", "sake", "same", "sang", "save", "saw", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "seldom", "selves",
                                              "sent", "several", "shalt", "she", "should", "shown", "sideways", "since", "slept", "slew", "slung", "slunk", "smote", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat",
                                              "somewhere", "spake", "spat", "spoke", "spoken", "sprang", "sprung", "stave", "staves", "still", "such", "supposing", "than", "that", "the", "thee", "their", "them", "themselves", "then", "thence", "thenceforth", "there",
                                              "thereabout", "thereabouts", "thereafter", "thereby", "therefore", "therein", "thereof", "thereon", "thereto", "thereupon", "these", "they", "this", "those", "thou", "though", "thrice", "through", "throughout", "thru",
                                              "thus", "thy", "thyself", "till", "to", "together", "too", "toward", "towards", "ugh", "unable", "under", "underneath", "unless", "unlike", "until", "up", "upon", "upward", "upwards", "us", "use", "used", "using",
                                              "very", "via", "vs", "want", "was", "we", "week", "well", "were", "what", "whatever", "whatsoever", "when", "whence", "whenever", "whensoever", "where", "whereabouts", "whereafter", "whereas", "whereat", "whereby",
                                              "wherefore", "wherefrom", "wherein", "whereinto", "whereof", "whereon", "wheresoever", "whereto", "whereunto", "whereupon", "wherever", "wherewith", "whether", "whew", "which", "whichever", "whichsoever", "while",
                                              "whilst", "whither", "who", "whoa", "whoever", "whole", "whom", "whomever", "whomsoever", "whose", "whosoever", "why", "will", "wilt", "with", "within", "without", "worse", "worst", "would", "wow", "ye", "yet", "year",
                                              "yippee", "you", "your", "yours", "yourself", "yourselves"]
                                # "stopwords_path": "./topic_keywords/stoplist.txt"
                            }
                        },
                        "analyzer": {
                            "stopped": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "porter_stem",
                                    "english_stop"
                                ]
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "_size": {
                            "enabled": True
                        },
                        "text": {
                            "type": "text",
                            "fielddata": True,
                            "analyzer": "stopped",
                            "index_options": "positions"
                        },
                        "id": {
                            "type": "keyword",
                            "index": True
                        },
                        "inlinks": {
                            "type": "keyword",
                            "index": True
                        },
                        "outlinks": {
                            "type": "keyword",
                            "index": True
                        }
                    }
                }
            }

            print("creating " + self.INDEX_NAME + " index...")
            res = self.es.indices.create(index=self.INDEX_NAME, body=request_body, timeout = '1000s')
            print(" response: '%s'" % res)

    def index_data(self, data, failed_doc):
        bulk_data = []  # list of all crawled urls to be indexed

        # data[url] = {"id": url, "headers": header, "raw_html": aw_html, "text": text, # "inlinks": (list)inlinks[url], "outlinks": (list)outlinks}
        for key in data:
            #if "https:" in key:
             #   data[key]["id"] = data[key]["id"].replace('https', 'http')
              #  key = key.replace('https', 'http')
            #exists = self.check_doc_exists(key)

            if len(data[key]["text"]) > 70000000:
                data[key]["text"] = data[key]["text"][:70000000]        # create a new entry
            data_refined = {
                    "_index": self.INDEX_NAME,
                    "_id": str(key).replace("://", "-").replace("/", "-"),
                    "_source": data[key]
            }
            bulk_data.append(data_refined)

            #else:  # get existing inlinks and merge
                # print(old_inlinks)
                # print(data[key]["inlinks"])
                #if len(old_inlinks) > 0:
                #    new_inlinks = list(set(old_inlinks + data[key]["inlinks"]))
                 #   data[key]["inlinks"] = new_inlinks
                  #  change = len(old_inlinks) - len(new_inlinks)
                   # if change != 0:
                    #    print("New inlinks added for " + key + " " + str(change))
                #data_refined = {
                 #   "_index": self.INDEX_NAME,
                 #   "_id": str(key).replace("://", "-").replace("/", "-"),
                 #   "_source": data[key]
                #}
                #bulk_data.append(data_refined)

        print("----------------Indexing bulk data--------------------")
        try:
            res = helpers.bulk(self.es, bulk_data)
            #print(res)

            # sanity check
            #res = self.es.search(index=self.INDEX_NAME, size=2, body={"query": {"match_all": {}}})
            #print(" response: '%s'" % res)
        except Exception as e:
            print(e)
            print("Indexing failed for chunk")
            failed_doc = failed_doc + bulk_data
            pass
        finally:
            return failed_doc

    def check_doc_exists(self, doc_id):
        try:
            # No exception indicates record is found in the index
            result = self.es.get(index=self.INDEX_NAME, id=doc_id)
                                 #.replace("://", "-").replace("/", "-"))
            # print(doc_id + " " + str(result["found"]))
            #return True, result["_source"]["inlinks"]
            return True
        except:
            # print(doc_id + " not found")
            return False


if __name__ == '__main__':
    start_time = time.time()
    batch_size = 500
    no_batches = int(42000 / batch_size)

    # with open(STOPWORDS_FILE, 'r', encoding='ISO-8859-1') as file:
    #     stopword_list = file.read().split("\n")
    # file.close()
    # print(stopword_list)
    # exit()

    maritime_index = Index()

    # Deletion should be only run by the first team member
    #maritime_index.delete_and_create_new_index()

    failed_docs = []
    for i in range(1, no_batches + 1):
        print("Processing batch "+str(i))
        data_to_be_indexed_dict = read_data(i, batch_size)
        #data_to_be_indexed = info
        # test
        # data_to_be_indexed_dict["http://en.wikipedia.org/wiki/List_of_maritime_disasters"]["inlinks"].append(
        #         "http://en.wikipedia.org/wiki/test3")
        print("New docs to index " + str(len(data_to_be_indexed_dict)))
        failed_docs = maritime_index.index_data(data_to_be_indexed_dict, failed_docs)
        print("Indexed total of" + str(i * batch_size) + "docs")
        print("--- %s seconds ---" % (time.time() - start_time))

    print("----------------Indexing failed files --------------------")
    try:
        batch_size = batch_size/10
        start = 0
        end = int(start + batch_size + 1)
        for i in range(start, end):
            res = helpers.bulk(maritime_index.es, failed_docs[start:end])
            print(res)
            start = start+end
    except Exception:
        print("Cannot index")

