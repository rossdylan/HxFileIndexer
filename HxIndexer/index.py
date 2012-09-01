from grabber import grab
import xappy


def sort_by(data, func=None, key=None):
    if func != None:
        return sorted(data, key=func)

def slices(word):
    if word == "":
        return []
    else:
        l = [word]
        l.extend(slices(word[:-1]))
        return l
last = ""
def reprint(text):
    global last
    print '{0}\r'.format(' '*len(last)),
    print '{0}\r'.format(text),
    last = text

class XapianIndexer(object):
    def __init__(self, dbpath):
        self.dbpath = dbpath
        self.create_index()

    def create_index(self):
        iconn = xappy.IndexerConnection(self.dbpath)

        iconn.add_field_action('file_name', xappy.FieldActions.INDEX_FREETEXT)
        iconn.add_field_action('file_type', xappy.FieldActions.INDEX_FREETEXT)
        iconn.add_field_action('file_size', xappy.FieldActions.INDEX_EXACT)

        self.iconn = iconn


    def index_files(self, files):
        print "Indexing..."
        count = 0
        for f in files:
            reprint(f.name)
            doc = xappy.UnprocessedDocument()
            doc.fields.append(xappy.Field('file_name', f.name, weight=10.0))
            doc.fields.append(xappy.Field('file_type', str(f.type), weight=1.0))
            doc.fields.append(xappy.Field('file_size', str(f.size), weight=0.2))
            processed_doc = self.iconn.process(doc, False)
            processed_doc._doc.set_data(f.json)
            processed_doc._data = None
            self.iconn.add(processed_doc)
            count += 1
        return count

def search(dbpath, querystring, offset=0, pagesize=10):
    sconn = xappy.SearchConnection(dbpath)
    query = sconn.query_parse(querystring, default_op=sconn.OP_AND)
    results = sconn.search(query, offset, pagesize)
    for result in results:
        raw = result._doc.get_data()
        print raw



def run(dbpath, filepath):
    "Indexing {0} into {1}".format(filepath, dbpath)
    indexer = XapianIndexer(dbpath)
    grabber = grab(filepath)
    num = indexer.index_files(grabber)
    print
    print "Indexed {0} files".format(num)

if __name__ == "__main__":
    import sys
    if sys.argv[1] == 'index':
        run("./index.db", "/home/rossdylan/Files")
    elif sys.argv[1] == 'search':
        search("./index.db", " ".join(sys.argv[2:]))
