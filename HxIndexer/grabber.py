from multiprocessing import Pool
from collections import namedtuple
from itertools import chain
from functools import partial
import os
import mimetypes
import json


File = namedtuple('File', ['name', 'path', 'type', 'size', 'json'])
def make_file(name, path):
    _type = mimetypes.guess_type(path)
    size = os.path.getsize(path)
    le_file = File(
            name=name,
            path=path,
            type=_type,
            size=size,
            json=json.dumps(
                dict(
                    name=name,
                    path=path,
                    type=_type,
                    size=size,
                    )))
    return le_file

def grab_file_structure(root):
    docs = []
    for wroot, dirs, files in os.walk(root):
        for f in files:
            doc = make_file(f, os.path.join(wroot,f))
            docs.append(doc)
    return docs

def grab(root):
    le_join = partial(os.path.join, root)
    dir_list = map(le_join, os.walk(root).next()[1])
    proc_pool = Pool(5)
    results = proc_pool.imap_unordered(grab_file_structure, dir_list)
    proc_pool.close()
    proc_pool.join()
    docs = []
    while True:
        try:
            docs.append(results.next())
        except:
            break
    output = list(chain.from_iterable(docs))
    return output

if __name__ == "__main__":
    results = grab("/home/rossdylan/")
    print results
