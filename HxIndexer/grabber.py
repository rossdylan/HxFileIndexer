from multiprocessing import Process, Queue
from functools import partial
from collections import namedtuple
import os, mimetypes, json

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


def filer(dq, fq):
    """
    Waits on the directory queue, and dumps files into the file queue
    """
    while True:
        d = dq.get()
        if d == None:
            print "Filer exiting"
            break
        get_files_in_dir(d, fq)
    fq.put(None)


def get_files_in_dir(d, q):
    """
    Get all files in a directory and dump them into a queue
    """
    join_here = partial(os.path.join, d)
    walkd = os.walk(d).next()
    names = walkd[2]
    files = map(join_here, names)
    zipped_files = zip(names, files)
    for f in zipped_files:
        q.put(make_file(f[0], f[1]))


def get_dirs_from_root(root, q, mf):
    """
    Walk a fs starting at root, dumping all directories into a queue
    """
    for wroot, dirs, files in os.walk(root):
        join_here = partial(os.path.join, wroot)
        the_dirs = map(join_here, dirs)
        map(q.put, the_dirs)
    [q.put(None) for x in xrange(mf)]


class ProcPool(object):
    def __init__(self, num, func, *args):
        self.procs = [Process(target=func, args=args) for x in xrange(num)]

    def start(self):
        for proc in self.procs:
            proc.start()

    def join(self):
        for proc in self.procs:
            proc.join()


def grab(root, max_filers=4):
    dir_queue = Queue()
    file_queue = Queue()

    get_files_in_dir(root, file_queue)
    directory_proc = Process(
            target=get_dirs_from_root,
            args=(root, dir_queue, max_filers))
    filer_pool = ProcPool(max_filers, filer, dir_queue, file_queue)
    directory_proc.start()
    filer_pool.start()
    term_count = 0
    while True:
        f = file_queue.get()
        if f == None:
            term_count += 1
        if term_count == max_filers:
            raise StopIteration()
        else:
            if f != None:
                yield f

if __name__ == "__main__":
    print [f for f in grab('/home/rossdylan/Files')]

