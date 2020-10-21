from python.src.xml_processor import XMLProcessor
from python.src.index_writer import IndexWriter
from python.src.worker import Worker
from multiprocessing import Pool, Process, Manager, Queue
import json
import os

NUM_WORKERS = 4
LOGGING = False
# LANG = 'eng'
LANG = 'svk'


def run(args):
    w = Worker(args['id'], args['xml_queue'],
               args['index_queue'], LOGGING, LANG)
    w.do_work()


def cleanup_index(line):
    try:
        alt_title, title = line.strip(' []\n').split(':')
        return alt_title, title
    except:
        return None, None


if __name__ == '__main__':
    # paths to the files
    if LANG == 'svk':
        wiki_dump = 'wiki_dump\\skwiki-latest-pages-articles-multistream.xml.bz2'
        index_file = 'indices\\skwiki_index.json'
        tmp_file = 'indices\\svk_tmp_index.txt'
    elif LANG == 'eng':
        wiki_dump = 'wiki_dump\\enwiki-latest-pages-articles-multistream.xml.bz2'
        index_file = 'indices\\enwiki_index.json'
        tmp_file = 'indices\\eng_tmp_index.txt'
    else:
        raise Exception("Language not supported")

    # create the manager used for multiprocessing
    man = Manager()
    # create the queues used
    xml_queue = man.Queue()
    index_queue = man.Queue()

    # instantiate XML processor and index writer
    xml_processor = Process(
        target=XMLProcessor(
            wiki_dump, NUM_WORKERS).parse_xml, args=(xml_queue,))
    index_writer = Process(
        target=IndexWriter(
            tmp_file, LOGGING).write, args=(index_queue,))
    # prepare args for workers
    args = [{'id': i, 'xml_queue': xml_queue, 'index_queue': index_queue}
            for i in range(NUM_WORKERS)]
    # start the processor and index writer processes
    xml_processor.start()
    index_writer.start()
    # start the workers
    with Pool(processes=NUM_WORKERS) as p:
        p.map(run, args)
    # after the file is processed, join the processor and writer
    xml_processor.join()
    index_writer.join()
    # post process the index with workers
    p = Pool(NUM_WORKERS)
    # dictionary that will be constructed from the temporary index
    index_dict = dict()
    with open(tmp_file, mode='r', encoding='utf-8') as file:
        for alt_title, title in p.map(cleanup_index, file):
            if alt_title != None and title != None:
                try:
                    if alt_title not in index_dict[title]:
                        index_dict[title].append(alt_title)
                except:
                    index_dict[title] = [alt_title]
    # dump the constructed dict as JSON
    with open(index_file, mode='w+', encoding='utf-8') as file:
        json.dump(index_dict, file, ensure_ascii=False)
