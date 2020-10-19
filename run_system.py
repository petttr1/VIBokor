from xml_processor import XMLProcessor
from index_writer import IndexWriter
from worker import Worker
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
    man = Manager()
    index_dict = dict()
    if LANG == 'svk':
        wiki_dump = 'wiki_dump\\skwiki-latest-pages-articles-multistream.xml.bz2'
        index_file = 'indices\\skwiki_index.json'
        tmp_file = 'indices\\svk_tmp_index.txt'
    elif LANG == 'eng':
        # wiki_dump = 'wiki_dump\\enwiki-20201001-pages-articles1.xml-p1p41242.bz2'
        wiki_dump = 'wiki_dump\\enwiki-latest-pages-articles-multistream.xml.bz2'
        index_file = 'indices\\enwiki_index.json'
        tmp_file = 'indices\\eng_tmp_index.txt'
    else:
        raise Exception("Language not supported")

    xml_queue = man.Queue()
    index_queue = man.Queue()
    xml_processor = Process(
        target=XMLProcessor(
            wiki_dump, NUM_WORKERS).parse_xml, args=(xml_queue,))
    index_writer = Process(
        target=IndexWriter(
            tmp_file, LOGGING).write, args=(index_queue,))
    args = [{'id': i, 'xml_queue': xml_queue, 'index_queue': index_queue}
            for i in range(NUM_WORKERS)]
    xml_processor.start()
    index_writer.start()
    with Pool(processes=NUM_WORKERS) as p:
        p.map(run, args)
    xml_processor.join()
    index_writer.join()
    print('SORTING INDEX')
    p = Pool(NUM_WORKERS)
    with open(tmp_file, mode='r', encoding='utf-8') as file:
        for alt_title, title in p.map(cleanup_index, file):
            if alt_title != None and title != None:
                try:
                    if alt_title not in index_dict[title]:
                        index_dict[title].append(alt_title)
                except:
                    index_dict[title] = [alt_title]

    with open(index_file, mode='w+', encoding='utf-8') as file:
        json.dump(index_dict, file, ensure_ascii=False)
    # os.remove(tmp_file)
