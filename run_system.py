from xml_processor import XMLProcessor
from index_writer import IndexWriter
from worker import Worker
from multiprocessing import Pool, Process, Manager, Queue


NUM_WORKERS = 4
LOGGING = False
LANG = 'eng'
# LANG = 'svk'


def run(args):
    w = Worker(args['id'], args['xml_queue'],
               args['index_queue'], LOGGING, LANG)
    w.do_work()


if __name__ == '__main__':
    if LANG == 'svk':
        wiki_dump = 'wiki_dump\\skwiki-latest-pages-articles-multistream.xml.bz2'
        index_file = 'indices\\skwiki_index.json'
    elif LANG == 'eng':
        # wiki_dump = 'wiki_dump\\enwiki-20201001-pages-articles1.xml-p1p41242.bz2'
        wiki_dump = 'wiki_dump\\enwiki-latest-pages-articles-multistream.xml.bz2'
        index_file = 'indices\\enwiki_index.json'
    else:
        raise Exception("Language not supported")

    m = Manager()
    xml_queue = m.Queue()
    index_queue = m.Queue()
    xml_processor = Process(
        target=XMLProcessor(
            wiki_dump, NUM_WORKERS).parse_xml, args=(xml_queue,))
    index_writer = Process(
        target=IndexWriter(
            index_file, LOGGING).write, args=(index_queue,))
    args = [{'id': i, 'xml_queue': xml_queue, 'index_queue': index_queue}
            for i in range(NUM_WORKERS)]
    xml_processor.start()
    index_writer.start()
    with Pool(processes=NUM_WORKERS) as p:
        p.map(run, args)
    xml_processor.join()
    index_writer.join()
