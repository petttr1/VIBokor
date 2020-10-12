from xml_processor import XMLProcessor
from index_writer import IndexWriter
from worker import Worker
# from queue_module import Queue

from multiprocessing import Process, Queue

from multiprocessing import Pool, Process, Manager


NUM_WORKERS = 4
LOGGING = False


def run(args):
    w = Worker(args['id'], args['xml_queue'], args['index_queue'], LOGGING)
    w.do_work()


if __name__ == "__main__":
    m = Manager()
    xml_queue = m.Queue()
    index_queue = m.Queue()
    xml_processor = Process(
        target=XMLProcessor(
            'wiki_dump\\skwiki-20200901-pages-articles.xml', NUM_WORKERS).parse_xml, args=(xml_queue,))
    index_writer = Process(
        target=IndexWriter(
            'indices\\skwiki-20200901-pages-articles.txt', LOGGING).write, args=(index_queue,))
    args = [{'id': i, 'xml_queue': xml_queue, 'index_queue': index_queue}
            for i in range(NUM_WORKERS)]
    xml_processor.start()
    index_writer.start()
    with Pool(4) as p:
        p.map(run, args)
    xml_processor.join()
    index_writer.join()

# en_wiki = https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream12.xml-p7054860p8554859.bz2-rss.xml
