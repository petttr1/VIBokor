from python.src.xml_processor import XMLProcessor
from python.src.index_writer import IndexWriter
from python.src.worker import Worker
from multiprocessing import Pool, Process, Manager, Queue
import json
import argparse


def run(args):
    """Function which all the workers do when spawned."""
    w = Worker(args['id'], args['xml_queue'],
               args['index_queue'], args['logging'], args['lang'])
    w.do_work()


def cleanup_index(line):
    try:
        alt_title, title = line.strip(' []\n').split(':')
        return alt_title, title
    except:
        return None, None


if __name__ == '__main__':
    # handle argument parsing
    parser = argparse.ArgumentParser(
        description='Run the Wiki Alternate name finder tool.')
    parser.add_argument(
        '--lang', help='Language the wiki file is written in. Defualt = eng', default='eng', required=False)
    parser.add_argument(
        '--wiki_dump', help='Path to your wiki dump file. Recquired format is .bz2. Default = wiki_dump/enwiki-latest-pages-articles-multistream.xml.bz2', default='wiki_dump\\enwiki-latest-pages-articles-multistream.xml.bz2', required=False)
    parser.add_argument(
        '--index_file', help='Path where the resulting index will be stored. Default = indices/enwiki_index.json', default='indices\\enwiki_index.json', required=False)
    parser.add_argument(
        '--num_workers', help='Number of workers to use. Default = 1.', default=1, required=False, type=int)
    parser.add_argument(
        '--debug', help='Whether to use debug mode - print woker and index writer logs', default=False, required=False, type=bool)
    args = parser.parse_args()

    # paths to the files
    wiki_dump = args.wiki_dump
    index_file = args.index_file
    # should be something like /tmp/temp-index on Linux, but let's not be THAT politically correct
    tmp_file = 'indices\\tmp_index.txt'

    # create the manager used for multiprocessing
    man = Manager()
    # create the queues used
    xml_queue = man.Queue()
    index_queue = man.Queue()

    # instantiate XML processor and index writer
    xml_processor = Process(
        target=XMLProcessor(
            wiki_dump, args.num_workers).parse_xml, args=(xml_queue,))
    index_writer = Process(
        target=IndexWriter(
            tmp_file, args.debug).write, args=(index_queue,))
    # prepare args for workers
    worker_args = [{'id': i, 'xml_queue': xml_queue, 'index_queue': index_queue, 'lang': args.lang, 'logging': args.debug}
                   for i in range(args.num_workers)]
    # start the processor and index writer processes
    xml_processor.start()
    index_writer.start()
    # start the workers
    with Pool(processes=args.num_workers) as p:
        p.map(run, worker_args)
    # after the file is processed, join the processor and writer
    xml_processor.join()
    index_writer.join()
    # post process the index with workers
    p = Pool(args.num_workers)
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
