import time
from collections import OrderedDict
import json


class IndexWriter:
    """
    Class for writing the final index file. Reads processed data from Queue and wrties them into a file

    Args:
        index_file (str): name of the index file to be created
        logging (bool): whether to log what this process is doing
    """

    def __init__(self, index_file, logging):
        self.index_file = index_file
        self.logging = logging

    def write(self, queue):
        """
        Main function of this class. Reads processed data from queue and writes them into a index file.

        Args:
            queue (multiprocessing.Queue): Queue used to read processed data
        """
        # Open the file for writing
        with open(self.index_file, mode='w', encoding='utf-8') as file:
            while True:
                try:
                    # get an entry from queue
                    entry = queue.get()
                    # if it is a kill signal quit
                    if entry == 'kill process':
                        self.__cleaup_index()
                        return
                    # else write the data to a file
                    file.write("""{}:{} \n""".format(
                        entry['title'].strip(' []\n'), entry['alt_title'].strip(' []\n')))
                    if self.logging:
                        print('INDEX_WRITER:',
                              "Title: {}".format(entry['title']))
                # if the queue is empty, wait for a second
                except Exception as e:
                    print('QUEUE EMPTY', e)
                    time.sleep(1)

    def __cleaup_index(self):
        with open(self.index_file, mode='r', encoding='utf-8') as file:
            index_dict = {}
            for line in file:
                try:
                    alt_title, title = line.strip(' []\n').split(':')
                except:
                    print(line, line.split(':'))
                    continue
                try:
                    index_dict[title].append(alt_title)
                except:
                    index_dict[title] = [alt_title]
        with open(self.index_file, mode='w', encoding='utf-8') as file:
            json.dump(index_dict, file, ensure_ascii=False)
