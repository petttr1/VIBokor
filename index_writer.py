import time
from collections import OrderedDict
import json
import os


class IndexWriter:
    """
    Class for writing the final index file. Reads processed data from Queue and wrties them into a file

    Args:
        index_file (str): name of the index file to be created
        logging (bool): whether to log what this process is doing
    """

    def __init__(self, index_file, logging):
        self.index_file = index_file
        self.tmp_file = "".join(self.index_file.split('\\')[
                                :-1]) + '\\temp_index.txt'
        self.logging = logging

    def write(self, queue):
        """
        Main function of this class. Reads processed data from queue and writes them into a index file.

        Args:
            queue (multiprocessing.Queue): Queue used to read processed data
        """
        # Open the file for writing
        with open(self.tmp_file, mode='w', encoding='utf-8') as file:
            while True:
                try:
                    # get an entry from queue
                    entry = queue.get()
                    # if it is a kill signal quit
                    if entry == 'kill process':
                        break

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

        self.__cleaup_index()
        return

    def __cleaup_index(self):
        # build a dictionary from entries in the file changing the mapping
        with open(self.tmp_file, mode='r', encoding='utf-8') as file:
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
        with open(self.index_file, mode='w+', encoding='utf-8') as file:
            json.dump(index_dict, file, ensure_ascii=False)
        os.remove(self.tmp_file)
