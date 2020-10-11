import time


class IndexWriter:
    def __init__(self, index_file, logging):
        self.index_file = index_file
        self.logging = logging

    def write(self, queue):
        with open(self.index_file, mode='w', encoding='utf-8') as file:
            while True:
                try:
                    entry = queue.get()
                    if entry == 'kill process':
                        return
                    file.write("Title: {} \n".format(
                        entry['title']))
                    if self.logging:
                        print('INDEX_WRITER:',
                              "Title: {}".format(entry['title']))
                except Exception as e:
                    print('QUEUE EMPTY', e)
                    time.sleep(1)
