import time


class Worker:
    def __init__(self, id, source_queue, target_queue, logging):
        self.source_queue = source_queue
        self.target_queue = target_queue
        self.id = id
        self.logging = logging

    def do_work(self):
        while True:
            try:
                page = self.source_queue.get()
                if page == 'kill process':
                    self.target_queue.put('kill process')
                    return
                page['alt_title'] = self._process_page(page['text'])
                self.target_queue.put(page)
                if self.logging:
                    print("WORKER {}:".format(self.id), page['title'])
            except Exception as e:
                print('QUEUE ERROR', e)
                time.sleep(1)

    def _process_page(self, page):
        return ""
