import time
import re


class Worker:
    """
    Text processing class. Processes text extracted from XML data
    and creates the Index entries.

    Args:
        id (int): id of this worker
        source_queue (multiprocessing.Queue): queue to get data from
        target_queue (multiprocessing.Queue): queue to write processed data to
        logging (bool): whether to log this processes activity
        lang (str): language to use
    """

    def __init__(self, id, source_queue, target_queue, logging, lang):
        self.source_queue = source_queue
        self.target_queue = target_queue
        self.id = id
        self.logging = logging
        self.lang = lang

    def do_work(self):
        """
        Main function of this class. 
        Extracts data from XML text, creates index entries, and fills queue used to pass indices to index writer.
        """
        while True:
            try:
                # get an entry from queue
                page = self.source_queue.get()
                # if the netry is kill siognal, quit
                if page == 'kill process':
                    self.target_queue.put('kill process')
                    return
                # process the text to get an alternate title
                page['alt_title'] = self._process_page(str(page['text']))
                # enter the result into the index queue
                if page['alt_title'] != None:
                    self.target_queue.put(page)
                    if self.logging == True:
                        print("WORKER {}:".format(self.id),
                              page['title'], ':', page['alt_title'])
            # if the queue is empty, wait for a second
            except:
                time.sleep(1)

    def _process_page(self, page):
        """
        Function used to process the text of the page.

        Args:
            page (str): text of the page.
        """
        # if the page contains a #redirect clause, return the text in [[brackets]]
        match = self._contains_redirect(page)
        if match != None:
            return self._parse_redirect(str(match.string))
        # else, if the text contains infobox:
        match = self._contains_redirect(page)
        if match != None:
            # check whether there is alt name
            if self._alt_in_infobox(str(match.string)):
                # if yes, parse and return it
                return self._parse_alt_infobox(str(match.string))
        return None

    def _contains_redirect(self, page):
        """Function used to find out whether the text contains a #redirect clause

        Args:
            page (str): text of a wiki page
        """
        return re.search(r"(#REDIRECT|#redirect){1}\ ?\[{2}[A-Za-zá-žÁ-Ž[A-Za-zá-žÁ-Ž$&+,:;=?@#|'\"<>.^*()%!\]-]*\]{2}", page)

    def _parse_redirect(self, match):
        """Function used to parse the #redirect clause of a wiki page

        Args:
            match (str): matched redirect pattern
        """
        return match.split('{')[0].split('[')[-1][:-2]

    def _contains_infobox(self, page):
        """Function used to find out whether the text contains an infobox

        Args:
            page (str): text of a wiki page
        """
        return re.search(r"\{{2} ?(Infobox){1}[\s\S]*\}{2}", page)

    def _alt_in_infobox(self, match):
        """Function used to find out whether the text contains a alt title in an infobox

        Args:
            page (str): Text of an Infobox
        """
        if self.lang == 'svk':
            return len(re.findall(r"(Natívny názov|Rodné meno|Plné meno|Celý názov|Krátky miestny názov|Dlhý miestny názov).*", match)) > 0
        elif self.lang == 'eng':
            return len(re.findall(r"(official_name|nickname|name|native_name|pseudonym|conventional_long_name|conventional_short_name|fullname|altname).*", match)) > 0

    def _parse_alt_infobox(self, match):
        """Function used to parse alt title in infobox

        Args:
            page (str): Text of an Infobox
        """
        alt_titles = []
        if self.lang == 'svk':
            for single_match in re.findall(r"(Natívny názov|Rodné meno|Plné meno|Celý názov|Krátky miestny názov|Dlhý miestny názov).*", match):
                alt_titles.append(single_match.split('=')[-1])
        elif self.lang == 'eng':
            for single_match in re.findall(r"(official_name|nickname|name|native_name|pseudonym|conventional_long_name|conventional_short_name|fullname|altname|aka|alias).*", match):
                alt_titles.append(single_match.split('=')[-1])
