from lxml import etree


class XMLProcessor:
    """
    Class for stream processing XML files. Reads the file by tags and extracts info. Extracted info is put into a Queue.

    Args:
        xml_file (str): name of the XML file to be processed
        num_workers (int): number of text processing - used to send stop signal at the end of processing the file  
    """

    def __init__(self, xml_file, num_workers):
        self.xml_file = xml_file
        self.num_workers = num_workers

    def parse_xml(self, queue):
        """
        Main function of this class. Extracts data from XML and fills queue used to pass data to data processing units.

        Args:
            queue (multiprocessing.Queue): Queue used to pass extracted data downstream.
        """
        # iteratively parse the XML (stream process)
        for _, elem in etree.iterparse(self.xml_file, events=("start",), remove_blank_text=True):
            title = ""
            text = ""
            # if "page" tag is found:
            if elem.tag.split('}')[-1] == 'page':
                # find title and text tags and extract their text
                for child in elem.iterchildren():
                    if child.tag.split('}')[-1] == 'title':
                        title = child.text
                    if child.tag.split('}')[-1] == 'revision':
                        for another_child in child:
                            if another_child.tag.split('}')[-1] == 'text':
                                text = another_child.text
                # enqueue the title together with text
                queue.put({'title': title, 'text': text})
                # delete the element from memory
                elem.clear()
            else:
                elem.clear()
        # after the whole file is processed, send signal to kill the workers
        for _ in range(self.num_workers):
            queue.put('kill process')
        return
