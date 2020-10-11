from lxml import etree


class XMLProcessor:
    def __init__(self, xml_file, num_workers):
        self.xml_file = xml_file
        self.num_workers = num_workers

    def parse_xml(self, queue):
        for _, elem in etree.iterparse(self.xml_file, events=("start",), remove_blank_text=True):
            title = ""
            text = ""
            if elem.tag.split('}')[-1] == 'page':
                for child in elem.iterchildren():
                    if child.tag.split('}')[-1] == 'title':
                        title = child.text
                    if child.tag.split('}')[-1] == 'revision':
                        for another_child in child:
                            if another_child.tag.split('}')[-1] == 'text':
                                text = another_child.text
                queue.put({'title': title, 'text': text})
                elem.clear()
            else:
                elem.clear()
        for _ in range(self.num_workers):
            queue.put('kill process')
        return
