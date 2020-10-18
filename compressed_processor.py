from bz2 import BZ2File
from itertools import islice
from lxml import etree

INDEX_FILE = "wiki_dump/skwiki-latest-pages-articles-multistream-index.txt"
WIKI_FILE = "wiki_dump/skwiki-latest-pages-articles-multistream.xml.bz2"

BZ_FILE = BZ2File(WIKI_FILE)


def print_page(offset, size):
    ret = BZ_FILE.seek(offset, 0)
    pages = BZ_FILE.read(size)
    print(ret, offset, size)
    print(pages)
    return
    root = etree.XML(pages)
    for _, elem in etree.iterparse(root, events=("start",), remove_blank_text=True):
        title = ""
        text = ""
        # if "page" tag is found:
        if elem.tag.split('}')[-1] == 'page':
            # find title and text tags and extract their text
            for child in elem.iterchildren():
                # we can skip non-article (ns != 0) pages
                if child.tag.split('}')[-1] == 'ns':
                    if int(child.text) != 0:
                        continue
                if child.tag.split('}')[-1] == 'title':
                    title = child.text
                if child.tag.split('}')[-1] == 'revision':
                    for another_child in child:
                        if another_child.tag.split('}')[-1] == 'text':
                            text = another_child.text
            # enqueue the title together with text
            print({'title': title, 'text': text})
            # delete the element from memory
            elem.clear()
        else:
            elem.clear()


def process_wiki_index(index_file):
    with open(index_file, 'r', 8096, encoding='utf-8') as file:
        prev_num = 0
        prev_line = "0:"
        for i, line in enumerate(file):
            # if we begin new 100 pages
            if i % 100 == 0:
                prev_num = int(prev_line.split(':')[0])
                current_num = int(line.split(':')[0])
                print(prev_num, current_num)
                # calculate block byte size
                pages_size = current_num - prev_num
                print_page(prev_num, pages_size)
            if i == 101:
                return
            prev_line = line
            # get the current and next block byte offset


if __name__ == "__main__":
    process_wiki_index(INDEX_FILE)
