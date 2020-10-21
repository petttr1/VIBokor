import sys
sys.path.append(sys.path[0] + "/..")
import re
import unittest
from src.index_writer import IndexWriter
from src.worker import Worker
from src.xml_processor import XMLProcessor


class TestWorkerRegexes(unittest.TestCase):
    redirect_string = """#REDIRECT [[Platón]]

# redirect[[vcítenie]]

# REDIRECT [[kynizmus]]
"""

    infobox_string = """
{{Infobox Osobnosť
  |Meno = Hans Küng
  |Portrét = Küng3.JPG
  |Popis = švajčiarsky teológ
  |Dátum narodenia = {{dnv|1928|3|19}}
  |Miesto narodenia = [[Sursee]], [[Švajčiarsko]]
  |Dátum úmrtia =
  |Miesto úmrtia =
|}}
"""

    def test_redirect_no_match(self):
        # try to search for redirect in string with no redirect
        self.assertEqual(re.search(
            r"(#REDIRECT|#redirect){1}\ ?\[{2}[A-Za-zá-žÁ-Ž[A-Za-zá-žÁ-Ž$&+,:;=?@#|'\"<>.^*()%!\]-]*\]{2}", self.infobox_string), None)

    def test_redirect(self):
        # try to search for redirect in string with redirect
        self.assertNotEqual(re.search(
            r"(#REDIRECT|#redirect){1}\ ?\[{2}[A-Za-zá-žÁ-Ž[A-Za-zá-žÁ-Ž$&+,:;=?@#|'\"<>.^*()%!\]-]*\]{2}", self.redirect_string), None)

    def test_infobox_found(self):
        # try to search for Infobox in string with Infobox
        self.assertNotEqual(re.search(
            r"\{{2} ?(Infobox){1}[\s\S]*\}{2}", self.infobox_string), None)

    def test_infobox_not_found(self):
        # try to search for Infobox in string without Infobox
        self.assertEqual(re.search(
            r"\{{2} ?(Infobox){1}[\s\S]*\}{2}", self.redirect_string), None)


if __name__ == '__main__':
    unittest.main()
