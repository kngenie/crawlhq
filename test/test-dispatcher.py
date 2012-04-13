#
import testhelper
import unittest

from dispatcher import *

class WorksetMapperTestCase(unittest.TestCase):
    def setUp(self):
        self.mapper = WorksetMapper(8)
        
    def testHosthash(self):
        h1 = self.mapper.hosthash('http://www.archive.org/')

        h2 = self.mapper.hosthash('http://archive.org/')
        assert h1 == h2

        h3 = self.mapper.hosthash('http://download.archive.org/')
        assert h3 == h1

        h4 = self.mapper.hosthash('http://www.yahoo.com/')
        assert h4 != h1

    def testWorkset(self):
        curi = dict(u='http://www.archive.org/')
        ws1 = self.mapper.workset(curi)
        assert 0 <= ws1 <= 255, '%s is not within 8 bits' % ws1

if __name__ == '__main__':
    unittest.main()
