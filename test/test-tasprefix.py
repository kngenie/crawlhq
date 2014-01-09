#!/usr/bin/python
from fixture import *
import unittest

import tasprefix

class TasPreifxTestCase(unittest.TestCase):
    def testBasic(self):
        assert tasprefix.prefix('http://www.archive.org') == 'org,archive,'
        assert tasprefix.prefix('http://www.archive.org/movies') == \
            'org,archive,'
        assert tasprefix.prefix('http://www.archive.org:8080/') == \
            'org,archive,'
        assert tasprefix.prefix('http://user:pass@www.archive.org/movies/') \
            == 'org,archive,'

    def testIPaddr(self):
        assert tasprefix.prefix('http://127.2.34.56/foo') == \
            '127.2.34.56'
        # IP V6
        assert tasprefix.prefix('http://[2001:0db8:85a3:08d3:1319:8a2e:0370:7344]/') \
            == '[2001:0db8:85a3:08d3:1319:8a2e:0370:7344]'

    def testMoreDomains(self):
        assert tasprefix.prefix('http://www.example.com/') == 'com,example,'
        assert tasprefix.prefix('http://example.com/') == 'com,example,'
        assert tasprefix.prefix('http://www.yahoo.fr/') == 'fr,yahoo,'
        assert tasprefix.prefix('http://www.foobar.com.au/') == 'au,com,foobar,'
        assert tasprefix.prefix('http://www.virgin.co.uk/') == 'uk,co,virgin,'
        assert tasprefix.prefix('http://www.assigned.public.tokyo.jp') == \
            'jp,tokyo,public,assigned,'

        assert tasprefix.prefix('http://www.bad-site.de') == 'de,bad-site,'
        assert tasprefix.prefix('http://www.archive4u.de') == 'de,archive4u,'


    def testExceptionRules(self):
        assert tasprefix.prefix('http://www.bl.uk') == 'uk,bl,'
        assert tasprefix.prefix('http://subdomain.metro.tokyo.jp') == \
            'jp,tokyo,metro,'
        assert tasprefix.prefix('http://metro.tokyo.jp') == 'jp,tokyo,metro,'

    def testUnknownTLD(self):
        assert tasprefix.prefix('http://www.example.zzz') == 'zzz,example,'

if __name__ == '__main__':
    unittest.main()
