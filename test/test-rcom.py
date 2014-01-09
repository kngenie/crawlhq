#!/usr/bin/python
import sys
import os
from fixture import *
import unittest

import rcom

class TestObject(object):
    PARAMS = (('a', int),
              ('b', str),
              ('c', bool))
    def __init__(self):
        self.a = 1
        self.b = 'test'
        self.c = True

class RcomTestCase(unittest.TestCase):
    def testGetProperty(self):
        m = rcom.Model(dict(o=TestObject()))
        v = m.get_property('o.a')
        assert v == 1
        v = m.get_property('o.b')
        assert v == 'test'
        v = m.get_property('o.c')
        assert v == True

    def testGetPropertyError(self):
        m = rcom.Model(dict(o=TestObject()))
        try:
            v = m.get_property('x.a')
            self.fail('did not raise')
        except ValueError:
            pass
        try:
            v = m.get_property('o.d')
            self.fail('did not raise')
        except ValueError:
            pass

    def testSetProperty(self):
        m = rcom.Model(dict(o=TestObject()))
        nv, ov = m.set_property('o.a', '2')
        assert nv == 2
        assert ov == 1
        nv, ov = m.set_property('o.b', 'hoge')
        assert nv == 'hoge'
        assert ov == 'test'
        nv, ov = m.set_property('o.c', 'false')
        assert nv == False
        assert ov == True

    def testSetPropertyError(self):
        m = rcom.Model(dict(o=TestObject()))
        try:
            nv, ov = m.set_property('o.a', 'hoge')
            self.fail('did not raise ValueError')
        except ValueError, ex:
            pass

if __name__ == '__main__':
    unittest.main()

