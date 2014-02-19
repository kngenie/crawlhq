#!/usr/bin/env py.test
from pytest import fail, raises

import rcom

class TestObject(object):
    PARAMS = (('a', int),
              ('b', str),
              ('c', bool))
    def __init__(self):
        self.a = 1
        self.b = 'test'
        self.c = True

def testGetProperty():
    m = rcom.Model(dict(o=TestObject()))
    v = m.get_property('o.a')
    assert v == 1
    v = m.get_property('o.b')
    assert v == 'test'
    v = m.get_property('o.c')
    assert v == True

def testGetPropertyError():
    m = rcom.Model(dict(o=TestObject()))

    with raises(ValueError):
        v = m.get_property('x.a')

    with raises(ValueError):
        v = m.get_property('o.d')

def testSetProperty():
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

def testSetPropertyError():
    m = rcom.Model(dict(o=TestObject()))
    with raises(ValueError):
        nv, ov = m.set_property('o.a', 'hoge')

