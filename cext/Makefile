TOP=..
LIBDIR=$(TOP)/lib
TARGETS=$(LIBDIR)/cfpgenerator.so $(LIBDIR)/leveldb.so

$(TARGETS): fpgenerator.cpp leveldb.cpp
	python setup.py build
	find build -name '*.so' -print -exec mv {} $(LIBDIR) ';'

