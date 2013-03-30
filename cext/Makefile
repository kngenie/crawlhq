TOP=..
LIBDIR=$(TOP)/lib
TARGETS=$(LIBDIR)/cfpgenerator.so $(LIBDIR)/leveldb.so
CXXFLAGS=-g

all: $(TARGETS) mseenrepair

$(TARGETS): fpgenerator.cpp leveldb.cpp
	python setup.py build
	find build -name '*.so' -print -exec mv {} $(LIBDIR) ';'

mseenrepair: mseenrepair.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<
