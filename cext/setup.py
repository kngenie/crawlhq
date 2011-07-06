from distutils.core import setup, Extension
import sys

module1 = Extension('cfpgenerator',
                    language='c++',
                    sources=['pythonif.cpp', 'fpgenerator.cpp'])
module2 = Extension('leveldb',
                    language='c++',
                    sources=['leveldb.cpp'],
                    libraries=['leveldb'])

setup(name='cFPGenerator',
      version='0.1',
      description='C version of FPGenerator',
      author='Kenji Nagahashi',
      author_email='kenji@archive.org',
      long_description='''this module is a C-port of st.ata.util.FPGenerator
class included in Heritrix 3.''',
      ext_modules=[module1, module2])
