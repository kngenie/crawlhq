#!/usr/bin/env python
from setuptools import setup, find_packages, Extension

import sys
import os
import glob
# tentative - until all hq modules are moved under 'crawlhq' package.
def list_modules():
    return [n[:-3] for n in os.listdir('lib')
            if not n.startswith('.') and n.endswith('.py')]

ext_modules=[
    Extension('cfpgenerator', ['cext/pythonif.cpp', 'cext/fpgenerator.cpp'])
    ]
# XXX linux only
if os.path.isfile('/usr/lib/libleveldb.a'):
    ext_modules.append(Extension('leveldb', ['cext/leveldb.cpp'],
                                 libraries=['leveldb', 'snappy']))

# probably we could make mseenrepair an extension module.
action = sys.argv and sys.argv[0]
if action in ("install", "develop"):
    os.system("cd cext; make")

setup(
    name="crawlhq",
    version="1.0.0p3",
    author="Kenji Nagahashi",
    author_email="kenji@archive.org",
    description="a hub for a large-scale web crawl",
    url="https://github.com/kngenie/crawlhq",
    license="GPLv3",
    keywords="crawl management",

    package_dir={'':'lib'},
    packages=find_packages('lib'),
    py_modules=list_modules(),
    ext_modules=ext_modules,
    zip_safe=False,
    entry_points={
        'console_scripts':[
            'shuffle = shuffle:main',
            'processinq-sa = inqprocessor:main_standalone'
            ]
        },
    install_requires=[
        'configobj',
        # 0.3.2 is a version included in Ubuntu Precise. depends on
        # libsnappy-dev package.
        'python-snappy>=0.3.2',
        'web.py>=0.34',
        'kazoo>=1.3',
        'pymongo>=2.4.1'
        ],
    tests_require=[
        'pytest'
        ],
    # TODO: use entry_points
    scripts=[
        'bin/schedule',
        'cext/mseenrepair',
        ],
    data_files=[
        (os.path.join(sys.prefix, d), ff) for d, ff in
        {
        'conf': ['conf/apache2.inc'],
        'ws': glob.glob('ws/*.py'),
        'ws/t': glob.glob('ws/t/*.html'),
        'static': glob.glob('static/*.css')+glob.glob('static/*.js'),
        'static/img': glob.glob('static/img/*')
        }.items()
        ]
)
