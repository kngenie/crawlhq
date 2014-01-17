from setuptools import setup, find_packages

import os
# tentative - until all hq modules are moved under 'crawlhq' package.
def list_modules():
    return [n[:-3] for n in os.listdir('lib') if n.endswith('.py')]

setup(
    name="crawlhq",
    version="1.0.0",
    author="Kenji Nagahashi",
    author_email="kenji@archive.org",
    description="a hub for a large-scale web crawl",
    url="https://github.com/kngenie/crawlhq",
    license="GPLv3",
    keywords="crawl management",

    package_dir={'':'lib'},
    packages=find_packages('lib'),
    py_modules=list_modules(),
    entry_points={
        'console_scripts':[
            'shuffle = shuffle:main'
            ]
        },
    install_requires=[
        'configobj',
        # 0.3.2 is a version included in Ubuntu Precise. depends on
        # libsnappy-dev package.
        'python-snappy>=0.3.2',
        'web.py>=0.34',
        'kazoo>=1.3',
        ],
    # TODO: use entry_points
    scripts=[
        'bin/processinq-sa',
        'bin/schedule',
        ]
)
