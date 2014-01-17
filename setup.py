from setuptools import setup, find_packages

setup(
    name="crawlhq",
    version="1.0.0",
    author="Kenji Nagahashi",
    author_email="kenji@archive.org",
    description="a hub for a large-scale web crawl",
    license="GPLv3",
    keywords="crawl management"

    packages=find_packages('lib'),
)
