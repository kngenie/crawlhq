#!/usr/bin/python
#
import web
from pymongo import Connection
import re
import os
import time

urls = (
    '.*', 'App'
    )
#web.config.debug = False
application = web.application(urls, globals()).wsgifunc()

class App:
    def __init__(self):
        pass

    def GET(self):
        conn = Connection()
        db = conn.crawl
        web.header('Content-Type', 'text/plain')
        
        it = db.seeds.find({'nocrawl.active': 1}, {'url': 1})
        s = ''
        for r in it:
            url = r['url']
            s += 'http://%s\n' % url
        conn.disconnect()
        return s
