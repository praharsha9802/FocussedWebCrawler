from url_normalize import url_normalize
from urllib.parse import urljoin, urlparse
import urllib.robotparser as robot
import requests
import sys
import re

class UrlManagement:
    def __init__(self, url):
        self.url = url
        self.rParser = robot.RobotFileParser()
        try:
            ind = self.url.find('//')
            if requests.head(self.url[:ind+2]+self.getDomain()+'/robots.txt', timeout = 5).status_code > 400:
                self.robotsFlag = False
                return
            self.rParser.set_url(self.url)
            self.rParser.read()
            self.robotsFlag = True
        except Exception as e:
            print(e)
            self.robotsFlag = False
            sys.exit(1)
    
    def normalizeURL(self, url = None):
        if url:
            res = re.search(re.compile(r'\#[a-z]*\b'), url)
            if res:
                url = url.replace(res.group(),'')
            url = url_normalize(url)
            return url
        res = re.search(re.compile(r'\#[a-z]*\b'), self.url)
        if res:
            self.url = self.url.replace(res.group(),'')
        self.url = url_normalize(self.url)
        return self.url
    
    def getURL(self):
        return self.url
    
    def getDomain(self):
        return urlparse(self.url).netloc
    
    def transformRelative(self, url):
        if not(bool(urlparse(url).netloc)):
            url = urljoin(self.url, url)
            return url
        else:
            return url
    
    def getOutlinks(self, urlList):
        resUrlList = []
        urlList = self.getValidLinks(urlList)
        if self.robotsFlag:
            for url in urlList:
                try:
                    if self.rParser.can_fetch('*',url):
                        resUrlList.append(url)
                except Exception:
                    continue
            return resUrlList
    
    def getWaitTime(self):
        if self.robotsFlag:
            delay = self.rParser.crawl_delay('*')
            if delay == None:
                return 1
            else:
                return delay
        return 1
    
    def getValidLinks(self, urlList):
        resUrlList = []
        for url in urlList:
            url = self.transformRelative(url)
            url = self.normalizeURL(url)
            try:
                if 'http://www.' in url or 'https://www.':
                    resUrlList.append(url)
            except Exception as e:
                print(e)
                sys.exit(1)
        return resUrlList
