from UrlManagementClass import UrlManagement
from FrontierManagementClass import FrontierManagement
from DocumentProcessingClass import DocumentProcessing
import numpy as np
import requests
from datetime import datetime
import time


class CrawlURLs:
    def __init__(self, seedUrls):
        self.urlList = []
        for i in range(len(seedUrls)):
            tUrl = UrlManagement(seedUrls[i])
            url = tUrl.normalizeURL()
            self.urlList.append([url, 1, 0, 2])
        self.probw = [0.1,0.5,0.4]
        self.linkGraph = dict()
        self.crawlDelay = dict()
    
    def geturlList(self):
        return self.urlList
    
    def crawl(self):
        frontier = FrontierManagement()
        frontier.insertUrl(self.urlList)
        fLength = frontier.getLen()
        fileNo = 1
        while fLength > 0:
            k = [500,1000,5000][np.random.choice(3,p = self.probw)]
            if k > fLength:
                batch = frontier.sortFrontier(fLength)
            else:
                batch = frontier.sortFrontier(k)
            fl = open("test"+str(fileNo)+".txt",'w')
            fileNo = fileNo+1
            fl.write(str(batch))
            fl.close()
            batchUrls = list(map(lambda entry: entry[0], batch))
            for i in range(len(batch)):
                
                try:
                    headers = dict(requests.head(batch[i][0], timeout = 5).headers)
                except Exception as e:
                    print(e,"=",batch[i], i)
                    continue
                if 'Content-Type' not in headers.keys() or 'text/html' not in headers['Content-Type']:
                    print("Not valid = ",batch[i], i)
                    continue
                if 'Content-Language' in headers.keys():
                    if headers['Content-Language'] != 'en':
                        print("Not English = ",batch[i], i)
                        continue
                try:
                    doc = DocumentProcessing(batch[i][0])
                except:
                    continue
                domain = doc.getDomain()
                if domain in self.crawlDelay.keys():
                    time.sleep(self.crawlDelay[domain])
                else:
                    self.crawlDelay[domain] = doc.getWaitTime()
                    time.sleep(self.crawlDelay[domain])
                try:
                    doc.getHTMLPage()
                except Exception as e:
                    print(e,"\nurl = ",batch[i], i)
                    continue
                doc.parseContent()
                doc.writeFile()
                outlinks = doc.parseLinks()
                resOutlinks = []
                if batch[i][0] == 'https://web.archive.org/web/20120122034041/http:/www.maritime-executive.com/article/president-of-rina-resigns-possible-consequence-of-costa-concordia-incident':
                    print("debug points")
                for link in outlinks:
                    if not(frontier.isVisited(link)):
                        if frontier.isUrlPresent(link):
                            frontier.updateInlinks(link)
                            continue
                        elif link in batchUrls:
                            continue
                        else:
                            importance = self.calculateImportance(link)
                            frontier.insertUrl([[link, batch[i][1] + 1, 1, importance]])
                            resOutlinks.append(link)
                self.updateLinkGraph(resOutlinks, batch[i][0])
                print("DONE = ",batch[i])
            fLength = frontier.getLen()
            if DocumentProcessing.fileNo >= 40000:
                return
    
    def calculateImportance(self, link):
        link = link.lower()
        wordList = ['death', 'survivor', 'sank', 'passenger', 'capsize', 'wreck', 'rescue', 'accident', 'victim', 'hurricane', 'storm', 'ship', 'ferry']
        count = 0
        for word in wordList:
            if word in link:
                count = count + 1
        return count

        
    def updateLinkGraph(self, outlinks, url):
        if url in self.linkGraph.keys():
            self.linkGraph[url[0]][0] = outlinks
        else:
            self.linkGraph[url[0]] = [outlinks,[]]
        for link in outlinks:
            if link in self.linkGraph.keys():
                self.linkGraph[link][1].append(url[0])
            else:
                self.linkGraph[link] = [[],[url[0]]]
    
    def getLinkGraph(self):
        return self.linkGraph
    
    
    
    