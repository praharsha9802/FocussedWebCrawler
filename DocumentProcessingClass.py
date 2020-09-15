from UrlManagementClass import UrlManagement
from nltk.stem import SnowballStemmer
import requests
from bs4 import BeautifulSoup

class DocumentProcessing(UrlManagement):
    fileNo = 1
    wordList = ['death', 'survivor', 'sank', 'passenger', 'capsize', 'wreck', 'rescue', 'accident', 'victim', 'hurricane', 'storm', 'ship', 'ferry']
    def __init__(self, url):
        self.content = ""
        self.text = ""
        self.title = ""
        self.file = ""
        self.stemmer = SnowballStemmer('english')
        UrlManagement.__init__(self, url)
    
    def getHTMLPage(self):
        response = requests.get(self.url, allow_redirects = False, timeout = 5)
        self.meta = dict(response.headers)
        self.content = response.text
    
    def getContent(self):
        return self.content
    
    def validateUrl(self):
        if self.meta['Content-Type'] != 'text/html' or self.meta['Content-Language'] != 'en':
            return False
        else:
            return True
    
    def parseLinks(self):
        outLinks = []
        soup = BeautifulSoup(self.content, 'html.parser')
        for link in soup.find_all('a'):
            href = link.get('href')
            if link.get('class') == 'image' or link.get('class') == "mw-redirect":
                continue
            if href:
                if "cite" in href:
                    continue
                elif "Edit" in href:
                    continue
                elif "File:" in href or "Wikipedia:" in href:
                    continue
                elif "https://javascript:" in href or "http://javascript" in href:
                    continue
            outLinks.append(link.get('href'))
        outLinks = super().getOutlinks(outLinks)
        return outLinks
    
    def parseContent(self):
        soup = BeautifulSoup(self.content, 'html.parser')
        self.title = self.url
        paragraphs = soup.select("p")
        self.text = '\n'.join(list(map(lambda entry: entry.text, paragraphs)))
        
    def writeFile(self):
        self.file = self.file + "<DOC>\n"
        self.file = self.file + "<DOCNO>"
        self.file = self.file + self.title
        self.file = self.file + "</DOCNO>\n"
        self.file = self.file + "<HEAD>"+self.url+"</HEAD>\n"
        self.file = self.file + "<TEXT>\n"
        self.file = self.file + self.text+"\n"
        self.file = self.file + "</TEXT>\n"
        self.file = self.file + "</DOC>"
        fl = open("DATA/fileNo" + str(DocumentProcessing.fileNo) + ".txt",'w')
        DocumentProcessing.fileNo += 1
        fl.write(self.file)
        print("wrote")
        fl.close()