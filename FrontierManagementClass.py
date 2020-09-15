
class FrontierManagement:
    def __init__(self):
        self.frontier = []
        self.visited = dict()
        self.inLinkDt = dict()
    
    def isUrlPresent(self, url):
        try:
            return self.inLinkDt[url]
        except KeyError:
            return False
    
    def getFrontier(self):
        return self.frontier
    
    def insertUrl(self, urlList):
        for url in urlList:
            self.inLinkDt[url[0]] = 1
            self.frontier.append(url)
    def updateVisited(self, urlList):
        for url in urlList:
            self.visited[url[0]] = True

    def sortFrontier(self,k):
        sortedQueue = self.frontier[:k+1]
        sortedQueue = sorted(sortedQueue, key = lambda entry: 0.5*entry[1] + 0.1*self.inLinkDt[entry[0]] + 0.4*entry[3], reverse= True)
        self.frontier = self.frontier[k+1:]
        self.updateVisited(sortedQueue)
        return sortedQueue
    
    def updateInlinks(self, url):
        self.inLinkDt[url] += 1
    
    def getVisited(self):
        return self.visited

    def isVisited(self, url):
        try:
            return self.visited[url]
        except KeyError as e:
            return False
    
    def getLen(self):
        return len(self.frontier)
    
