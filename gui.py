__author__ = 'soldier'

from  wx.grid import Grid
import wx
from wx.lib.pubsub.pub import Publisher

ID_START_PAUSE = wx.NewId()
ID_REFRESH_URLS = wx.NewId()

def urlSortFunction(t):
    return t[1]

class UrlsGrid(Grid):
    def __init__(self, parent):
        Grid.__init__(self, parent, -1)
        self.CreateGrid(0, 4)
        self.SetColLabelValue(0, "URL")
        self.SetColLabelValue(1, "Freq.")
        self.SetColLabelValue(2, "Expanded")
        self.SetColLabelValue(3, "Class")

    def update(self, urlsDict):
        i = 0
        list = sorted(urlsDict.items(), key=urlSortFunction, reverse=True)
        self.AppendRows(len(list) - self.NumberRows)
        for url, freq in list:
            self.SetRowLabelValue(i, unicode(i+1))
            self.SetCellValue(i, 0, url.getUrl())
            self.SetCellValue(i, 1, unicode(freq))
            self.SetCellValue(i, 2, url.getExpandedUrl())
            self.SetCellValue(i, 3, ', '.join(map(str, url.documentClasses())))
            self.SetCellBackgroundColour(i, 3, self.__colour(url.documentClasses()))
            i += 1

    def __colour(self, classList):
        if len(classList) == 0:
            return "#FFFFFF"
        if "short" in classList:
            return "#A0A0A0"
        if "medium" in classList:
            return "#CCCC66"
        return "#66CC33"


class Gui(wx.Frame):

    def __init__(self):
        self.app = wx.App()
        wx.Frame.__init__(self, None, title='PyNews', pos=(150,150), size=(350,200))

        menuBar = wx.MenuBar()
        menu = wx.Menu()
        self.menuStartPause = menu.Append(id=ID_START_PAUSE, text="Start")
        self.menuRefresh = menu.Append(id=ID_REFRESH_URLS, text="Refresh URL's")
        self.Bind(wx.EVT_MENU, self.onStartPauseButtonClick, self.menuStartPause)
        self.Bind(wx.EVT_MENU, self.onRefreshMenuClick, self.menuRefresh)
        menuBar.Append(menu, "&Analyze")
        self.SetMenuBar(menuBar)

        self.grid = UrlsGrid(self)
        self.CreateStatusBar()
        self.Show()
        Publisher.subscribe(self.updateUrls, "update.urls")
        Publisher.subscribe(self.onModelPaused, "model.paused")
        Publisher.subscribe(self.onModelStarted, "model.started")
        self.__paused = True

    def run(self):
        self.app.MainLoop()

    def updateUrls(self, msg):
        wx.CallAfter(self.doUpdate, msg.data)

    def doUpdate(self, data):
        cacheMsg = "Cache hit rate: " + str(data["cache"]) + "%"
        end = data["position_end"] if data["position_end"] else "UNKNOWN"
        tweetMsg = "Position: " + str(data["position"]) + "/" + end
        fileMsg = "File: " + str(data["current_file_c"]) + "/" + str(data["last_file_c"])
        self.SetStatusText(cacheMsg + "\t" + tweetMsg + "\t" + fileMsg)
        self.grid.update(data["urls"])

    def onModelPaused(self, msg):
        self.__paused = True
        self.menuStartPause.SetText("Start")

    def onModelStarted(self, msg):
        self.__paused = False
        self.menuStartPause.SetText("Pause")

    def onStartPauseButtonClick(self, event):
        if self.__paused:
            Publisher.sendMessage("model.start")
        else:
            Publisher.sendMessage("model.pause")

    def onRefreshMenuClick(self, event):
        Publisher.sendMessage("model.refreshGui")
