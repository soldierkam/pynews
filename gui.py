# -*- coding: utf-8 *-*
# import simplejson
from logger import logger
from wx import grid
from  wx.grid import Grid
import wx, simplejson
from wx.lib.pubsub.pub import Publisher

ID_START_PAUSE = wx.NewId()
ID_REFRESH_URLS = wx.NewId()
ID_SHOW_ONLY_FINAL_URLS = wx.NewId()

class UrlsGrid(Grid):
    def __init__(self, parent):
        Grid.__init__(self, parent, -1)
        self.__urls = []
        self.__onlyFinalUrls = False
        self.__sortColumnId = 1
        self.__sortDirectionDesc = True
        self.CreateGrid(0, 6)
        self.SetColLabelValue(0, "URL")
        self.SetColLabelValue(1, "Tweets count")
        self.SetColLabelValue(2, "Expanded")
        self.SetColLabelValue(3, "Lang")
        self.SetColLabelValue(4, "Class")
        self.SetColLabelValue(5, "Title")
        self.DisableCellEditControl()
        self.Bind(grid.EVT_GRID_CELL_LEFT_DCLICK, self._OnLinkClicked)
        self.Bind(grid.EVT_GRID_LABEL_LEFT_CLICK, self._OnLabelLeftClicked)

    def update(self, urls):
        self.__urls = urls
        self._doUpdate()

    def _doUpdate(self):
        i = 0
        self._sort()
        self._prepareGrid()
        for url in self.__urls:
            self._setRow(i, url, len(url["tweets"]))
            i += 1

    def _urlSortFunction(self, url):
        freq = len(url["tweets"])
        if self.__sortColumnId == 0:
            return url["tco"]
        elif self.__sortColumnId == 1:
            return freq
        elif self.__sortColumnId == 2:
            return url["url"]
        elif self.__sortColumnId == 3:
            return url["lang"]
        elif self.__sortColumnId == 4:
            return url["cat"]
        raise ValueError("Wrong value: " + str(self.__sortColumnId))

    def _sort(self):
        self.__urls = sorted(self.__urls, key=self._urlSortFunction, reverse=self.__sortDirectionDesc)

    def _prepareGrid(self):
        diff = len(self.__urls) - self.NumberRows
        if diff > 0:
            self.AppendRows(diff)
        else:
            self.DeleteRows(numRows=abs(diff))

    def _setRow(self, i, url, count):
        self.SetRowLabelValue(i, unicode(i+1))
        self.SetCellValue(i, 0, url["tco"])
        self.SetCellValue(i, 1, unicode(count))
        self.SetCellValue(i, 2, url["url"])

        self.SetCellValue(i, 3, str(url["lang"]))
        self.SetCellBackgroundColour(i, 3, self.__colourForLang(url["lang"]))

        self.SetCellValue(i, 4, url["cat"] + "," + url["len"] )
        self.SetCellBackgroundColour(i, 4, self.__colourForClass(url["len"]))

        self.SetCellValue(i, 5, unicode(url["title"]))


    def setOnlyFinalUrls(self, value):
        self.__onlyFinalUrls = value

    def _OnLinkClicked(self, event):
        r = event.GetRow()
        c = event.GetCol()
        url = self.__urls[r][0]
        if c == 5:
            logger.info(simplejson.dumps(url, indent="\t"))
        elif c == 4:
            Publisher.sendMessage("model.prob_dist", data = url)
        elif c == 3:
            import webbrowser
            webbrowser.open(url["url"])

    def _OnLabelLeftClicked(self, evt):
        col = evt.GetCol()
        if col != -1:
            if col == self.__sortColumnId:
                self.__sortDirectionDesc = not self.__sortDirectionDesc
            else:
                self.__sortColumnId = col
            self._doUpdate()

    def __colourForClass(self, len):
        if "short" == len:
            return "#A0A0A0"
        if "medium" == len:
            return "#CCCC66"
        return "#66CC33"

    def __colourForLang(self, lang):
        if "en" == lang:
            return "#66CC33"
        return "#FFFFFF"

class Gui(wx.Frame):

    def __init__(self):
        self.app = wx.App()
        wx.Frame.__init__(self, None, title='PyNews', pos=(150,150), size=(350,200))

        menuBar = wx.MenuBar()
        menuBar.Append(self.__buildMenuAnalyze(), "&Analyze")
        menuBar.Append(self.__buildMenuView(), "&View")
        self.SetMenuBar(menuBar)

        self.grid = UrlsGrid(self)
        self.CreateStatusBar()
        self.Show()
        self.timer = wx.Timer(self)
        Publisher.subscribe(self.updateUrls, "update.urls")
        Publisher.subscribe(self.updateStatusBar, "update.statusBar")
        Publisher.subscribe(self.onModelPaused, "model.paused")
        Publisher.subscribe(self.onModelStarted, "model.started")
        self.Bind(wx.EVT_TIMER, self.onTimerEvent, self.timer)
        self.__paused = True
        self.timer.Start(1000 * 10)

    def __buildMenuAnalyze(self):
        menu = wx.Menu()
        self.menuStartPause = menu.Append(id=ID_START_PAUSE, text="Start")
        self.menuRefresh = menu.Append(id=ID_REFRESH_URLS, text="Refresh URL's")
        self.Bind(wx.EVT_MENU, self.onStartPauseButtonClick, self.menuStartPause)
        self.Bind(wx.EVT_MENU, self.onRefreshMenuClick, self.menuRefresh)
        return menu

    def __buildMenuView(self):
        menu = wx.Menu()
        self.showTreeMap = menu.Append(wx.ID_ANY, 'Show TreeMap', 'Show TreeMap')
        self.Bind(wx.EVT_MENU, self.onShowTreeMap, self.showTreeMap)
        return menu

    def run(self):
        self.app.MainLoop()

    def updateUrls(self, msg):
        wx.CallAfter(self.doUpdate, msg.data)

    def updateStatusBar(self, msg):
        wx.CallAfter(self.doUpdateStatusBar, msg.data)

    def doUpdate(self, data):
        self.grid.update(data["urls"])

    def doUpdateStatusBar(self, data):
        cacheMsg = "Cache hit rate: " + str(data["cache"]) + "%"
        end = data["position_end"] if data["position_end"] else "UNKNOWN"
        tweetMsg = "Position: " + str(data["position"]) + "/" + str(end)
        fileMsg = "File: " + str(data["current_file_c"]) + "/" + str(data["last_file_c"])
        self.SetStatusText(cacheMsg + "\t" + tweetMsg + "\t" + fileMsg)

    def onTimerEvent(self, event):
        logger.info("Request status for GUI")
        Publisher.sendMessage("model.refreshStatusBar")

    def onModelPaused(self, msg):
        self.__paused = True
        self.menuStartPause.SetText("Start")

    def onModelStarted(self, msg):
        self.__paused = False
        self.menuStartPause.SetText("Pause")

    def onShowTreeMap(self, event):
        Publisher.sendMessage("model.showTreeMap")

    def onStartPauseButtonClick(self, event):
        if self.__paused:
            Publisher.sendMessage("model.start")
        else:
            Publisher.sendMessage("model.pause")

    def onRefreshMenuClick(self, event):
        logger.info("Request data for GUI")
        Publisher.sendMessage("model.refreshGui")
