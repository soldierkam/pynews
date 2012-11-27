# -*- coding: utf-8 *-*
# import simplejson
from logger import logger

__author__ = 'soldier'

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
        self.SetColLabelValue(1, "Freq. (‰)")
        self.SetColLabelValue(2, "Expanded")
        self.SetColLabelValue(3, "Lang")
        self.SetColLabelValue(4, "Class")
        self.SetColLabelValue(5, "Mark")
        self.DisableCellEditControl()
        self.Bind(grid.EVT_GRID_CELL_LEFT_DCLICK, self._OnLinkClicked)
        self.Bind(grid.EVT_GRID_LABEL_LEFT_CLICK, self._OnLabelLeftClicked)

    def update(self, urls):
        self.__urls = urls
        self._doUpdate()

    def _doUpdate(self):
        i = 0
        self._sort()
        self._filterUrls()
        self._prepareGrid()
        for url, freq, isFinal in self.__urls:
            self._setRow(i, url, freq, isFinal)
            i += 1

    def _urlSortFunction(self, urlFreqAndFinal):
        url = urlFreqAndFinal[0]
        freq = urlFreqAndFinal[1]
        isFinal = urlFreqAndFinal[2]
        if self.__sortColumnId == 0:
            return url.getUrl()
        elif self.__sortColumnId == 1:
            return freq
        elif self.__sortColumnId == 2:
            return url.getExpandedUrl()
        elif self.__sortColumnId == 3:
            return url.lang()
        elif self.__sortColumnId == 4:
            return url.documentClasses()[0] if len(url.documentClasses()) > 0 else ""
        elif self.__sortColumnId == 5:
            return url.mark()
        raise ValueError("Wrong value: " + self.__sortColumnId)

    def _sort(self):
        self.__urls = sorted(self.__urls, key=self._urlSortFunction, reverse=self.__sortDirectionDesc)

    def _filterUrls(self):
        self.__urls = [(url, freq, isFinal) for url, freq, isFinal in self.__urls if not self.__onlyFinalUrls or isFinal]

    def _prepareGrid(self):
        diff = len(self.__urls) - self.NumberRows
        if diff > 0:
            self.AppendRows(diff)
        else:
            self.DeleteRows(numRows=abs(diff))

    def _setRow(self, i, url, freq, isFinal):
        self.SetRowLabelValue(i, unicode(i+1))
        self.SetCellValue(i, 0, url.getUrl())
        self.SetCellValue(i, 1, unicode(freq * 1000))
        self.SetCellValue(i, 2, url.getExpandedUrl())

        self.SetCellValue(i, 3, str(url.lang()))
        self.SetCellBackgroundColour(i, 3, self.__colourForLang(url.lang()))

        self.SetCellValue(i, 4, ', '.join(map(str, url.documentClasses())))
        self.SetCellBackgroundColour(i, 4, self.__colourForClass(url.documentClasses()))

        self.SetCellValue(i, 5, unicode(url.mark()))


    def setOnlyFinalUrls(self, value):
        self.__onlyFinalUrls = value

    def _OnLinkClicked(self, event):
        r = event.GetRow()
        c = event.GetCol()
        url = self.__urls[r][0]
        if c == 5:
            logger.info(simplejson.dumps(url.dump(), indent="\t"))
        elif c == 4:
            Publisher.sendMessage("model.prob_dist", data = url)
        elif c == 3:
            import webbrowser
            webbrowser.open(url.getExpandedUrl())

    def _OnLabelLeftClicked(self, evt):
        col = evt.GetCol()
        if col != -1:
            if col == self.__sortColumnId:
                self.__sortDirectionDesc = not self.__sortDirectionDesc
            else:
                self.__sortColumnId = col
            self._doUpdate()

    def __colourForClass(self, classList):
        if len(classList) == 0:
            return "#FFFFFF"
        if "short" in classList:
            return "#A0A0A0"
        if "medium" in classList:
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
        Publisher.subscribe(self.onModelPaused, "model.paused")
        Publisher.subscribe(self.onModelStarted, "model.started")
        self.Bind(wx.EVT_TIMER, self.onTimerEvent, self.timer)
        self.__paused = True

    def __buildMenuAnalyze(self):
        menu = wx.Menu()
        self.menuStartPause = menu.Append(id=ID_START_PAUSE, text="Start")
        self.menuRefresh = menu.Append(id=ID_REFRESH_URLS, text="Refresh URL's")
        self.Bind(wx.EVT_MENU, self.onStartPauseButtonClick, self.menuStartPause)
        self.Bind(wx.EVT_MENU, self.onRefreshMenuClick, self.menuRefresh)
        return menu

    def __buildMenuView(self):
        menu = wx.Menu()
        self.showOnlyFinalUrls = menu.Append(wx.ID_ANY, 'Show only final', 'Show only final', kind=wx.ITEM_CHECK)
        self.Bind(wx.EVT_MENU, self.onShowFinalUrlsToggle, self.showOnlyFinalUrls)
        return menu

    def run(self):
        self.app.MainLoop()

    def updateUrls(self, msg):
        wx.CallAfter(self.doUpdate, msg.data)

    def doUpdate(self, data):
        cacheMsg = "Cache hit rate: " + str(data["cache"]) + "%"
        end = data["position_end"] if data["position_end"] else "UNKNOWN"
        tweetMsg = "Position: " + str(data["position"]) + "/" + str(end)
        fileMsg = "File: " + str(data["current_file_c"]) + "/" + str(data["last_file_c"])
        self.SetStatusText(cacheMsg + "\t" + tweetMsg + "\t" + fileMsg)
        self.grid.update(data["urls"])

    def onTimerEvent(self, event):
        #self.onRefreshMenuClick(event)
        pass

    def onModelPaused(self, msg):
        self.__paused = True
        self.menuStartPause.SetText("Start")
        self.timer.Stop()

    def onModelStarted(self, msg):
        self.__paused = False
        self.menuStartPause.SetText("Pause")
        self.timer.Start(1000 * 10)

    def onShowFinalUrlsToggle(self, event):
        self.grid.setOnlyFinalUrls(event.Selection == 1)
        self.onRefreshMenuClick(event)

    def onStartPauseButtonClick(self, event):
        if self.__paused:
            Publisher.sendMessage("model.start")
        else:
            Publisher.sendMessage("model.pause")

    def onRefreshMenuClick(self, event):
        logger.info("Request data for GUI")
        Publisher.sendMessage("model.refreshGui")
