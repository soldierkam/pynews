from wxPython.grid import wxGridTableMessage, wxGRIDTABLE_NOTIFY_ROWS_DELETED, wxGRIDTABLE_NOTIFY_ROWS_APPENDED, wxGRIDTABLE_NOTIFY_COLS_APPENDED, wxGRIDTABLE_NOTIFY_COLS_DELETED

__author__ = 'soldier'

from  wx.grid import Grid
import wx


class UrlsGrid(Grid):
    def __init__(self, parent):
        Grid.__init__(self, parent, -1)
        self.CreateGrid(0, 3)
        self.SetColLabelValue(0, "URL")
        self.SetColLabelValue(1, "Count")
        self.SetColLabelValue(2, "Expanded")

        #self.SetRowLabelValue(0, "1")
        #self.SetCellValue(0, 0, "A")
        #self.SetCellValue(0, 1, "A")

    def update(self, urlsDict):
        i = 0
        self.AppendRows(len(urlsDict.items()) - self.NumberRows)
        for url, count in urlsDict.items():
            self.SetRowLabelValue(i, unicode(i))
            self.SetCellValue(i, 0, url.getUrl())
            self.SetCellValue(i, 1, unicode(count))
            self.SetCellValue(i, 2, url.getExpandedUrl())
            self.SetToolTipString("asdasd")
            i += 1

class Gui:

    def __init__(self):
        self.app = wx.App()
        self.frame = wx.Frame(None, -1, 'simple.py')
        self.grid = UrlsGrid(self.frame)

    def run(self):
        self.frame.Show()
        self.app.MainLoop()

    def updateUrls(self, urlsDict):
        self.grid.update(urlsDict)