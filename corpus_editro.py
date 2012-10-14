# -*- coding: utf-8 *-*
__author__ = 'soldier'

from  wx.grid import Grid
import wx
import os
from wx.lib.pubsub.pub import Publisher
from wx import grid
from wx.lib import newevent
from logger import logger
from lang import LangDetect
import shelve
import codecs

ID_SAVE = wx.NewId()
UrlSelectionChanged, EVT_URL_SELECTION_CHANGED = newevent.NewEvent()

def urlSortFunction(t):
    return t.url()

class UrlsGrid(Grid):
    def __init__(self, model, parent):
        Grid.__init__(self, parent)
        self.__defaultClass = model.defaultClass()
        self.CreateGrid(0, 2)
        self.SetColLabelValue(0, "URL")
        self.SetColLabelValue(1, "Type")
        #self.SetDefaultColSize(200, False)
        self.__editor = grid.GridCellChoiceEditor(model.classes())
        self.Bind(grid.EVT_GRID_SELECT_CELL, self._OnSelectedCell)
        self.Bind(grid.EVT_GRID_CELL_LEFT_DCLICK, self._OnLinkClicked)
        self.Bind(grid.EVT_GRID_CELL_CHANGE, self._OnCellEdit)
        self.Bind(grid.EVT_GRID_EDITOR_SHOWN, self._OnEditorShown)
        self.__ref = None
        self.__currentSelection = None

    def update(self, urls):
        i = 0
        self.AppendRows(len(urls) - self.NumberRows)
        self.__ref = {}
        for url in sorted(urls, key=urlSortFunction, reverse=True):
            self.SetRowLabelValue(i, unicode(i+1))
            self.SetCellValue(i, 0, url.url())
            self.SetReadOnly(i, 0)
            self.SetCellEditor(i, 1, self.__editor);
            self.SetCellValue(i, 1, url.klass())
            self.__ref[i] = url
            i += 1
        self.AutoSizeColumns()
        self.SetColSize(1, 100)

    def _OnEditorShown(self, event):
        if not self.GetCellValue(event.GetRow(), 1):
            self.SetCellValue(event.GetRow(), 1, self.__defaultClass)
            self.__currentSelection.setKlass(self.__defaultClass)

    def _OnSelectedCell( self, event ):
        if self.__ref:
            newSelection = self.__ref[ event.GetRow() ]
            if self.__currentSelection != newSelection:
                self.__currentSelection = newSelection
                evt = UrlSelectionChanged(url=self.__currentSelection)
                wx.PostEvent(self, evt)
        event.Skip()

    def _OnCellEdit(self, event):
        if self.__currentSelection:
            klass = self.GetCellValue(event.GetRow(), 1)
            self.__currentSelection.setKlass(klass)
        event.Skip()

    def _OnLinkClicked(self, event):
        r = event.GetRow()
        if event.Col == 0:
            href = self.__ref[r].url()
            import webbrowser
            webbrowser.open(href)

class Gui(wx.Frame):

    def __init__(self, model):
        self.app = wx.App()
        wx.Frame.__init__(self, None, title='PyNews', pos=(150,150), size=(750,500))

        menuBar = wx.MenuBar()
        menu = wx.Menu()
        self.menuSave = menu.Append(id=ID_SAVE, text="Save")
        self.Bind(wx.EVT_MENU, self.onSave, self.menuSave)
        menuBar.Append(menu, "&File")
        self.SetMenuBar(menuBar)

        box = wx.BoxSizer(wx.VERTICAL)

        self.grid = UrlsGrid(model, self)
        self.CreateStatusBar()
        self.grid.update(model.data())

        self.textarea = wx.TextCtrl(self, -1, style=wx.TE_MULTILINE|wx.BORDER_SUNKEN|wx.TE_READONLY|wx.TE_RICH2, size=(200,200))
        box.Add(self.grid, 1, wx.EXPAND)
        box.Add(self.textarea, 1, wx.EXPAND)

        self.grid.Bind(EVT_URL_SELECTION_CHANGED, self._OnUrlSelectionChanged)

        self.SetAutoLayout(True)
        self.SetSizer(box)
        self.Layout()
        self.Show()

    def _OnUrlSelectionChanged(self, event):
        urlObj = event.url
        self.textarea.Clear()
        self.textarea.WriteText(urlObj.text())
        event.Skip()

    def run(self):
        self.app.MainLoop()

    def updateUrls(self, msg):
        wx.CallAfter(self.doUpdate, msg.data)

    def onSave(self, ev):
        Publisher.sendMessage("model.save")

    def doUpdate(self, data):
        cacheMsg = "Cache hit rate: " + str(data["cache"]) + "%"
        end = data["position_end"] if data["position_end"] else "UNKNOWN"
        tweetMsg = "Position: " + str(data["position"]) + "/" + end
        fileMsg = "File: " + str(data["current_file_c"]) + "/" + str(data["last_file_c"])
        self.SetStatusText(cacheMsg + "\t" + tweetMsg + "\t" + fileMsg)
        self.grid.update(data["urls"])

class RowModel():

    def __init__(self, url, text, klass):
        self.__url = url
        self.__text = text
        self.__klass = klass

    def url(self):
        return self.__url

    def text(self):
        return self.__text

    def klass(self):
        return self.__klass if self.__klass else ""

    def setKlass(self, klass):
        self.__klass = klass

    def writeTextTo(self, filename):
        f = codecs.open(filename, "w", encoding="UTF-8")
        f.write(self.url())
        f.write("\n")
        f.write(self.text())
        f.close()

class Model():

    def __init__(self, mainDir, input):
        self.__mainDir = mainDir
        self.__input = input
        self.__langId = LangDetect()
        data = shelve.open(self.__input)
        self.__data = []
        url2klass = self.__readKlassFile()
        logger.info("Read shelve...")
        for item in data.itervalues():
            text = item["text"]
            url = item["url"]
            klass = self.__getKlass(url2klass, url)
            if not self.__ignorable(text, url):
                self.__data.append(RowModel(url, text, klass))
        logger.info("Done " + str(len(self.__data)))
        Publisher.subscribe(self._onSave, "model.save")

    def __ignorable(self, text, url):
        lang = self.__langId.detect(text)
        result = False
        if text == "ERROR":
            result = True
        elif lang != "en":
            result = True;
        elif text.startswith("Load new Tweets") or text.startswith("Suggested Language (we have set your preference to this)") or text.startswith("Embed this Photo"):
            result = True
        if result:
            logger.info("Skip " +str(lang)+ ": " + text[:50] + "... (" + url + ")")
        return result

    def _onSave(self, msg):
        self.save()

    def __getKlass(self, url2klass, url):
        if url2klass.has_key(url):
            return url2klass[url]
        else:
            return None

    def classes(self):
        return ["article", "news", "other", "announce", "blog"]

    def defaultClass(self):
        return "other"

    def data(self):
        return self.__data

    def __readKlassFile(self):
        filename = os.path.join(self.__mainDir, "cats.txt")
        urlToKlass = {}
        if os.path.exists(filename) and os.path.isfile(filename):
            f = codecs.open(filename, 'r', encoding="UTF-8")
            for line in f.readlines():
                file, cat = line.split()
                urlToKlass[self.__readUrlFromFile(file)] = cat
            logger.info("Read cats.txt: " + str(len(urlToKlass)))
        else:
            logger.info("No cats.txt")
        return urlToKlass

    def __readUrlFromFile(self, filename):
        filename = os.path.join(self.__mainDir, filename)
        f = open(filename, "r")
        url = f.readline()
        f.close()
        return url.replace("\n", "")

    def save(self):
        if not os.path.exists(self.__mainDir):
            os.makedirs(self.__mainDir)
        self.__writeKlassFile()
        self.__writeTextFiles()

    def __writeKlassFile(self):
        filenameTmp = os.path.join(self.__mainDir, "cats.txt.tmp")
        filenameDsc = os.path.join(self.__mainDir, "cats.txt")
        f = codecs.open(filenameTmp, 'w', encoding="UTF-8")
        i = 1
        for url in self.__data:
            klass = url.klass()
            if klass:
                name = "data/" + str(i) + ".txt"
                i += 1
                f.write(name + " " + klass + "\n")
        f.close()
        if os.path.exists(filenameDsc):
            os.remove(filenameDsc)
        os.rename(filenameTmp, filenameDsc)

    def __writeTextFiles(self):
        dirname = os.path.join(self.__mainDir, "data")
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        i = 1
        for url in self.__data:
            if url.klass():
                filename = os.path.join(dirname, str(i) + ".txt")
                i += 1
                url.writeTextTo(filename)


def main():
    mainDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/corpus2"
    input="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/tweets/cache"
    logger.info("Start app")
    model = Model(mainDir, input)
    gui = Gui(model)
    gui.run()
    logger.info("Exit app")

if __name__ == "__main__":
    main()