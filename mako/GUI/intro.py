"""
The introduction panel contains contact information and provides an interface for HTML text files.
These HTML text files contain a FAQ and step-by-step tutorial for running massoc.
"""

__author__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import wx
import wx.html
import wx.lib.wxpTag
from mako.scripts.utils import _resource_path
import webbrowser


class IntroPanel(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent)
        self.frame = parent
        self.leftsizer = wx.BoxSizer(wx.HORIZONTAL)
        self.topsizer = wx.BoxSizer(wx.VERTICAL)

        btnsize = (150, -1)
        mfs = wx.MemoryFSHandler()
        wx.FileSystem.AddHandler(mfs)

        self.htmlbox = wx.html.HtmlWindow(self, -1, size=(800,1500))
        self.htmlbox.SetPage(welcome)
        self.topsizer.Add(self.htmlbox, 1, wx.ALIGN_CENTER_HORIZONTAL | wx.EXPAND | wx.ALL, 40)

        self.ico = wx.StaticBitmap(self, -1, wx.Bitmap(_resource_path("mako.png"), wx.BITMAP_TYPE_ANY))

        self.menusizer = wx.BoxSizer(wx.VERTICAL)
        self.doc_btn = wx.Button(self, label="Documentation", size=btnsize)
        self.doc_btn.Bind(wx.EVT_BUTTON, self.link_docs)


        self.menusizer.AddSpacer(50)
        self.menusizer.Add(self.ico, 1, wx.ALIGN_CENTER_HORIZONTAL, 5)
        self.menusizer.AddSpacer(50)
        self.menusizer.Add(self.doc_btn, 1, wx.EXPAND, 5)
        self.menusizer.AddSpacer(300)

        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.menusizer)
        self.leftsizer.AddSpacer(50)
        self.leftsizer.Add(self.topsizer)
        self.SetSizerAndFit(self.leftsizer)

    # write documentation in PDF and link
    def link_docs(self, event):
        url = "https://github.com/ramellose/mako/"
        webbrowser.open(url)


welcome="""<h2><em>mako</em>,<br /> a platform for microbial associations.<br /></h2>
Currently, the following features are available:
<ul>
<li>Network + BIOM file storage in a graph database </li>
<li>Multi-network logic operations</li>
<li>Taxonomy-dependent edge agglomeration</li>
</ul>
<p>Welcome to mako! Contact the author at lisa.rottjers (at) kuleuven.be, <br />or visit the <a href="https://github.com/ramellose/">repository</a>. Your feedback is much appreciated!</p>
<p>Currently, you are using mako 0.1.0. This version is still in early alpha. Encountering bugs is highly likely!</p>
"""