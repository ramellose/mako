#!/usr/bin/env python

"""
This interface covers all of massoc's main features.
It allows users to select appropriate settings and export these as the appropriate command line call.
Moreover, it incorporates checks to make sure supplied files are correct.
By visualizing input and output, it provides interactive feedback to users that helps them in their
decision-making process.
To do: write a safe close button that also terminates R script processes.
Right now, the R scripts keep running even if you quit running massoc.
"""

__author__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import wx
import os
from mako.scripts.utils import _resource_path
from pubsub import pub
from mako.GUI.intro import IntroPanel
from mako.GUI.database import BasePanel
from mako.GUI.biom import BiomPanel
from mako.GUI.interface import InterfacePanel
from mako.GUI.analysis import AnalysisPanel
from mako.GUI.wraptab import WrapPanel
import multiprocessing

# the general_settings file is supposed to replace the argparse outcome.
# this dictionary has keys for all parameters and sets them to a default.
general_settings = {'address': 'bolt://localhost:7687',
                    'agglom': None,
                    'cyto': None,
                    'delete': None,
                    'fasta': None,
                    'fp': os.getcwd(),
                    'fraction': [0.5, 1],
                    'meta': None,
                    'neo4j': None,
                    'networks': None,
                    'password': None,
                    'encryption': None,
                    'set': None,
                    'store_config': False,
                    'username': 'neo4j',
                    'variable': None,
                    'weight': True,
                    'write': None}


class BuildFrame(wx.Frame):
    """Constructor"""
    def __init__(self):
        wx.Frame.__init__(self, None, title='massoc', size=(800, 700))

        ico = wx.Icon(_resource_path("mako.png"), wx.BITMAP_TYPE_PNG)
        self.SetIcon(ico)

        p = wx.Panel(self)
        self.nb = wx.Notebook(p)
        self.tab1 = IntroPanel(self.nb)
        self.tab2 = BasePanel(self.nb)
        self.tab3 = BiomPanel(self.nb)
        self.tab4 = InterfacePanel(self.nb)
        self.tab5 = AnalysisPanel(self.nb)
        self.tab6 = WrapPanel(self.nb)

        self.nb.AddPage(self.tab1, "Start")
        self.nb.AddPage(self.tab2, "Connection")
        self.nb.AddPage(self.tab3, "BIOM files")
        self.nb.AddPage(self.tab4, "Access database")
        self.nb.AddPage(self.tab5, "Network analysis")
        self.nb.AddPage(self.tab6, "manta | anuran")

        self.settings = general_settings

        sizer = wx.BoxSizer()
        sizer.Add(self.nb, 1, wx.EXPAND)
        p.SetSizer(sizer)

        # listens to help messages from uncoupled tab files
        self.CreateStatusBar()
        pub.subscribe(self.change_statusbar, 'change_statusbar')
        self.Show()
        pub.subscribe(self.format_settings, 'biom_settings')
        pub.subscribe(self.format_settings, 'database_settings')
        pub.subscribe(self.format_settings, 'interface_settings')
        pub.subscribe(self.format_settings, 'analysis_settings')
        pub.subscribe(self.load_settings, 'load_settings')

    def format_settings(self, msg):
        """
        Listener function for settings from tabs in notebook.
        """
        try:
            for key in msg:
                self.settings[key] = msg[key]
        except:
            pass
        pub.sendMessage('show_settings', msg=self.settings)

    def load_settings(self, msg):
        try:
            for key in msg:
                self.settings[key] = msg[key]
        except:
            pass
        pub.sendMessage('show_settings', msg=self.settings)

    def change_statusbar(self, msg):
        self.SetStatusText(msg)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    app = wx.App(False)
    frame = BuildFrame()
    app.MainLoop()