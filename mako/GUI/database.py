"""
The database panel allows users to start up a previously configured Neo4j database.
"""

__author__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

from threading import Thread
import wx
from wx.lib.pubsub import pub
from mako.scripts.base import start_base
from mako.scripts.utils import WxTextCtrlHandler, _resource_path, query
import webbrowser

import logging.handlers

logger = logging.getLogger()


class BasePanel(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent)
        # subscribe to inputs from tabwindow
        self.frame = parent

        btnsize = (300, -1)
        boxsize = (700, 400)
        # adds columns

        self.settings = {'fp': _resource_path(''),
                         'neo4j': '',
                         'username': 'neo4j',
                         'password': 'neo4j',
                         'address': 'bolt://localhost:7687',
                         'start': False,
                         'clear': False,
                         'quit': False,
                         'store_config': True,
                         'check': False}

        # defines columns
        self.rightsizer = wx.BoxSizer(wx.VERTICAL)
        self.leftsizer = wx.BoxSizer(wx.VERTICAL)
        self.topsizer = wx.BoxSizer(wx.HORIZONTAL)
        self.bottomsizer = wx.BoxSizer(wx.VERTICAL)
        self.fullsizer = wx.BoxSizer(wx.VERTICAL)
        self.paddingsizer = wx.BoxSizer(wx.HORIZONTAL)

        # local database grid
        self.local_txt = wx.StaticText(self, label='Only use this section if you\n'
                                                   'want to run the database locally.')
        # Opening neo4j folder
        self.neo_btn = wx.Button(self, label="Select Neo4j folder", size=btnsize)
        self.neo_btn.Bind(wx.EVT_BUTTON, self.open_neo)
        self.neo_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.neo_txt = wx.TextCtrl(self, size=btnsize)

        # set up database
        self.data_button = wx.Button(self, label='Launch database', size=btnsize)
        self.data_button.Bind(wx.EVT_MOTION, self.update_help)
        self.data_button.Bind(wx.EVT_BUTTON, self.start_database)

        # close database
        self.close_button = wx.Button(self, label='Close database', size=btnsize)
        self.close_button.Bind(wx.EVT_MOTION, self.update_help)
        self.close_button.Bind(wx.EVT_BUTTON, self.close_database)

        # actions
        # test database
        self.test_button = wx.Button(self, label='Test connection', size=btnsize)
        self.test_button.Bind(wx.EVT_MOTION, self.update_help)
        self.test_button.Bind(wx.EVT_BUTTON, self.test)

        # clear database
        self.clear_button = wx.Button(self, label='Clear database', size=btnsize)
        self.clear_button.Bind(wx.EVT_MOTION, self.update_help)
        self.clear_button.Bind(wx.EVT_BUTTON, self.clear)

        # open database in browser
        self.data_browser = wx.Button(self, label='Open database in browser', size=btnsize)
        self.data_browser.Bind(wx.EVT_MOTION, self.update_help)
        self.data_browser.Bind(wx.EVT_BUTTON, self.open_browser)

        # check database
        self.check_button = wx.Button(self, label='Check database', size=btnsize)
        self.check_button.Bind(wx.EVT_MOTION, self.update_help)
        self.check_button.Bind(wx.EVT_BUTTON, self.check_database)

        # General database info
        self.address_txt = wx.StaticText(self, label='Neo4j database address')
        self.address_box = wx.TextCtrl(self, value='bolt://localhost:7687', size=btnsize)
        self.username_txt = wx.StaticText(self, label='Neo4j username and password')
        self.username_box = wx.TextCtrl(self, value='neo4j', size=btnsize)
        self.pass_box = wx.TextCtrl(self, value='neo4j', size=btnsize)
        self.address_box.Bind(wx.EVT_TEXT, self.update_address)
        self.address_txt.Bind(wx.EVT_MOTION, self.update_help)
        self.address_box.Bind(wx.EVT_MOTION, self.update_help)
        self.username_box.Bind(wx.EVT_TEXT, self.update_username)
        self.username_box.Bind(wx.EVT_MOTION, self.update_help)
        self.pass_box.Bind(wx.EVT_TEXT, self.update_pass)
        self.pass_box.Bind(wx.EVT_MOTION, self.update_help)

        # Logger
        self.logtxt = wx.StaticText(self, label='Logging panel')
        self.logbox = wx.TextCtrl(self, value='', size=boxsize)
        self.logbox.Bind(wx.EVT_MOTION, self.update_help)
        self.logbox.SetForegroundColour(wx.WHITE)
        self.logbox.SetBackgroundColour(wx.BLACK)
        handler = WxTextCtrlHandler(self.logbox)
        logger.addHandler(handler)

        self.leftsizer.AddSpacer(50)
        self.leftsizer.Add(self.local_txt, flag=wx.ALIGN_CENTER)
        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.neo_btn, flag=wx.ALIGN_LEFT)
        self.leftsizer.Add(self.neo_txt, flag=wx.ALIGN_LEFT)
        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.data_button, flag=wx.ALIGN_LEFT)
        self.leftsizer.Add(self.close_button, flag=wx.ALIGN_LEFT)
        self.leftsizer.AddSpacer(20)

        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.address_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.Add(self.address_box, flag=wx.ALIGN_LEFT)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.username_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.Add(self.username_box, flag=wx.ALIGN_LEFT)
        self.rightsizer.Add(self.pass_box, flag=wx.ALIGN_LEFT)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.test_button, flag=wx.ALIGN_LEFT)
        self.rightsizer.Add(self.clear_button, flag=wx.ALIGN_LEFT)
        self.rightsizer.Add(self.data_browser, flag=wx.ALIGN_LEFT)
        self.rightsizer.Add(self.check_button, flag=wx.ALIGN_LEFT)


        self.bottomsizer.AddSpacer(50)
        self.bottomsizer.Add(self.logtxt, flag=wx.ALIGN_LEFT)
        self.bottomsizer.AddSpacer(10)
        self.bottomsizer.Add(self.logbox, flag=wx.ALIGN_CENTER)

        self.topsizer.AddSpacer(40)
        self.topsizer.Add(self.leftsizer)
        self.topsizer.AddSpacer(40)
        self.topsizer.Add(self.rightsizer)
        self.fullsizer.Add(self.topsizer)
        self.fullsizer.Add(self.bottomsizer)
        # add padding sizer
        self.paddingsizer.Add(self.fullsizer,  0, wx.EXPAND | wx.ALL, 30)
        self.SetSizerAndFit(self.paddingsizer)
        self.Fit()

        # help strings for buttons
        self.buttons = {self.pass_box: 'Supply password for Neo4j database.'
                                       'For details on configuring your database, check the Neo4j manual.',
                        self.address_txt: 'Supply address of Neo4j database.'
                                          'For details on configuring your database, check the Neo4j manual.',
                        self.address_box: 'Supply address of Neo4j database.'
                                          'For details on configuring your database, check the Neo4j manual.',
                        self.username_txt: 'Supply username for Neo4j database.'
                                           'For details on configuring your database, check the Neo4j manual.',
                        self.username_box: 'Supply username for Neo4j database.'
                                           'For details on configuring your database, check the Neo4j manual.',
                        self.data_button: 'Launch local Neo4j database.',
                        self.close_button: 'Shut down local Neo4j database.',
                        self.neo_btn: 'Location of your Neo4j folder.',
                        self.data_browser: 'Open Neo4j Browser and explore your data.',
                        self.logbox: 'Logging information for mako.',
                        self.test_button: 'Test connection through a Cypher Query. ',
                        self.clear_button: 'Clear all nodes from the database. ',
                        self.check_button: 'Checks whether database conforms to the data scheme. '
                        }

    def update_help(self, event):
        btn = event.GetEventObject()
        if btn in self.buttons:
            status = self.buttons[btn]
            pub.sendMessage('change_statusbar', msg=status)

    def open_neo(self, event):
        dlg = wx.DirDialog(self, "Select Neo4j directory", style=wx.DD_DEFAULT_STYLE)
        if dlg.ShowModal() == wx.ID_OK:
            neo4j = dlg.GetPath()
            self.neo_txt.SetValue(neo4j)
            self.settings['neo4j'] = neo4j
        dlg.Destroy()
        self.send_config()

    def update_address(self, event):
        text = self.address_box.GetValue()
        self.settings['address'] = text
        self.send_config()

    def update_username(self, event):
        text = self.username_box.GetValue()
        self.settings['username'] = text
        self.send_config()

    def update_pass(self, event):
        text = self.pass_box.GetValue()
        self.settings['password'] = text
        self.send_config()

    def update_pid(self, msg):
        """Listener for Neo4j PID."""
        self.settings['pid'] = msg

    def start_database(self, event):
        self.settings['start'] = True
        eg = Thread(target=start_base, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['start'] = False

    def close_database(self, event):
        self.settings['quit'] = True
        eg = Thread(target=start_base, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['quit'] = False

    def test(self, event):
        eg = Thread(target=query, args=(self.settings, 'MATCH (n) RETURN (count(n)'))
        eg.start()
        eg.join()

    def clear(self, event):
        self.settings['clear'] = True
        eg = Thread(target=start_base, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['check'] = False

    def open_browser(self, event):
        url = "http://localhost:7474/browser/"
        webbrowser.open(url)

    def send_config(self):
        """
        Publisher function for settings
        """
        config = {'address': self.settings['address'],
                  'username': self.settings['username'],
                  'password': self.settings['password'],
                  'neo4j': self.settings['neo4j']}
        pub.sendMessage('config', msg=config)

    def check_database(self):
        self.settings['check'] = True
        eg = Thread(target=start_base, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['check'] = False


if __name__ == "__main__":
    app = wx.App(False)
    app.MainLoop()