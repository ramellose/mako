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
from mako.scripts.io import start_io
from mako.scripts.utils import _read_config
import webbrowser
from biom import load_table
import networkx as nx
from copy import deepcopy
from psutil import Process, pid_exists
from time import sleep
import logging
import sys
import os
import logging.handlers

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class InterfacePanel(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent)
        # subscribe to inputs from tabwindow
        self.frame = parent

        btnsize = (300, -1)
        boxsize = (300, 50)
        # adds columns
        pub.subscribe(self.check_networks, 'network_settings')
        pub.subscribe(self.check_database, 'database_log')
        pub.subscribe(self.set_settings, 'data_settings')
        pub.subscribe(self.set_settings, 'input_settings')
        pub.subscribe(self.set_settings, 'show_settings')
        pub.subscribe(self.update_pid, 'pid')
        pub.subscribe(self.load_settings, 'load_settings')

        self.settings = dict()
        self.networks = None
        self.checks = str()
        self.address = 'bolt://localhost:7687'
        self.username = 'neo4j'
        self.password = 'neo4j'
        self.neo4j = None
        self.process = None
        self.output = 'network'
        self.metadata = None

        self.procbioms = None
        self.networks = None

        # defines columns
        self.leftsizer = wx.BoxSizer(wx.VERTICAL)
        self.rightsizer = wx.BoxSizer(wx.VERTICAL)
        self.topsizer = wx.BoxSizer(wx.HORIZONTAL)

        # Opening neo4j folder
        self.neo_btn = wx.Button(self, label="Select Neo4j folder", size=btnsize)
        self.neo_btn.Bind(wx.EVT_BUTTON, self.open_neo)
        self.neo_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.neo_txt = wx.TextCtrl(self, size=btnsize)

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

        # upload custom data
        self.custom_btn = wx.Button(self, label='Upload additional data to network', size=btnsize)
        self.custom_btn.Bind(wx.EVT_BUTTON, self.open_data)
        self.custom_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.custom_txt = wx.TextCtrl(self, size=(300, 80), style=wx.TE_MULTILINE)
        self.custom_btn.Enable(False)

        # review pane
        # review settings
        self.rev_text = wx.StaticText(self, label='Current operations')
        self.review = wx.TextCtrl(self, value='', size=(boxsize[0], 300), style=wx.TE_READONLY | wx.TE_MULTILINE)

        # export to cytoscape
        self.export_txt = wx.StaticText(self, label='Prefix for graph:')
        self.export_name = wx.TextCtrl(self, value='', size=btnsize)
        self.export_name.Bind(wx.EVT_TEXT, self.update_gml_name)

        # set up database
        self.data_button = wx.Button(self, label='Launch database', size=btnsize)
        self.data_button.Bind(wx.EVT_MOTION, self.update_help)
        self.data_button.Bind(wx.EVT_BUTTON, self.start_database)

        # clear database
        self.clear_button = wx.Button(self, label='Clear database', size=btnsize)
        self.clear_button.Bind(wx.EVT_MOTION, self.update_help)
        self.clear_button.Bind(wx.EVT_BUTTON, self.clear)

        # close database
        self.close_button = wx.Button(self, label='Close database', size=btnsize)
        self.close_button.Bind(wx.EVT_MOTION, self.update_help)
        self.close_button.Bind(wx.EVT_BUTTON, self.close_database)

        # open database in browser
        self.data_browser = wx.Button(self, label='Open database in browser', size=btnsize)
        self.data_browser.Bind(wx.EVT_MOTION, self.update_help)
        self.data_browser.Bind(wx.EVT_BUTTON, self.open_browser)
        self.data_browser.Enable(False)

        # Run button
        self.go = wx.Button(self, label='Export graph', size=btnsize)
        self.go.Bind(wx.EVT_BUTTON, self.writer)
        self.go.Enable(False)

        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.address_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.address_box, flag=wx.ALIGN_LEFT)
        self.leftsizer.AddSpacer(10)
        self.leftsizer.Add(self.username_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.username_box, flag=wx.ALIGN_LEFT)
        self.leftsizer.Add(self.pass_box, flag=wx.ALIGN_LEFT)
        self.leftsizer.AddSpacer(10)
        self.leftsizer.Add(self.neo_btn, flag=wx.ALIGN_LEFT)
        self.leftsizer.Add(self.neo_txt, flag=wx.ALIGN_LEFT)
        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.rev_text, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.review, flag=wx.ALIGN_CENTER)

        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.custom_btn, flag=wx.ALIGN_LEFT)
        self.rightsizer.Add(self.custom_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.export_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.Add(self.export_name, flag=wx.ALIGN_LEFT)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.data_button, flag=wx.ALIGN_LEFT)
        self.rightsizer.Add(self.clear_button, flag=wx.ALIGN_LEFT)
        self.rightsizer.Add(self.close_button, flag=wx.ALIGN_LEFT)
        self.rightsizer.Add(self.data_browser, flag=wx.ALIGN_LEFT)
        self.rightsizer.Add(self.go, flag=wx.ALIGN_LEFT)
        self.rightsizer.AddSpacer(20)

        self.topsizer.AddSpacer(20)
        self.topsizer.Add(self.leftsizer)
        self.topsizer.AddSpacer(40)
        self.topsizer.Add(self.rightsizer)

        self.SetSizerAndFit(self.topsizer)
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
                        self.go: 'Export a GraphML file from the database.',
                        self.custom_btn: 'Add an edge list of metadata nodes to the graph database.'
                        }


    def update_help(self, event):
        btn = event.GetEventObject()
        if btn in self.buttons:
            status = self.buttons[btn]
            pub.sendMessage('change_statusbar', msg=status)

    def open_neo(self, event):
        dlg = wx.DirDialog(self, "Select Neo4j directory", style=wx.DD_DEFAULT_STYLE)
        if dlg.ShowModal() == wx.ID_OK:
            self.neo4j = dlg.GetPath()
            self.neo_txt.SetValue(self.neo4j)
        self.send_settings()
        dlg.Destroy()

    def update_address(self, event):
        text = self.address_box.GetValue()
        self.address = text
        self.send_settings()

    def update_username(self, event):
        text = self.username_box.GetValue()
        self.username = text
        self.send_settings()

    def update_pass(self, event):
        text = self.pass_box.GetValue()
        self.password = text
        self.send_settings()

    def update_pid(self, msg):
        """Listener for Neo4j PID."""
        self.process = msg
        self.settings['pid'] = msg

    def open_data(self, event):
        dlg = wx.FileDialog(self, "Select additional metadata",
                            defaultFile="",
                            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR)
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            if len(paths) > 0:
                self.metadata = paths
        self.custom_txt.SetValue("\n".join(self.metadata))
        self.send_settings()
        try:
            dlg = LoadingBar()
            dlg.ShowModal()
            eg = Thread(target=data_adder, args=(self.settings,))
            eg.start()
            eg.join()
        except Exception:
            logger.error("Failed to upload data to database. ", exc_info=True)
        dlg.Destroy()
        self.settings['add'] = None

    def start_database(self, event):
        checks = str()
        try:
            eg = Thread(target=data_starter, args=(self.settings,))
            eg.start()
            dlg = LoadingBar()
            dlg.ShowModal()
            eg.join()
        except Exception:
            logger.error("Failed to initiate database. ", exc_info=True)
        # removed dlg.LoadingBar() dlg.ShowModal()
        self.custom_btn.Enable(True)
        self.export_name.Enable(True)
        self.export_txt.Enable(True)
        self.data_browser.Enable(True)
        self.go.Enable(True)

    def close_database(self, event):
        eg = Thread(target=data_closer, args=(self.settings,))
        eg.start()
        dlg = LoadingBar()
        dlg.ShowModal()
        eg.join()
        pub.sendMessage('update', msg='Completed database operations!')

    def writer(self, event):
        eg = Thread(target=data_writer, args=(self.settings,))
        eg.start()
        dlg = LoadingBar()
        dlg.ShowModal()
        eg.join()
        pub.sendMessage('update', msg='Completed database operations!')

    def clear(self, event):
        eg = Thread(target=data_clear, args=(self.settings,))
        eg.start()
        dlg = LoadingBar()
        dlg.ShowModal()
        eg.join()
        pub.sendMessage('update', msg='Completed database operations!')

    def open_browser(self, event):
        url = "http://localhost:7474/browser/"
        webbrowser.open(url)

    def update_gml_name(self, event):
        text = self.export_name.GetValue()
        self.output = text
        self.send_settings()

    def send_settings(self):
        """
        Publisher function for settings
        """
        settings = {'output': self.output, 'add': self.metadata,
                    'address': self.address, 'username': self.username,
                    'password': self.password, 'neo4j': self.neo4j}
        pub.sendMessage('data_settings', msg=settings)

    def check_networks(self, msg):
        # define how files should be checked for, it is important that import functions work!
        if 'network' in msg:
            if msg['network'] is not None:
                filelist = deepcopy(msg['network'])
                for file in filelist:
                    network = nx.read_weighted_edgelist(file)
                    self.checks += "Loaded network from " + file + ". \n\n"
                    nodes = len(network.nodes)
                    edges = len(network.edges)
                    self.checks += "This network has " + str(nodes) + \
                                   " nodes and " + str(edges) + " edges. \n\n"
                    weight = nx.get_edge_attributes(network, 'weight')
                    if len(weight) > 0:
                        self.checks += 'This is a weighted network. \n\n'
                    else:
                        self.checks += 'This is an unweighted network. \n\n'
                    allbioms = list()
                    for level in msg['procbioms']:
                        for biom in msg['procbioms'][level]:
                            allbioms.append(msg['procbioms'][level][biom])
                    match = 0
                    taxa = None
                    for biomfile in allbioms:
                        try:
                            biomtab = load_table(biomfile)
                            taxa = biomtab.ids(axis='observation')
                        except TypeError:
                            wx.LogError("Could not access source BIOM file '%s'." % file)
                            logger.error("Could not access source BIOM file. ", exc_info=True)
                        if len(taxa) > 1:
                            nodes = list(network.nodes)
                            if all(elem in taxa for elem in nodes):
                                match += 1
                                self.checks += 'Node identifiers in ' + biomfile + \
                                               ' matched node identifiers in ' + file + '. \n\n'
                    if match == 0:
                        wx.LogError("No BIOM file matched network nodes!")
                        logger.error("No BIOM file matched network nodes!. ", exc_info=True)
            self.review.SetValue(self.checks)

    def check_database(self, msg):
        self.checks += msg
        self.review.SetValue(self.checks)

    def load_settings(self, msg):
        """
        Listener function that changes input values
        to values specified in settings file.
        """
        self.settings = msg
        if msg['neo4j'] is not None:
            self.neo4j = msg['neo4j']
            self.neo_txt.SetValue(self.neo4j)
        else:
            self.neo4j = None
            self.data_browser.Enable(False)
            self.go.Enable(False)
            self.custom_btn.Enable(False)
            self.checks = ''
            self.review.SetValue(self.checks)
            self.custom_txt.SetValue('')
        if msg['password'] is not None:
            self.password = msg['password']
        if msg['output'] is not None:
            self.output = msg['output']
        if msg['address'] is not None:
            self.address = msg['address']
        if msg['username'] is not None:
            self.username = msg['username']
        if msg['output'] is not None:
            self.export_name.SetValue(self.output)
        else:
            self.output = None
            self.export_name.SetValue('')
        if msg['address'] is not None:
            self.address_box.SetValue(self.address)
        else:
            self.address = None
            self.address_box.SetValue('')
        if msg['username'] is not None:
            self.username_box.SetValue(self.username)
        else:
            self.username = None
            self.username_box.SetValue('')
        if msg['password'] is not None:
            self.pass_box.SetValue(self.password)
        else:
            self.password = None
            self.pass_box.SetValue('')
        if 'pid'in msg:
            existing_pid = pid_exists(self.settings['pid'])
            if existing_pid:
                self.checks = ("Existing database process with PID " + str(msg['pid']) + ". \n\n") + self.checks
                self.custom_btn.Enable(True)
                self.export_name.Enable(True)
                self.export_txt.Enable(True)
                self.data_browser.Enable(True)
                self.go.Enable(True)
        self.send_settings()

    def set_settings(self, msg):
        """
        Stores settings file as tab property so it can be read by save_settings.
        """
        try:
            for key in msg:
                self.settings[key] = msg[key]
        except Exception:
            logger.error("Failed to save settings. ", exc_info=True)


class LoadingBar(wx.Dialog):
    def __init__(self):
        """Constructor"""
        wx.Dialog.__init__(self, None, title="Progress")
        self.count = 0
        self.text = wx.StaticText(self, label="Starting...")
        self.progress = wx.Gauge(self, range=4)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.text, 0, wx.EXPAND | wx.ALL, 10)
        sizer.Add(self.progress, 0, wx.EXPAND | wx.ALL, 10)
        self.SetSizer(sizer)
        pub.subscribe(self.get_progress, "update")

    def get_progress(self, msg):
        """This is not working yet! Find a better way to track progress of the thread... """
        testlist = ["Agglomerating edges...", "Associating samples...",
                    "Uploading BIOM files...", "Uploading network files...",
                    "Processing associations...", "Exporting network..."]
        if msg in testlist:
            self.count += 1
        self.progress.SetValue(self.count)
        self.text.SetLabel(msg)
        if msg == 'Completed database operations!':
            self.progress.SetValue(4)
            sleep(3)
            self.Destroy()


def data_starter(inputs):
    """Starts up database and uploads specified files. """
    # first check if process already exists
    inputs['job'] = 'upload'
    start_io(inputs)
    # get PID setting
    settings = _read_config(inputs['fp'])
    new_pid = settings['pid']
    pub.sendMessage('pid', msg=new_pid)
    pub.sendMessage('update', msg='Completed database operations!')


def data_writer(inputs):
    """Exports GraphML file."""
    inputs['job'] = 'write'
    start_io(inputs)
    pub.sendMessage('update', msg='Completed database operations!')


def data_clear(inputs):
    """Clears the database."""
    inputs['job'] = 'clear'
    start_io(inputs)
    # get PID setting
    settings = _read_config(inputs['fp'])
    new_pid = settings['pid']
    pub.sendMessage('pid', msg=new_pid)
    pub.sendMessage('update', msg='Completed database operations!')


def data_adder(inputs):
    """Adds edge list of node properties to Neo4j database."""
    start_io(inputs)
    pub.sendMessage('update', msg='Completed database operations!')


def data_closer(inputs):
    """Adds edge list of node properties to Neo4j database."""
    inputs['job'] = 'quit'
    start_io(inputs)
    sleep(3)
    pub.sendMessage('update', msg='Completed database operations!')


if __name__ == "__main__":
    app = wx.App(False)
    app.MainLoop()