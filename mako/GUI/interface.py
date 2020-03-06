"""
The interface panel allows users to upload and interact with networks.
"""

__author__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import wx
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from wx.lib.pubsub import pub
import os
from mako.scripts.io import start_io
from mako.scripts.utils import _resource_path, query, _get_unique
import logging
import logging.handlers

logger = logging.getLogger()
wxLogEvent, EVT_WX_LOG_EVENT = wx.lib.newevent.NewEvent()


class InterfacePanel(wx.Panel):
    """
    Panel for uploading and interacting with networks.
    """
    def __init__(self, parent):
        wx.Panel.__init__(self, parent)
        # subscribe to inputs from tabwindow
        self.frame = parent

        pub.subscribe(self.set_config, 'config')
        pub.subscribe(self.set_fp, 'fp')


        self.frame = parent

        self.settings = {'networks': [],
                         'fp': _resource_path(''),
                         'username': 'neo4j',
                         'password': 'neo4j',
                         'address': 'bolt://localhost:7687',
                         'store_config': False,
                         'delete': None,
                         'cyto': None,
                         'fasta': None,
                         'meta': None,
                         'write': None}

        btnsize = (300, -1)
        boxsize = (700, 400)

        # defines columns
        self.rightsizer = wx.BoxSizer(wx.VERTICAL)
        self.leftsizer = wx.BoxSizer(wx.VERTICAL)
        self.topsizer = wx.BoxSizer(wx.HORIZONTAL)
        self.bottomsizer = wx.BoxSizer(wx.VERTICAL)
        self.fullsizer = wx.BoxSizer(wx.VERTICAL)
        self.paddingsizer = wx.BoxSizer(wx.HORIZONTAL)

        # upload networks
        self.network_btn = wx.Button(self, label='Open networks', size=btnsize)
        self.network_btn.Bind(wx.EVT_BUTTON, self.open_networks)
        self.network_btn.Bind(wx.EVT_MOTION, self.update_help)

        # upload fasta
        self.fasta_btn = wx.Button(self, label='Open FASTA files', size=btnsize)
        self.fasta_btn.Bind(wx.EVT_BUTTON, self.open_fasta)
        self.fasta_btn.Bind(wx.EVT_MOTION, self.update_help)

        # upload meta
        self.meta_btn = wx.Button(self, label='Open metadata files', size=btnsize)
        self.meta_btn.Bind(wx.EVT_BUTTON, self.open_meta)
        self.meta_btn.Bind(wx.EVT_MOTION, self.update_help)

        # file txt
        self.file_txt = wx.TextCtrl(self, size=(300, 80), style=wx.TE_MULTILINE)
        self.file_txt.Bind(wx.EVT_MOTION, self.update_help)
        self.file_txt.AppendText('Uploaded files \n')

        # network buttons
        self.get_btn = wx.Button(self, label='Get list of networks in database', size=btnsize)
        self.get_btn.Bind(wx.EVT_BUTTON, self.get_networks)
        self.get_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.file_list = wx.ListBox(self, size=(300, 80), style=wx.LB_MULTIPLE)
        self.file_list.Bind(wx.EVT_MOTION, self.update_help)
        self.delete_btn = wx.Button(self, label='Delete selected networks', size=btnsize)
        self.delete_btn.Bind(wx.EVT_BUTTON, self.delete_networks)
        self.delete_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.write_btn = wx.Button(self, label='Write selected networks', size=btnsize)
        self.write_btn.Bind(wx.EVT_BUTTON, self.write_networks)
        self.write_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.export_btn = wx.Button(self, label='Export selection to Cytoscape', size=btnsize)
        self.export_btn.Bind(wx.EVT_BUTTON, self.export_cyto)
        self.export_btn.Bind(wx.EVT_MOTION, self.update_help)

        # Logger
        self.logtxt = wx.StaticText(self, label='Logging panel')
        self.logbox = wx.TextCtrl(self, value='', size=boxsize, style=wx.TE_MULTILINE)
        self.logbox.Bind(wx.EVT_MOTION, self.update_help)
        self.logbox.Bind(EVT_WX_LOG_EVENT, self.log_event)

        handler = LogHandler(ctrl=self.logbox)
        logger.addHandler(handler)
        self.logbox.SetForegroundColour(wx.WHITE)
        self.logbox.SetBackgroundColour(wx.BLACK)

        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.network_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.fasta_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.meta_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.file_txt, 1, wx.ALIGN_LEFT | wx.ALL, 10)
        self.leftsizer.AddSpacer(20)

        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.get_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.file_list, 1, wx.ALIGN_LEFT | wx.ALL, 10)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.delete_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.Add(self.write_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.Add(self.export_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)

        self.bottomsizer.Add(self.logtxt, flag=wx.ALIGN_LEFT)
        self.bottomsizer.AddSpacer(10)
        self.bottomsizer.Add(self.logbox, flag=wx.ALIGN_CENTER)

        self.topsizer.Add(self.leftsizer)
        self.topsizer.AddSpacer(40)
        self.topsizer.Add(self.rightsizer)
        self.fullsizer.Add(self.topsizer, flag=wx.ALIGN_CENTER)
        self.fullsizer.Add(self.bottomsizer, flag=wx.ALIGN_CENTER)
        # add padding sizer
        self.paddingsizer.Add(self.fullsizer,  0, wx.EXPAND | wx.ALL, 30)
        self.SetSizerAndFit(self.paddingsizer)
        self.Fit()

        # help strings for buttons
        self.buttons = {self.network_btn: 'Upload network files (graphml, gml, txt and cyjson).',
                        self.fasta_btn: 'Upload FASTA files.',
                        self.meta_btn: 'Metadata text files to upload (node name in left column, property in right).',
                        self.file_txt: 'List of imported files.',
                        self.delete_btn: 'Delete selected networks from database.',
                        self.write_btn: 'Write selected networks to graphml files.',
                        self.export_btn: 'Location of your Neo4j folder.',
                        self.file_list: 'Select networks for deleting, writing or exporting.',
                        self.logbox: 'Logging information for mako.',
                        self.get_btn: 'Get list of networks in database.'
                        }

    def update_help(self, event):
        """
        Publishes help message for statusbar at the bottom of the notebook.

        :param event: UI event
        :return:
        """
        btn = event.GetEventObject()
        if btn in self.buttons:
            status = self.buttons[btn]
            pub.sendMessage('change_statusbar', msg=status)

    def set_config(self, msg):
        """
        Sets parameters for accessing Neo4j database
        :param msg: pubsub message
        :return:
        """
        for key in msg:
            self.settings[key] = msg[key]

    def log_event(self, event):
        """
        Listerer for logging handler that generates a wxPython event
        :param event: custom event
        :return:
        """
        msg = event.message.strip("\r") + "\n"
        self.logbox.AppendText(msg)
        event.Skip()

    def set_fp(self, msg):
        """
        Listener for fp event from BIOM tab.
        :param msg: pubsub message
        :return:
        """
        self.settings['fp'] = msg

    def open_networks(self, event):
        """
        FileDialog for selecting and uploading networks.
        :param event: Button event.
        :return:
        """
        dlg = wx.FileDialog(
            self, message="Select network files",
            defaultDir=self.settings['fp'],
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            self.settings['networks'] = paths
            if len(paths) > 0:
                for file in paths:
                    self.file_txt.AppendText(os.path.basename(file) + '\n')
        dlg.Destroy()
        self.logbox.AppendText("Starting operation...\n")
        eg = Thread(target=start_io, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['networks'] = None

    def open_fasta(self, event):
        """
        FileDialog for selecting and uploading FASTA files.
        :param event: Button event.
        :return:
        """
        dlg = wx.FileDialog(
            self, message="Select FASTA files",
            defaultDir=self.settings['fp'],
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            self.settings['fasta'] = paths
            if len(paths) > 0:
                for file in paths:
                    self.file_txt.AppendText(os.path.basename(file) + '\n')
        dlg.Destroy()
        self.logbox.AppendText("Starting operation...\n")
        eg = Thread(target=start_io, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['fasta'] = None

    def open_meta(self, event):
        """
        FileDialog for selecting and uploading metadata files.
        :param event: Button event.
        :return:
        """
        dlg = wx.FileDialog(
            self, message="Select metadata files",
            defaultDir=self.settings['fp'],
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            self.settings['meta'] = paths
            if len(paths) > 0:
                for file in paths:
                    self.file_txt.AppendText(os.path.basename(file) + '\n')
        dlg.Destroy()
        self.logbox.AppendText("Starting operation...\n")
        eg = Thread(target=start_io, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['meta'] = None

    def get_networks(self, event):
        """
        Get list of Network nodes from database.
        :param event: Button event
        :return:
        """
        eg = ThreadPoolExecutor()
        worker = eg.submit(query, self.settings, 'MATCH (n) WHERE n:Network OR n:Set RETURN n')
        del_values = _get_unique(worker.result(), key='n')
        self.file_list.Set(list(del_values))

    def delete_networks(self, event):
        """
        Deletes selected Network nodes.
        :param event: Button event.
        :return:
        """
        self.logbox.AppendText("Starting operation...\n")
        self.settings['networks'] = [self.file_list.GetString(i)
                                   for i in self.file_list.GetSelections()]
        self.settings['delete'] = True
        eg = Thread(target=start_io, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['delete'] = None
        self.settings['networks'] = None

    def write_networks(self, event):
        """
        Writes selected networks to graphml files.
        :param event: Button event.
        :return:
        """
        self.logbox.AppendText("Starting operation...\n")
        self.settings['networks'] = [self.file_list.GetString(i)
                                   for i in self.file_list.GetSelections()]
        self.settings['write'] = True
        eg = Thread(target=start_io, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['write'] = None
        self.settings['networks'] = None

    def export_cyto(self, event):
        """
        Exports selected networks to Cytoscape.
        :param event: Button event.
        :return:
        """
        self.logbox.AppendText("Starting operation...\n")
        self.settings['networks'] = [self.file_list.GetString(i)
                                   for i in self.file_list.GetSelections()]
        self.settings['cyto'] = True
        print(self.settings)
        eg = Thread(target=start_io, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['cyto'] = None
        self.settings['networks'] = None


class LogHandler(logging.Handler):
    """
    Object defining custom handler for logger.
    """
    def __init__(self, ctrl):
        logging.Handler.__init__(self)
        self.ctrl = ctrl
        self.level = logging.INFO

    def flush(self):
        """
        Overwrites default flush
        :return:
        """
        pass

    def emit(self, record):
        """
        Handler triggers custom wx Event and sends a message.
        :param record: Logger record
        :return:
        """
        try:
            s = self.format(record) + '\n'
            evt = wxLogEvent(message=s, levelname=record.levelname)
            wx.PostEvent(self.ctrl, evt)
        except (KeyboardInterrupt, SystemExit):
            raise

