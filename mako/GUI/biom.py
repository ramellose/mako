"""
The input panel allows users to supply tab-delimited files and BIOM files to massoc.
It shows key properties of the input files and checks for incompatibility issues.
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
from mako.scripts.neo4biom import start_biom
from mako.scripts.utils import _resource_path, _get_unique, query
import logging
import logging.handlers

logger = logging.getLogger()
wxLogEvent, EVT_WX_LOG_EVENT = wx.lib.newevent.NewEvent()


class BiomPanel(wx.Panel):
    """
    Panel for uploading biom files.
    """
    def __init__(self, parent):
        wx.Panel.__init__(self, parent)
        # subscribe to inputs from tabwindow
        pub.subscribe(self.set_config, 'config')

        self.frame = parent

        self.settings = {'biom_file': [],
                         'fp': _resource_path(''),
                         'count_table': None,
                         'tax_table': None,
                         'sample_meta': None,
                         'taxon_meta': None,
                         'username': 'neo4j',
                         'password': 'neo4j',
                         'address': 'bolt://localhost:7687',
                         'store_config': False,
                         'delete': None}

        btnsize = (300, -1)
        boxsize = (700, 400)

        # defines columns
        self.rightsizer = wx.BoxSizer(wx.VERTICAL)
        self.leftsizer = wx.BoxSizer(wx.VERTICAL)
        self.topsizer = wx.BoxSizer(wx.HORIZONTAL)
        self.bottomsizer = wx.BoxSizer(wx.VERTICAL)
        self.fullsizer = wx.BoxSizer(wx.VERTICAL)
        self.paddingsizer = wx.BoxSizer(wx.HORIZONTAL)

        # set default directory
        self.dir_btn = wx.Button(self, label="Set default directory", size=btnsize)
        self.dir_btn.Bind(wx.EVT_BUTTON, self.open_dir)
        self.dir_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.dir_txt = wx.TextCtrl(self, value="", size=btnsize)

        # Opening BIOM files box
        self.biom_btn = wx.Button(self, label="Open BIOM files", size=btnsize)
        self.biom_btn.Bind(wx.EVT_BUTTON, self.open_biom)
        self.biom_btn.Bind(wx.EVT_MOTION, self.update_help)

        # Open tab files
        self.count_btn = wx.Button(self, label="Open count tables", size=btnsize)
        self.count_btn.Bind(wx.EVT_BUTTON, self.open_count)
        self.count_btn.Bind(wx.EVT_MOTION, self.update_help)

        self.tax_btn = wx.Button(self, label="Open taxonomy tables", size=btnsize)
        self.tax_btn.Bind(wx.EVT_BUTTON, self.open_tax)
        self.tax_btn.Bind(wx.EVT_MOTION, self.update_help)

        self.samplemeta_btn = wx.Button(self, label="Open sample metadata", size=btnsize)
        self.samplemeta_btn.Bind(wx.EVT_BUTTON, self.open_samplemeta)
        self.samplemeta_btn.Bind(wx.EVT_MOTION, self.update_help)

        self.taxmeta_btn = wx.Button(self, label="Open taxon metadata", size=btnsize)
        self.taxmeta_btn.Bind(wx.EVT_BUTTON, self.open_taxmeta)
        self.taxmeta_btn.Bind(wx.EVT_MOTION, self.update_help)

        self.tab_btn = wx.Button(self, label="Import files to Neo4j", size=btnsize)
        self.tab_btn.Bind(wx.EVT_BUTTON, self.import_files)
        self.tab_btn.Bind(wx.EVT_MOTION, self.update_help)

        self.file_txt = wx.TextCtrl(self, size=(300, 80), style=wx.TE_MULTILINE)
        self.file_txt.Bind(wx.EVT_MOTION, self.update_help)
        self.file_txt.AppendText('Uploaded files \n')

        self.get_btn = wx.Button(self, label='Get list of files in database', size=btnsize)
        self.get_btn.Bind(wx.EVT_BUTTON, self.get_files)
        self.get_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.delete_btn = wx.Button(self, label='Delete selected files', size=btnsize)
        self.delete_btn.Bind(wx.EVT_BUTTON, self.delete_files)
        self.delete_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.file_list = wx.ListBox(self, size=(300, 80), style=wx.LB_MULTIPLE)
        self.file_list.Bind(wx.EVT_MOTION, self.update_help)

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
        self.leftsizer.Add(self.dir_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.dir_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.biom_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.count_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.tax_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.samplemeta_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.taxmeta_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.tab_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.AddSpacer(20)

        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.file_txt, 1, wx.ALIGN_LEFT | wx.ALL, 10)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.get_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.file_list, 1, wx.ALIGN_LEFT | wx.ALL, 10)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.delete_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)

        self.bottomsizer.AddSpacer(50)
        self.bottomsizer.Add(self.logtxt, flag=wx.ALIGN_LEFT)
        self.bottomsizer.AddSpacer(10)
        self.bottomsizer.Add(self.logbox, flag=wx.ALIGN_CENTER)

        self.topsizer.Add(self.leftsizer)
        self.topsizer.AddSpacer(40)
        self.topsizer.Add(self.rightsizer)
        self.fullsizer.Add(self.topsizer, flag=wx.ALIGN_CENTER)
        self.fullsizer.Add(self.bottomsizer, flag=wx.ALIGN_CENTER)
        # add padding sizer
        # add padding sizer
        self.paddingsizer.Add(self.fullsizer,  0, wx.EXPAND | wx.ALL, 30)
        self.SetSizerAndFit(self.paddingsizer)
        self.Fit()

        # help strings for buttons
        self.buttons = {self.dir_btn: 'Make sure all sample names and taxa names match '
                        'in the different files!',
                        self.biom_btn: 'Upload one or more BIOM files'
                        ' with associated metadata. '
                        'Leave the other inputs empty if you supply BIOM files.',
                        self.tab_btn: 'Upload experiment data to Neo4j database.',
                        self.count_btn: 'Add filenames for count tables.',
                        self.tax_btn: 'Add filenames for taxonomy tables.',
                        self.samplemeta_btn: 'Add filenames for sample metadata.',
                        self.taxmeta_btn: 'Add filenames for taxon metadata.',
                        self.file_txt: 'Overview of imported files.',
                        self.logbox: 'Logging information for mako.',
                        self.delete_btn: 'Delete selected files from database.',
                        self.file_list: 'Select files for deleting.',
                        self.get_btn: 'Get list of files in database.'
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

    def open_dir(self, event):
        """
        DirDialog for choosing default directory
        :param event: Button event
        :return:
        """
        dlg = wx.DirDialog(self, "Choose default directory", style=wx.DD_DEFAULT_STYLE)
        if dlg.ShowModal() == wx.ID_OK:
            self.settings['fp'] = dlg.GetPath()
        self.dir_txt.SetValue(self.settings['fp'])
        dlg.Destroy()
        pub.sendMessage('fp', msg=self.settings['fp'])

    def open_biom(self, event):
        """
        FileDialog for selecting BIOM files.
        :param event: Button event.
        :return:
        """
        dlg = wx.FileDialog(
            self, message="Select BIOM files",
            defaultDir=self.settings['fp'],
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            self.settings['biom_file'] = paths
            if len(paths) > 0:
                for file in paths:
                    self.file_txt.AppendText(os.path.basename(file) + '\n')
        dlg.Destroy()

    def open_count(self, event):
        """
        FileDialog for selecting count files.
        :param event: Button event.
        :return:
        """
        dlg = wx.FileDialog(
            self, message="Select count tables",
            defaultDir=self.settings['fp'],
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            self.settings['count_table'] = paths
            if len(paths) > 0:
                for file in paths:
                    self.file_txt.AppendText(os.path.basename(file) + '\n')
        dlg.Destroy()

    def open_tax(self, event):
        """
        FileDialog for selecting taxonomy files.
        :param event: Button event.
        :return:
        """
        dlg = wx.FileDialog(
            self, message="Select taxonomy tables",
            defaultDir=self.settings['fp'],
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            self.settings['tax_table'] = paths
            if len(paths) > 0:
                for file in paths:
                    self.file_txt.AppendText(os.path.basename(file) + '\n')
        dlg.Destroy()

    def open_samplemeta(self, event):
        """
        FileDialog for selecting sample metadata files.
        :param event: Button event.
        :return:
        """
        dlg = wx.FileDialog(
            self, message="Select sample metadata",
            defaultDir=self.settings['fp'],
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            self.settings['sample_meta'] = paths
            if len(paths) > 0:
                for file in paths:
                    self.file_txt.AppendText(os.path.basename(file) + '\n')
        dlg.Destroy()

    def open_taxmeta(self, event):
        """
        FileDialog for selecting taxon metadata files.
        :param event: Button event.
        :return:
        """
        dlg = wx.FileDialog(
            self, message="Select taxon metadata",
            defaultDir=self.settings['fp'],
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            self.settings['taxon_meta'] = paths
            if len(paths) > 0:
                for file in paths:
                    self.file_txt.AppendText(os.path.basename(file) + '\n')
        dlg.Destroy()

    def get_files(self, event):
        """
        Queries database to get a list of existing Experiment nodes.
        :param event: Button event.
        :return:
        """
        eg = ThreadPoolExecutor()
        worker = eg.submit(query, self.settings, 'MATCH (n:Experiment) RETURN n')
        del_values = _get_unique(worker.result(), key='n')
        self.file_list.Set(list(del_values))

    def delete_files(self, event):
        """
        Deletes selected Experiment nodes.
        :param event: Button event.
        :return:
        """
        self.logbox.AppendText("Starting operation...\n")
        self.settings['delete'] = [self.file_list.GetString(i)
                                   for i in self.file_list.GetSelections()]
        eg = Thread(target=start_biom, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['delete'] = None

    def import_files(self, event):
        """
        Takes all previously selected files and uploads them to
        the Neo4j database.
        :param event: Button event.
        :return:
        """
        self.logbox.AppendText(str(self.settings))
        self.logbox.AppendText("Starting operation...\n")
        if self.settings['biom_file']:
            for file in self.settings['biom_file']:
                subsettings = self.settings.copy()
                subsettings['biom_file'] = [file]
                subsettings['count_table'] = None
                eg = Thread(target=start_biom, args=(subsettings,))
                eg.start()
                eg.join()
        if self.settings['count_table']:
            for i in range(len(self.settings['count_table'])):
                subsettings = self.settings.copy()
                subsettings['biom_file'] = None
                subsettings['count_table'] = [self.settings['count_table'][i]]
                for key in ['sample_meta', 'taxon_meta', 'tax_table']:
                    try:
                        subsettings[key] = [self.settings[key][i]]
                    except IndexError:
                        pass
                eg = Thread(target=start_biom, args=(subsettings,))
                eg.start()
                eg.join()
        for var in ['biom_file', 'count_table', 'sample_meta', 'taxon_meta', 'tax_table']:
            self.settings[var] = None


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