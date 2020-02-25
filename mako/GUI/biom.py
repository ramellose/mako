"""
The input panel allows users to supply tab-delimited files and BIOM files to massoc.
It shows key properties of the input files and checks for incompatibility issues.
"""

__author__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

import wx
import os
from wx.lib.pubsub import pub
import biom
from biom.parse import MetadataMap
from biom.exception import BiomParseException
from mako.scripts.neo4biom import start_biom
from mako.scripts.utils import _read_config, ParentDriver
import sys
import logging
import os
import logging.handlers

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class BiomPanel(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent)
        # subscribe to inputs from tabwindow
        pub.subscribe(self.set_settings, 'show_settings')

        self.frame = parent

        self.settings = dict()

        self.currentDirectory = None
        self.currentDirectory = os.getcwd()
        self.count_file = None
        self.tax_file = None
        self.sample_file = None
        self.biom_file = None
        self.network_path = None
        self.checks = str()
        self.split = None

        btnsize = (300, -1)
        btnmargin = 10
        # adds columns
        self.topsizer = wx.BoxSizer(wx.HORIZONTAL)

        # defines column of file loading properties
        self.leftsizer = wx.BoxSizer(wx.VERTICAL)
        self.topleftsizer = wx.BoxSizer(wx.VERTICAL)
        self.bottomleftsizer = wx.BoxSizer(wx.VERTICAL)

        # include a column to report errors or check file formats
        self.rightsizer = wx.BoxSizer(wx.VERTICAL)

        # set default directory
        self.dir_btn = wx.Button(self, label="Set default directory", size=btnsize)
        self.dir_btn.Bind(wx.EVT_BUTTON, self.open_dir)
        self.dir_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.dir_txt = wx.TextCtrl(self, value="", size=btnsize)

        # Opening BIOM files box
        self.biom_btn = wx.Button(self, label="Open BIOM files", size=btnsize)
        self.biom_btn.Bind(wx.EVT_BUTTON, self.open_biom)
        self.biom_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.biom_txt = wx.TextCtrl(self, size=(300, 80), style=wx.TE_MULTILINE)

        # Opening network files
        self.net_choice = wx.ListBox(self, choices=['Construct network', 'Open network file'], size=(300, 50))
        self.net_choice.Bind(wx.EVT_LISTBOX, self.toggle_networks)
        self.net_choice.Bind(wx.EVT_MOTION, self.update_help)
        self.net_btn = wx.Button(self, label="Open network file", size=btnsize)
        self.net_btn.Bind(wx.EVT_BUTTON, self.open_network)
        self.net_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.net_btn.Disable()

        # Opening tab-delimited files box
        self.tab_btn = wx.Button(self, label="Show dialog for tab-delimited files", size=btnsize)
        self.tab_btn.Bind(wx.EVT_BUTTON, self.show_dialog)
        self.tab_btn.Bind(wx.EVT_MOTION, self.update_help)

        self.count_btn = wx.Button(self, label="Open count tables", size=btnsize)
        self.count_btn.Bind(wx.EVT_BUTTON, self.open_count)
        self.count_txt = wx.TextCtrl(self, size=(300, 40), style=wx.TE_MULTILINE)
        self.count_txt.Hide()
        self.count_btn.Hide()

        self.tax_btn = wx.Button(self, label="Open taxonomy tables", size=btnsize)
        self.tax_txt = wx.TextCtrl(self, size=(300, 40), style=wx.TE_MULTILINE)
        self.tax_btn.Bind(wx.EVT_BUTTON, self.open_tax)
        self.tax_txt.Hide()
        self.tax_btn.Hide()

        self.meta_btn = wx.Button(self, label="Open metadata", size=btnsize)
        self.meta_txt = wx.TextCtrl(self, size=(300, 40), style=wx.TE_MULTILINE)
        self.meta_btn.Bind(wx.EVT_BUTTON, self.open_meta)
        self.meta_txt.Hide()
        self.meta_btn.Hide()

        # File summary
        self.summ_txt = wx.StaticText(self, label="Checking selected files...")
        self.summ_box = wx.TextCtrl(self, size=(900, 80), style=wx.TE_MULTILINE | wx.TE_READONLY)

        # Save settings button
        self.save_btn = wx.Button(self, label="Save settings", size=btnsize)
        self.save_btn.SetFont(wx.Font(16, wx.DECORATIVE, wx.NORMAL, wx.BOLD))
        self.save_btn.Bind(wx.EVT_BUTTON, self.save_settings)
        self.save_btn.Bind(wx.EVT_MOTION, self.update_help)

        # Load settings button
        self.load_btn = wx.Button(self, label="Load settings", size=btnsize)
        self.load_btn.SetFont(wx.Font(16, wx.DECORATIVE, wx.NORMAL, wx.BOLD))
        self.load_btn.Bind(wx.EVT_BUTTON, self.load_settings)
        self.load_btn.Bind(wx.EVT_MOTION, self.update_help)

        # Clear settings button
        self.clear_btn = wx.Button(self, label="Clear settings", size=btnsize)
        self.clear_btn.SetFont(wx.Font(16, wx.DECORATIVE, wx.NORMAL, wx.BOLD))
        self.clear_btn.Bind(wx.EVT_BUTTON, self.clear_settings)
        self.clear_btn.Bind(wx.EVT_MOTION, self.update_help)

        self.topleftsizer.Add(self.dir_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.topleftsizer.Add(self.dir_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.topleftsizer.AddSpacer(10)
        self.topleftsizer.Add(self.biom_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.topleftsizer.Add(self.biom_txt, 1, wx.EXPAND | wx.ALIGN_LEFT | wx.ALL, btnmargin)
        self.topleftsizer.AddSpacer(10)
        self.topleftsizer.Add(self.net_choice, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.topleftsizer.Add(self.net_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.topleftsizer.AddSpacer(10)
        self.topleftsizer.Add(self.tab_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.bottomleftsizer.Add(self.count_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.bottomleftsizer.Add(self.count_txt, 1, wx.EXPAND | wx.ALIGN_LEFT | wx.ALL, btnmargin)
        self.bottomleftsizer.Add(self.tax_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.bottomleftsizer.Add(self.tax_txt, 1, wx.EXPAND | wx.ALIGN_LEFT | wx.ALL, btnmargin)
        self.bottomleftsizer.Add(self.meta_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.bottomleftsizer.Add(self.meta_txt, 1, wx.EXPAND | wx.ALIGN_LEFT | wx.ALL, btnmargin)

        self.rightsizer.Add(self.summ_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.Add(self.summ_box, 1, wx.ALIGN_LEFT | wx.EXPAND | wx.ALL, btnmargin)
        self.rightsizer.Add(self.save_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.Add(self.load_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.Add(self.clear_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.topleftsizer, wx.ALL, 20)
        self.leftsizer.AddSpacer(10)
        self.leftsizer.Add(self.bottomleftsizer, wx.ALL, 20)
        self.bottomleftsizer.ShowItems(show=False)
        self.topsizer.Add(self.leftsizer, 0, wx.ALL, 20)
        self.topsizer.Add(self.rightsizer, 0, wx.ALL | wx.EXPAND, 20)
        self.SetSizerAndFit(self.topsizer)

        # help strings for buttons
        self.buttons = {self.dir_btn: 'Make sure all sample names and taxa names match '
                        'in the different files!',
                        self.biom_btn: 'Upload one or more BIOM files'
                        ' with associated metadata. '
                        'Leave the other inputs empty if you supply BIOM files.',
                        self.tab_btn: 'Upload tab-delimited count tables, taxonomy tables and '
                        'metadata. '
                        'Make sure to include the taxonomy table as well.',
                        self.clear_btn: 'Clear all settings in massoc.',
                        self.save_btn: 'Save all settings to a text file.',
                        self.load_btn: 'Load settings from a text file.',
                        self.net_choice: 'Construct a network in massoc or open one from an edge list.',
                        self.net_btn: 'Load a network from an edge list. Supply the matching BIOM / tab-delimited files as well!'}

    def open_dir(self, event):
        dlg = wx.DirDialog(self, "Choose default directory", style=wx.DD_DEFAULT_STYLE)
        if dlg.ShowModal() == wx.ID_OK:
            self.currentDirectory = dlg.GetPath()
        self.dir_txt.SetValue(self.currentDirectory)
        self.send_settings()
        dlg.Destroy()

    def open_biom(self, event):
        """
        Create file dialog and show it.
        """
        dlg = wx.FileDialog(
            self, message="Select BIOM files",
            defaultDir=self.currentDirectory,
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            self.biom_file = paths
            if len(paths) > 0:
                self.biom_txt.SetValue("\n".join(self.biom_file))
        self.send_settings()
        self.checkfiles('biom')
        dlg.Destroy()

    def open_network(self, event):
        """
        Create file dialog and show it.
        """
        dlg = wx.FileDialog(
            self, message="Select network edge lists",
            defaultDir=self.currentDirectory,
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            if len(paths) > 0:
                self.network_path = paths
        # if networks are imported here, biom files should already be set
        self.send_settings()
        self.checkfiles('network')
        dlg.Destroy()

    def update_help(self, event):
        """
        Publishes help message for statusbar at the bottom of the notebook.
        """
        btn = event.GetEventObject()
        if btn in self.buttons:
            status = self.buttons[btn]
            pub.sendMessage('change_statusbar', msg=status)

    def send_settings(self):
        """
        Publisher function for settings
        """
        path = self.currentDirectory.replace("\\", "/")
        settings = {'fp': path, 'otu_table': self.count_file, 'tax_table': self.tax_file,
                    'sample_data': self.sample_file, 'biom_file': self.biom_file,
                    'network': self.network_path, 'otu_meta': None}
        pub.sendMessage('input_settings', msg=settings)

    def clear_settings(self, event):
        """
        Publisher function that clears all current massoc settings
        """
        # empty settings dictionary
        self.settings = {"biom_file": None,
                         "otu_table": None,
                         "tax_table": None,
                         "sample_data": None,
                         "otu_meta": None,
                         "cluster": None,
                         "split": None,
                         "prev": 20,
                         "fp": None,
                         "levels": None,
                         "tools": None,
                         "spiec": None,
                         "conet": None,
                         "conet_bash": None,
                         "spar": None,
                         "spar_pval": None,
                         "spar_boot": None,
                         "nclust": None,
                         "name": None,
                         "cores": None,
                         "rar": None,
                         "min": None,
                         "network": None,
                         "assoc": None,
                         "agglom": None,
                         "logic": None,
                         "agglom_weight": None,
                         "export": None,
                         "neo4j": None,
                         "procbioms": None,
                         "address": "bolt://localhost:7687",
                         "username": "neo4j",
                         "password": "neo4j",
                         "variable": None,
                         "weight": None,
                         "networks": None,
                         "output": None,
                         "add": None}
        self.biom_file = None
        self.biom_txt.SetValue('')
        self.count_file = None
        self.count_txt.SetValue('')
        self.count_txt.SetValue('')
        self.tax_file = None
        self.tax_txt.SetValue('')
        self.sample_file = None
        self.meta_txt.SetValue('')
        choice = self.net_choice.GetSelection()
        self.net_choice.Deselect(choice)
        self.currentDirectory = None
        self.dir_txt.SetValue('')
        self.checks = ''
        self.summ_box.SetValue(self.checks)
        pub.sendMessage('load_settings', msg=self.settings)

    def load_settings(self, event):
        """
        Publisher function that loads a dictionary of settings
        and updates the GUI to show these.
        Source: wxpython FileDialog docs
        """
        self.settings = dict()
        with wx.FileDialog(self, "Open settings file", wildcard="json files (*.json)|*.json",
                           style=wx.FD_OPEN | wx.FD_FILE_MUST_EXIST) as fileDialog:

            if fileDialog.ShowModal() == wx.ID_CANCEL:
                return  # the user changed their mind

            # Proceed loading the file chosen by the user
            pathname = fileDialog.GetPath()
            try:
                self.settings = _read_config({'fp': pathname})
            except IOError:
                wx.LogError("Cannot open file '%s'." % pathname)
                logger.error("Cannot open file. ", exc_info=True)
        self.currentDirectory = self.settings['fp']
        self.dir_txt.SetValue(self.settings['fp'])
        self.biom_file = self.settings['biom_file']
        self.biom_txt.SetValue('')
        self.network_path = self.settings['network']
        if self.settings['biom_file'] is not None:
            self.checkfiles('biom')
            self.biom_txt.SetValue('\n'.join(self.settings['biom_file']))
        if self.settings['otu_table'] is not None:
            self.count_file = self.settings['otu_table']
            self.checkfiles('count')
            self.count_txt.SetValue('\n'.join(self.settings['otu_table']))
        if self.settings['tax_table'] is not None:
            self.tax_file = self.settings['tax_table']
            self.checkfiles('tax')
            self.tax_txt.SetValue('\n'.join(self.settings['tax_table']))
        if self.settings['split'] is not None:
            self.split = self.settings['split']
        if self.settings['sample_data'] is not None:
            self.sample_file = self.settings['sample_data']
            self.checkfiles('meta')
            self.meta_txt.SetValue('\n'.join(self.settings['sample_data']))
        if self.settings['network'] is not None:
            self.net_choice.SetSelection(1)
        else:
            self.net_choice.SetSelection(0)
        pub.sendMessage('load_settings', msg=self.settings)
        self.send_settings()

    def set_settings(self, msg):
        """
        Stores settings file as tab property so it can be read by save_settings.
        """
        self.settings = msg

    def save_settings(self, event):
        """
        Takes self.settings file to write to disk.
        Source: wxpython FileDialog docs
        """
        with wx.FileDialog(self, "Save settings file", wildcard="json files (*.json)|*.json",
                           style=wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT) as fileDialog:

            if fileDialog.ShowModal() == wx.ID_CANCEL:
                return  # the user changed their mind

            # save the current contents in the file
            pathname = fileDialog.GetPath()
            try:
                pass
            except IOError:
                wx.LogError("Cannot save current data in file '%s'." % pathname)
                logger.error("Cannot save current data in file. ", exc_info=True)

    def toggle_networks(self, event):
        """Disables network dir button or tab files, depending on selection."""
        choice_id = self.net_choice.GetSelection()
        choice = self.net_choice.GetString(choice_id)
        if choice == 'Construct network':
            pub.sendMessage('toggle_network', msg='Yes')
            self.net_btn.Enable(False)
        if choice == 'Open network file':
            pub.sendMessage('toggle_network', msg='No')
            self.net_btn.Enable(True)

    def show_dialog(self, event):
        """
        Shows buttons with filenames
        for tab-delimited files.
        """
        if self.count_btn.IsShown():
            self.bottomleftsizer.ShowItems(show=False)
        else:
            self.bottomleftsizer.ShowItems(show=True)
        self.Layout()

    def open_count(self, event):
        """
        Create file dialog and show it.
        """
        dlg = wx.FileDialog(
            self, message="Select count tables",
            defaultDir=self.currentDirectory,
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            self.count_file = paths
            if len(paths) > 0:
                self.count_txt.SetValue("\n".join(self.count_file))
        self.send_settings()
        self.checkfiles('count')
        dlg.Destroy()

    def open_tax(self, event):
        """
        Create file dialog and show it.
        """
        dlg = wx.FileDialog(
            self, message="Select taxonomy tables",
            defaultDir=self.currentDirectory,
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            self.tax_file = paths
            if len(paths) > 0:
                self.tax_txt.SetValue("\n.".join(self.tax_file))
        self.send_settings()
        self.checkfiles('tax')
        dlg.Destroy()

    def open_meta(self, event):
        """
        Create file dialog and show it.
        """
        dlg = wx.FileDialog(
            self, message="Select metadata",
            defaultDir=self.currentDirectory,
            defaultFile="",
            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_CHANGE_DIR
        )
        if dlg.ShowModal() == wx.ID_OK:
            paths = dlg.GetPaths()
            paths = [x.replace('\\', '/') for x in paths]
            self.sample_file = paths
            if len(paths) > 0:
                self.meta_txt.SetValue("\n".join(self.sample_file))
        self.send_settings()
        self.checkfiles('meta')
        dlg.Destroy()

    def checkfiles(self, filetype):
        # define how files should be checked for, it is important that import functions work!
        if filetype is 'count' and self.count_file:
            for x in self.count_file:
                try:
                    biomtab = biom.load_table(x)
                    self.checks += "Loaded count table from " + x + ". \n\n"
                    biomdims = biomtab.shape
                    self.checks += "This table contains " + str(biomdims[0]) + " taxa and " + str(biomdims[1]) +\
                                   " samples. \n\n"
                except(TypeError, BiomParseException):
                    wx.LogError("Cannot parse biom file '%s'." % x)
                    logger.error("Cannot parse biom file. \n", exc_info=True)
        if filetype is 'biom' and self.biom_file:
            for x in self.biom_file:
                try:
                    biomtab = biom.load_table(x)
                    self.checks += "Loaded BIOM file from " + x + ". \n\n"
                    biomdims = biomtab.shape
                    self.checks += "This BIOM file contains " + str(biomdims[0]) + " taxa and " + str(biomdims[1]) +\
                                   " samples. \n\n"
                    names = biomtab.metadata(biomtab.ids(axis='sample')[0], axis="sample")
                    if names is not None:
                        varlist = list()
                        for key, value in names.items():
                            varlist.append(key)
                        names = '\n'.join(varlist)
                        self.checks += "The sample data contains the following variables: \n" + names + "\n"
                        pub.sendMessage('receive_metadata', msg=varlist)
                    names = biomtab.metadata(biomtab.ids(axis='observation')[0], axis='observation')
                    if names is not None:
                        self.checks += "This BIOM file contains taxonomy data. \n\n"
                        pub.sendMessage('receive_tax', msg='added_tax')
                except(TypeError, BiomParseException):
                    wx.LogError(str(x) + ' does not appear to be a BIOM-compatible table!')
                    logger.error(str(x) + ' does not appear to be a BIOM-compatible table!. ', exc_info=True)
        if filetype is 'tax' and self.tax_file:
            for x, z in zip(self.count_file, self.tax_file):
                try:
                    biomtab = biom.load_table(x)
                    obs_f = open(z, 'r')
                    obs_data = MetadataMap.from_file(obs_f)
                    obs_f.close()
                    # for taxonomy collapsing,
                    # metadata variable needs to be a complete list
                    # not separate entries for each tax level
                    for i in list(obs_data):
                        tax = list()
                        for j in list(obs_data[i]):
                            tax.append(obs_data[i][j])
                            obs_data[i].pop(j, None)
                        obs_data[i]['taxonomy'] = tax
                    biomtab.add_metadata(obs_data, axis='observation')
                    self.checks += "Loaded taxonomy table from " + z + ". \n\n"
                    pub.sendMessage('receive_tax', msg='added_tax')
                except(TypeError, ValueError, BiomParseException):
                    wx.LogError(str(x) + ' and ' + str(z) + ' cannot be combined into a BIOM file!')
                    logger.error(str(x) + ' and ' + str(z) + ' cannot be combined into a BIOM file! ', exc_info=True)
        if filetype is 'meta' and self.sample_file:
            meta_dict = dict()
            for x, z in zip(self.count_file, self.sample_file):
                try:
                    biomtab = biom.load_table(x)
                    sample_f = open(z, 'r')
                    sample_data = MetadataMap.from_file(sample_f)
                    sample_f.close()
                    biomtab.add_metadata(sample_data, axis='sample')
                    self.checks += "Loaded sample data from " + z + ". \n\n"
                    data = biomtab.metadata_to_dataframe(axis='sample')
                    allnames = data.columns
                    num_cols = data._get_numeric_data()
                    names = list(set(allnames) - set(num_cols))
                    varlist = list()
                    for name in names:
                        varlist.append(name)
                    names = '\n'.join(allnames)
                    self.checks += "The sample data contains the following variables: \n" + names + "\n"
                    meta_dict[x] = varlist
                except(TypeError, KeyError, ValueError, BiomParseException):
                    wx.LogError(str(x) + ' and ' + str(z) + ' cannot be combined into a BIOM file!')
                    logger.error(str(x) + ' and ' + str(z) + ' cannot be combined into a BIOM file! ', exc_info=True)
            pub.sendMessage('input_metadata', msg=(meta_dict, self.split))
        if filetype is 'network':
            try:
                nets_object = start_biom(self.settings)
                nets_object.add_networks()
                self.checks += "Network objects could be added successfully."
            except (TypeError, ValueError):
                wx.LogError('Unable to load network edge list!')
                logger.error('Unable to load network edge list! ')
        self.summ_box.SetValue(self.checks)