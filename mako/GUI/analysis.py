"""
The network panel allows users to select network inference tools to run and to adjust the settings of these tools.
It also provides an overview of current settings, and can execute massoc with these settings.
"""

__author__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'

from threading import Thread
import wx
from wx.lib.pubsub import pub
from mako.scripts.netstats import start_netstats
from mako.scripts.metastats import start_metastats
from mako.scripts.utils import _read_config, ParentDriver
from time import sleep
from copy import deepcopy
import logging
import sys
import os
import logging.handlers

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class AnalysisPanel(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent)
        # subscribe to inputs from tabwindow
        self.frame = parent

        btnsize = (300, -1)
        boxsize = (300, 50)
        # adds columns
        pub.subscribe(self.enable_tax, 'receive_tax')
        pub.subscribe(self.network_login, 'data_settings')
        pub.subscribe(self.set_settings, 'analysis_settings')
        pub.subscribe(self.set_settings, 'input_settings')
        pub.subscribe(self.load_settings, 'load_settings')
        pub.subscribe(self.data_view, 'view')


        self.settings = dict()
        self.agglom = None
        self.agglom_weight = None
        self.logic = None
        self.networks = None
        self.assoc = None
        self.checks = str()
        self.address = 'bolt://localhost:7687'
        self.username = 'neo4j'
        self.password = 'neo4j'
        self.neo4j = None
        self.output = 'network'

        # defines columns
        self.leftsizer = wx.BoxSizer(wx.VERTICAL)
        self.rightsizer = wx.BoxSizer(wx.VERTICAL)
        self.topsizer = wx.BoxSizer(wx.HORIZONTAL)


        # select taxonomic levels
        self.tax_txt = wx.StaticText(self, label='Agglomerate edges to: ')
        self.tax_choice = wx.ListBox(self, choices=['Species', 'Genus',
                                                    'Family', 'Order', 'Class', 'Phylum'],
                                          size=(boxsize[0], 110), style=wx.LB_MULTIPLE)
        self.tax_choice.Bind(wx.EVT_MOTION, self.update_help)
        self.tax_choice.Bind(wx.EVT_LISTBOX, self.get_levels)
        self.tax_choice.Enable(False)
        self.tax_txt.Enable(False)

        # button for agglomeration
        self.weight_txt = wx.StaticText(self, label='During network agglomeration:')
        self.tax_weight = wx.ListBox(self, choices=['Take weight into account', 'Ignore weight'], size=(boxsize[0], 40))
        self.tax_weight.Bind(wx.EVT_MOTION, self.update_help)
        self.tax_weight.Bind(wx.EVT_LISTBOX, self.weight_agglomeration)
        self.tax_weight.Enable(False)
        self.weight_txt.Enable(False)

        self.overview = wx.Button(self, label='Get database overview', size=(btnsize[0], 40))
        self.overview.Bind(wx.EVT_BUTTON, self.overview_database)

        # button for sample association
        self.assoc_txt = wx.StaticText(self, label='Associate taxa to: ')
        self.assoc_box = wx.ListBox(self, choices=['Run database overview first'], size=(300, 95),
                                    style=wx.LB_MULTIPLE)
        self.assoc_box.Bind(wx.EVT_MOTION, self.update_help)
        self.assoc_box.Bind(wx.EVT_LISTBOX, self.run_association)
        self.assoc_txt.Enable(False)
        self.assoc_box.Enable(False)

        # logic operations
        self.logic_txt = wx.StaticText(self, label='Perform operations:')
        self.logic_choice = wx.ListBox(self, choices=['None', 'Union', 'Intersection', 'Difference'],
                                       size=(boxsize[0], 70))
        self.logic_choice.Bind(wx.EVT_MOTION, self.update_help)
        self.logic_choice.Bind(wx.EVT_LISTBOX, self.get_logic)
        self.logic_choice.Enable(False)
        self.logic_txt.Enable(False)

        # network selection
        self.network_txt = wx.StaticText(self, label="Perform operations on: ")
        self.network_choice = wx.ListBox(self, choices=['Run database overview first'],
                                         size=(boxsize[0], 130), style=wx.LB_MULTIPLE)
        self.network_choice.Bind(wx.EVT_LISTBOX, self.get_network)
        self.network_txt.Enable(False)
        self.network_choice.Enable(False)

        # Run button
        self.go_net = wx.Button(self, label='Run network operations', size=(btnsize[0], 40))
        self.go_net.Bind(wx.EVT_BUTTON, self.run_netstats)
        self.go_net.SetFont(wx.Font(16, wx.DECORATIVE, wx.NORMAL, wx.BOLD))
        self.go_net.SetBackgroundColour(wx.Colour(0, 153, 51))

        # Run button
        self.go_meta = wx.Button(self, label='Run metadata operations', size=(btnsize[0], 40))
        self.go_meta.Bind(wx.EVT_BUTTON, self.run_metastats)
        self.go_meta.SetFont(wx.Font(16, wx.DECORATIVE, wx.NORMAL, wx.BOLD))
        self.go_meta.SetBackgroundColour(wx.Colour(0, 153, 51))

        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.tax_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.tax_choice, flag=wx.ALIGN_LEFT)
        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.weight_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.tax_weight, flag=wx.ALIGN_LEFT)
        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.assoc_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.assoc_box, flag=wx.ALIGN_LEFT)
        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.go_meta, flag=wx.ALIGN_CENTER)
        self.rightsizer.AddSpacer(20)

        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.overview)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.logic_txt)
        self.rightsizer.Add(self.logic_choice)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.network_txt)
        self.rightsizer.Add(self.network_choice)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.go_net, flag=wx.ALIGN_CENTER)
        self.rightsizer.AddSpacer(20)

        self.topsizer.AddSpacer(20)
        self.topsizer.Add(self.leftsizer)
        self.topsizer.AddSpacer(40)
        self.topsizer.Add(self.rightsizer)

        self.SetSizerAndFit(self.topsizer)
        self.Fit()

        # help strings for buttons
        self.buttons = {self.tax_choice: 'Associations that are taxonomically similar at the specified level are '
                                         'combined into agglomerated associations. ',
                        self.tax_weight: 'If selected, only edges with matching weight are agglomerated. ',
                        self.assoc_box: "Taxa are linked to categorical variables through a hypergeometric test"
                                        " and to continous variables through Spearman's rank correlation.",
                        self.logic_choice: 'Find associations that are present in only one or all of your networks.',
                        self.go_net: 'Run the selected network operations and export a GraphML file.',
                        self.go_meta: 'Run the selected metadata operations. Export network in the previous tab.',
                        self.network_choice: "Network(s) to carry out logic operation on.",
                        self.overview: 'Get summary of networks and metadata in network.'
                        }

    def update_help(self, event):
        btn = event.GetEventObject()
        if btn in self.buttons:
            status = self.buttons[btn]
            pub.sendMessage('change_statusbar', msg=status)

    def enable_tax(self, msg):
        self.tax_choice.Enable(True)

    def get_levels(self, event):
        text = list()
        ids = self.tax_choice.GetSelections()
        for i in ids:
            text.append(self.tax_choice.GetString(i))
        text = [x.lower() for x in text]
        self.agglom = text
        self.send_settings()

    def weight_agglomeration(self, event):
        self.agglom_weight = list()
        name = self.tax_weight.GetSelection()
        if name == 0:
            self.agglom_weight = True
        elif name == 1:
            self.agglom_weight = False
        self.send_settings()

    def run_association(self, event):
        self.assoc = list()
        text = list()
        ids = self.assoc_box.GetSelections()
        ids.sort()
        for i in ids:
            text.append(self.assoc_box.GetString(i))
        self.assoc = text
        self.send_settings()

    def get_logic(self, event):
        name = self.logic_choice.GetSelection()
        text = list()
        text.append(self.logic_choice.GetString(name))
        self.logic = list()
        if 'None' in text:
            self.logic = None
        else:
            for val in text:
                self.logic.append(val.lower())
        self.send_settings()

    def get_network(self, event):
        text = list()
        name = self.network_choice.GetSelections()
        for item in name:
            text.append(self.network_choice.GetString(item))
        if 'All' in text:
            self.networks = None
        else:
            self.networks = text
        self.send_settings()

    def send_settings(self):
        """
        Publisher function for settings
        """
        settings = {'variable': self.assoc, 'agglom': self.agglom,
                    'logic': self.logic, 'weight': self.agglom_weight,
                    'networks': self.networks}
        pub.sendMessage('analysis_settings', msg=settings)

    def set_settings(self, msg):
        """
        Stores settings file as tab property so it can be read by save_settings.
        """
        try:
            for key in msg:
                self.settings[key] = msg[key]
        except Exception:\
            logger.error("Failed to save settings. ", exc_info=True)

    def load_settings(self, msg):
        """
        Listener function that changes input values
        to values specified in settings file.
        """
        self.settings = msg
        if msg['agglom'] is not None:
            self.agglom = msg['agglom']
            agglomdict = {'otu': 0, 'species': 1, 'genus': 2,
                          'family': 3, 'order': 4, 'class': 5,
                          'phylum': 6}
            for tax in msg['agglom']:
                self.tax_choice.SetSelection(agglomdict[tax])
        else:
            self.agglom = None
            choice = self.tax_choice.GetSelections()
            for selection in choice:
                self.tax_choice.Deselect(selection)
            self.tax_choice.Enable(False)
            self.tax_txt.Enable(False)
        if msg['weight'] is not None:
            self.agglom_weight = msg['weight']
            if self.agglom_weight:
                self.tax_weight.SetSelection(0)
            if not self.agglom_weight:
                self.tax_weight.SetSelection(1)
        else:
            self.agglom_weight = None
            choice = self.tax_weight.GetSelection()
            self.tax_weight.Deselect(choice)
            self.tax_weight.Enable(False)
            self.weight_txt.Enable(False)
        if 'logic' in msg:
            logicdict = {'Union': 1, 'Intersection': 2, 'Difference': 3}
            self.logic = msg['logic']
            if msg['logic'] is None:
                self.logic_choice.SetSelection(0)
            else:
                for logic in msg['logic']:
                    self.logic_choice.SetSelection(logicdict[logic])
        else:
            self.logic = None
            choice = self.logic_choice.GetSelections()
            for selection in choice:
                self.logic_choice.Deselect(selection)
            self.logic_choice.Enable(False)
            self.logic_txt.Enable(False)
        if msg['networks'] is not None:
            self.network_choice.Set(msg['networks'])
            for i in range(len(msg['networks'])):
                self.network_choice.SetSelection(i)
            self.networks = msg['networks']
        else:
            self.networks = None
            self.network_choice.Set(['Run database overview first'])
            self.network_choice.Enable(False)
            self.network_txt.Enable(False)
        if msg['variable'] is not None:
            vars = msg['variable']
            vars.sort()
            self.assoc_box.Set(vars)
            for i in range(len(msg['variable'])):
                self.assoc_box.SetSelection(i)
            self.assoc = msg['variable']
        else:
            self.assoc = None
            self.assoc_box.Set(['Run database overview first'])
            self.assoc_box.Enable(False)
            self.assoc_txt.Enable(False)
        self.send_settings()

    def network_login(self, msg):
        """Gets login info."""
        self.neo4j = msg['neo4j']
        self.password = msg['password']
        self.address = msg['address']
        self.username = msg['username']
        self.output = msg['output']
        self.send_settings()

    def data_view(self, msg):
        """After getting a view, sets the list boxes."""
        networks = deepcopy(msg[1])
        networks.sort()
        assocs = deepcopy(msg[0])
        assocs.sort()
        self.assoc_box.Set(assocs)
        networks.append('All')
        self.network_choice.Set(networks)

    def overview_database(self, event):
        try:
            eg = Thread(target=data_viewer, args=(self.settings,))
            eg.start()
            dlg = LoadingBar()
            dlg.ShowModal()
            eg.join()
            self.assoc_txt.Enable(True)
            self.assoc_box.Enable(True)
            self.logic_choice.Enable(True)
            self.logic_txt.Enable(True)
            self.network_txt.Enable(True)
            self.network_choice.Enable(True)
            self.tax_choice.Enable(True)
            self.tax_txt.Enable(True)
            self.tax_weight.Enable(True)
            self.weight_txt.Enable(True)
        except Exception:
            logger.error("Failed to get database content. ", exc_info=True)

    def run_netstats(self, event):
        try:
            eg = Thread(target=stats_worker, args=(self.settings,))
            eg.start()
            dlg = LoadingBar()
            dlg.ShowModal()
            eg.join()
        except Exception:
            logger.error("Failed to start database worker. ", exc_info=True)
        # removed LoadingBar()

    def run_metastats(self, event):
        try:
            eg = Thread(target=meta_worker, args=(self.settings,))
            eg.start()
            dlg = LoadingBar()
            dlg.ShowModal()
            eg.join()
        except Exception:
            logger.error("Failed to start database worker. ", exc_info=True)
        # removed LoadingBar()


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
        testlist = ["Completed netstats operations!"]
        if msg in testlist:
            self.count += 1
        self.progress.SetValue(self.count)
        self.text.SetLabel(msg)
        if msg == 'Completed database operations!':
            self.progress.SetValue(4)
            sleep(3)
            self.Destroy()


def stats_worker(inputs):
    """
    Carries out operations on database as specified by user.
    """
    inputs['settings'] = inputs['fp'] + '/settings.json'
    start_netstats(inputs)
    pub.sendMessage('update', msg='Completed database operations!')


def meta_worker(inputs):
    """
    Carries out operations on database as specified by user.
    """
    inputs['settings'] = inputs['fp'] + '/settings.json'
    start_metastats(inputs)
    pub.sendMessage('update', msg='Completed database operations!')


def data_viewer(inputs):
    """
    Gets metadata variables and network names from database.
    """
    old_inputs = _read_config(inputs['fp'])
    old_inputs.update(inputs)
    inputs = old_inputs
    netdriver = ParentDriver(user=inputs['username'],
                             password=inputs['password'],
                             uri=inputs['address'],
                             filepath=inputs['fp'])
    meta = netdriver.query(query="MATCH (n:Property)--(Sample) RETURN n.type")
    meta = set([x[y] for x in meta for y in x])
    networks = netdriver.query(query="MATCH (n:Network) RETURN n.name")
    networks = set([x[y] for x in networks for y in x])
    netdriver.close()
    pub.sendMessage('view', msg=(list(meta), list(networks)))
    pub.sendMessage('update', msg='Completed database operations!')
    return list(meta), list(networks)


if __name__ == "__main__":
    app = wx.App(False)
    app.MainLoop()