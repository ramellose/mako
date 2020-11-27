"""
The analysis panel allows users to merge networks by taxonomy,
return intersections and carry out some statistics.
"""

__author__ = 'Lisa Rottjers'
__email__ = 'lisa.rottjers@kuleuven.be'
__status__ = 'Development'
__license__ = 'Apache 2.0'


import wx
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from pubsub import pub
import os
from mako.scripts.netstats import start_netstats
from mako.scripts.metastats import start_metastats
from mako.scripts.utils import _resource_path, query, _get_unique
import logging
import logging.handlers

logger = logging.getLogger()
wxLogEvent, EVT_WX_LOG_EVENT = wx.lib.newevent.NewEvent()


class AnalysisPanel(wx.Panel):
    """
    Panel for carrying out analyses on the database.
    """
    def __init__(self, parent):
        wx.Panel.__init__(self, parent)
        # subscribe to inputs from tabwindow
        pub.subscribe(self.set_config, 'config')

        self.frame = parent
        self.settings = {'networks': None,
                         'fp': _resource_path(''),
                         'username': 'neo4j',
                         'password': 'neo4j',
                         'address': 'bolt://localhost:7687',
                         'store_config': False,
                         'variable': None,
                         'weight': True,
                         'agglom': None,
                         'set': None,
                         'fraction': None}

        btnsize = (300, -1)
        boxsize = (700, 400)

        # defines columns
        self.rightsizer = wx.BoxSizer(wx.VERTICAL)
        self.leftsizer = wx.BoxSizer(wx.VERTICAL)
        self.topsizer = wx.BoxSizer(wx.HORIZONTAL)
        self.bottomsizer = wx.BoxSizer(wx.VERTICAL)
        self.fullsizer = wx.BoxSizer(wx.VERTICAL)
        self.paddingsizer = wx.BoxSizer(wx.HORIZONTAL)

        # weight selection
        self.weight_txt = wx.StaticText(self, label='For agglomerating and intersections:')
        self.weight_btn = wx.RadioBox(self, style = wx.RA_SPECIFY_ROWS,
                                      choices=['Include edge weight', 'Ignore edge weight'])
        self.weight_btn.Bind(wx.EVT_BUTTON, self.weight)
        self.weight_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.weight_btn.SetSelection(0)

        # agglomerate
        self.agglom_txt = wx.StaticText(self, label='Select taxonomic level for network agglomeration:')
        self.agglom_box = wx.RadioBox(self, style = wx.RA_SPECIFY_ROWS,
                                      choices=['Species', 'Genus', 'Family', 'Order', 'Class', 'Phylum'])
        self.agglom_box.Bind(wx.EVT_MOTION, self.update_help)
        self.agglom_box.SetSelection(0)
        self.agglom_btn = wx.Button(self, label='Run agglomeration', size=btnsize)
        self.agglom_btn.Bind(wx.EVT_BUTTON, self.agglomerate)
        self.agglom_btn.Bind(wx.EVT_MOTION, self.update_help)

        # get property types
        self.get_btn = wx.Button(self, label='Get list of properties', size=btnsize)
        self.get_btn.Bind(wx.EVT_BUTTON, self.get_properties)
        self.get_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.property_list = wx.ListBox(self, size=(300, 40), style=wx.LB_MULTIPLE)
        self.property_list.Bind(wx.EVT_MOTION, self.update_help)
        self.cor_btn = wx.Button(self, label='Correlate properties', size=btnsize)
        self.cor_btn.Bind(wx.EVT_BUTTON, self.correlate_properties)
        self.cor_btn.Bind(wx.EVT_MOTION, self.update_help)

        # get networks
        self.net_btn = wx.Button(self, label='Get list of networks', size=btnsize)
        self.net_btn.Bind(wx.EVT_BUTTON, self.get_networks)
        self.net_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.network_list = wx.ListBox(self, size=(300, 40), style=wx.LB_MULTIPLE)
        self.network_list.Bind(wx.EVT_MOTION, self.update_help)

        # fractions and sets
        self.fraction_txt = wx.StaticText(self, label='Fractions for intersections')
        self.fraction_ctrl = wx.TextCtrl(self, value='0.5;1', size=btnsize)
        self.fraction_ctrl.Bind(wx.EVT_MOTION, self.update_help)

        # set button
        self.set_btn = wx.Button(self, label='Construct sets', size=btnsize)
        self.set_btn.Bind(wx.EVT_BUTTON, self.get_sets)
        self.set_btn.Bind(wx.EVT_MOTION, self.update_help)

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
        self.leftsizer.Add(self.weight_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.weight_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.agglom_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.Add(self.agglom_box, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.AddSpacer(20)
        self.leftsizer.Add(self.agglom_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.leftsizer.AddSpacer(20)

        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.net_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.network_list, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.fraction_txt, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.Add(self.fraction_ctrl, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.set_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.get_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.property_list, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.AddSpacer(20)
        self.rightsizer.Add(self.cor_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)

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
        self.paddingsizer.Add(self.fullsizer, 0, wx.EXPAND | wx.ALL, 30)
        self.SetSizerAndFit(self.paddingsizer)
        self.Fit()

        # help strings for buttons
        self.buttons = {self.weight_btn: 'Intersections with weight only include edges with matching weights.',
                        self.agglom_box: 'Specify taxonomic level for agglomeration.',
                        self.agglom_btn: 'Merges edges if the taxa have the same taxonomic levels.',
                        self.get_btn: 'Get list of properties in database.',
                        self.property_list: 'Select properties for correlations. ',
                        self.cor_btn: 'Correlate taxon abundances to properties.',
                        self.net_btn: 'Get list of networks in database.',
                        self.network_list: 'Select networks to include in sets.',
                        self.fraction_ctrl: 'Fractions for partial intersections.',
                        self.logbox: 'Logging information for mako.',
                        self.set_btn: 'Construct set nodes in Neo4j database.',
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

    def weight(self, event):
        """
        Sets the weight parameter.
        :param event:
        :return:
        """
        self.settings['weight'] = self.weight_btn.GetSelection()

    def agglomerate(self, event):
        """
        Starts worker for agglomerating networks.
        :param event:
        :return:
        """
        self.logbox.AppendText("Starting operation...\n")
        self.settings['agglom'] = self.agglom_box.GetString(self.agglom_box.GetSelection())
        eg = Thread(target=start_metastats, args=(self.settings,))
        eg.start()
        eg.join()

    def get_properties(self, event):
        """
        Gets properties from Neo4j database.
        :param event:
        :return:
        """
        eg = ThreadPoolExecutor()
        worker = eg.submit(query, self.settings, 'MATCH (n:Property) RETURN n.name')
        result = worker.result()
        property_types = set([x[key] for x in result for key in x])
        self.property_list.Set(list(property_types))

    def correlate_properties(self, event):
        """
        Correlates properties in Neo4j database.
        :param event:
        :return:
        """
        self.logbox.AppendText("Starting operation...\n")
        self.settings['variable'] = [self.property_list.GetString(i)
                                   for i in self.property_list.GetSelections()]
        eg = Thread(target=start_metastats, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['variable'] = None

    def get_networks(self, event):
        """
        Gets network nodes from Neo4j database.
        :param event:
        :return:
        """
        eg = ThreadPoolExecutor()
        worker = eg.submit(query, self.settings, 'MATCH (n) WHERE n:Network OR n:Set RETURN n')
        del_values = _get_unique(worker.result(), key='n')
        self.network_list.Set(list(del_values))

    def get_sets(self, event):
        """
        Makes networks that contain network overlap.
        :param event:
        :return:
        """
        self.logbox.AppendText("Starting operation...\n")
        self.settings['set'] = True
        fracs = self.fraction_ctrl.GetValue()
        fracs = [float(x) for x in fracs.split(';')]
        self.settings['fraction'] = fracs
        self.settings['networks'] = [self.network_list.GetString(i)
                                     for i in self.network_list.GetSelections()]
        eg = Thread(target=start_netstats, args=(self.settings,))
        eg.start()
        eg.join()
        self.settings['networks'] = None
        self.settings['set'] = False


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