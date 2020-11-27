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
from mako.scripts.wrapper import start_wrapper
from mako.scripts.utils import _resource_path, query, _get_unique
import logging
import logging.handlers

logger = logging.getLogger()
wxLogEvent, EVT_WX_LOG_EVENT = wx.lib.newevent.NewEvent()


class WrapPanel(wx.Panel):
    """
    Panel for running manta or anuran on the database.
    Limit, iter and perm settings are left out,
    since these probably do not have a large impact on outcome.
    """
    def __init__(self, parent):
        wx.Panel.__init__(self, parent)
        # subscribe to inputs from tabwindow
        pub.subscribe(self.set_config, 'config')

        self.frame = parent
        self.settings = {'address': 'bolt://localhost:7687',
                         'anuran': False,
                         'b': False,
                         'centrality': True,
                         'comparison': False,
                         'core': 4,
                         'cr': False,
                         'cs': None,
                         'draw': False,
                         'edgescale': 0.8,
                         'error': 0.1,
                         'fp': _resource_path(''),
                         'iter': 20,
                         'limit': 2,
                         'manta': True,
                         'max': 2,
                         'min': 2,
                         'ms': 0.2,
                         'n': None,
                         'network': False,
                         'networks': None,
                         'nperm': 10,
                         'password': 'neo4j',
                         'perm': 3,
                         'prev': [1],
                         'ratio': 0.8,
                         'rel': 20,
                         'sample': False,
                         'sign': True,
                         'size': [1],
                         'stats': 'bonferroni',
                         'store_config': False,
                         'subset': 0.8,
                         'username': 'neo4j'}
        btnsize = (300, -1)
        boxsize = (700, 400)

        # defines columns
        self.leftsizer = wx.BoxSizer(wx.VERTICAL)
        self.rightsizer = wx.BoxSizer(wx.VERTICAL)
        self.topsizer = wx.BoxSizer(wx.HORIZONTAL)
        self.mantasizer = wx.BoxSizer(wx.HORIZONTAL)
        self.mantaleftsizer = wx.BoxSizer(wx.VERTICAL)
        self.mantarightsizer = wx.BoxSizer(wx.VERTICAL)
        self.anuransizer = wx.BoxSizer(wx.HORIZONTAL)
        self.anuranleftsizer = wx.BoxSizer(wx.VERTICAL)
        self.anuranrightsizer = wx.BoxSizer(wx.VERTICAL)
        self.bottomsizer = wx.BoxSizer(wx.VERTICAL)
        self.fullsizer = wx.BoxSizer(wx.VERTICAL)
        self.paddingsizer = wx.BoxSizer(wx.HORIZONTAL)

        # alg selection
        self.alg_txt = wx.StaticText(self, label='Show settings for:')
        self.alg_btn = wx.RadioBox(self, style = wx.RA_SPECIFY_COLS,
                                         choices=['manta', 'anuran'])
        self.alg_btn.Bind(wx.EVT_RADIOBOX, self.show_alg)
        self.alg_btn.Bind(wx.EVT_MOTION, self.update_help)

        # run button

        self.run_btn = wx.Button(self, label='Run algorithm')
        self.run_btn.Bind(wx.EVT_BUTTON, self.run_wrapper)
        self.run_btn.Bind(wx.EVT_MOTION, self.update_help)

        # get networks
        self.net_btn = wx.Button(self, label='Get list of networks', size=btnsize)
        self.net_btn.Bind(wx.EVT_BUTTON, self.get_networks)
        self.net_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.net_txt = wx.StaticText(self, label='Run algorithms on networks:')
        self.network_list = wx.ListBox(self, size=(300, 40), style=wx.LB_MULTIPLE)
        self.network_list.Bind(wx.EVT_MOTION, self.update_help)

        # manta settings
        self.choice_btn = wx.CheckBox(self, label='Treat edge weights as -1 and 1')
        self.choice_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.rob_btn = wx.CheckBox(self, label='Calculate cluster robustness')
        self.rob_btn.Bind(wx.EVT_MOTION, self.update_help)

        self.minmax_txt = wx.StaticText(self, label='Set min, max number of clusters and min cluster size')
        self.min_slider = wx.Slider(self, value=2, minValue=2, maxValue=6,
                                    style=wx.SL_HORIZONTAL | wx.SL_LABELS, size=btnsize)
        self.min_slider.Bind(wx.EVT_MOTION, self.update_help)
        self.max_slider = wx.Slider(self, value=3, minValue=2, maxValue=10,
                                    style=wx.SL_HORIZONTAL | wx.SL_LABELS, size=btnsize)
        self.max_slider.Bind(wx.EVT_MOTION, self.update_help)
        self.clus_slider = wx.Slider(self, value=80, minValue=10, maxValue=100,
                                    style=wx.SL_HORIZONTAL | wx.SL_LABELS, size=btnsize)
        self.clus_slider.Bind(wx.EVT_MOTION, self.update_help)

        self.sub_txt = wx.StaticText(self, label='Fraction of edges for partial iterations')
        self.sub_slider = wx.Slider(self, value=80, minValue=10, maxValue=100,
                                    style=wx.SL_HORIZONTAL | wx.SL_LABELS, size=btnsize)
        self.sub_slider.Bind(wx.EVT_MOTION, self.update_help)

        self.perm_manta_txt = wx.StaticText(self, label='Partial iterations')
        self.perm_slider = wx.Slider(self, value=100, minValue=10, maxValue=1000,
                                     style=wx.SL_HORIZONTAL | wx.SL_LABELS, size=btnsize)
        self.perm_slider.Bind(wx.EVT_MOTION, self.update_help)

        self.ratio_txt = wx.StaticText(self, label='Ratio of partial iterations')
        self.ratio_slider = wx.Slider(self, value=80, minValue=10, maxValue=100,
                                    style=wx.SL_HORIZONTAL | wx.SL_LABELS, size=btnsize)
        self.ratio_slider.Bind(wx.EVT_MOTION, self.update_help)

        self.scale_txt = wx.StaticText(self, label='Threshold for weak cluster')
        self.scale_slider = wx.Slider(self, value=80, minValue=10, maxValue=100,
                                    style=wx.SL_HORIZONTAL | wx.SL_LABELS, size=btnsize)
        self.scale_slider.Bind(wx.EVT_MOTION, self.update_help)

        self.rel_txt = wx.StaticText(self, label='Maximum number of iterations')
        self.rel_slider = wx.Slider(self, value=20, minValue=10, maxValue=100,
                                    style=wx.SL_HORIZONTAL | wx.SL_LABELS, size=btnsize)
        self.rel_slider.Bind(wx.EVT_MOTION, self.update_help)

        self.e_txt = wx.StaticText(self, label='Fraction of edges for reliability tests')
        self.error_slider = wx.Slider(self, value=80, minValue=10, maxValue=100,
                                    style=wx.SL_HORIZONTAL | wx.SL_LABELS, size=btnsize)
        self.error_slider.Bind(wx.EVT_MOTION, self.update_help)

        # anuran settings
        self.weight_btn = wx.CheckBox(self, label='Do not use edge weights')
        self.weight_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.sample_btn = wx.CheckBox(self, label='Resample networks')
        self.sample_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.central_btn = wx.CheckBox(self, label='Evaluate centralities')
        self.central_btn.Bind(wx.EVT_MOTION, self.update_help)
        self.network_btn = wx.CheckBox(self, label='Evaluate network properties')
        self.network_btn.Bind(wx.EVT_MOTION, self.update_help)

        self.fraction_txt = wx.StaticText(self, label='Fractions for intersections')
        self.fraction_ctrl = wx.TextCtrl(self, value='0.5;1', size=btnsize)
        self.fraction_ctrl.Bind(wx.EVT_MOTION, self.update_help)

        self.sample_txt = wx.StaticText(self, label='Size of resampling:')
        self.sample_ctrl = wx.TextCtrl(self, value='', size=btnsize)
        self.sample_ctrl.Bind(wx.EVT_MOTION, self.update_help)

        self.n_txt = wx.StaticText(self, label='Sample numbers to resample:')
        self.n_box = wx.TextCtrl(self, value='', size=btnsize)
        self.n_box.Bind(wx.EVT_MOTION, self.update_help)

        self.core_txt = wx.StaticText(self, label='Size of synthetic core')
        self.core_ctrl = wx.TextCtrl(self, value='', size=btnsize)
        self.core_ctrl.Bind(wx.EVT_MOTION, self.update_help)

        self.prev_txt = wx.StaticText(self, label='Prevalence of synthetic core')
        self.prev_ctrl = wx.TextCtrl(self, value='', size=btnsize)
        self.prev_ctrl.Bind(wx.EVT_MOTION, self.update_help)

        self.perm_txt = wx.StaticText(self, label='Number of null models per network')
        self.perm_ctrl = wx.Slider(self, value=10, minValue=1, maxValue=100,
                                     style=wx.SL_HORIZONTAL | wx.SL_LABELS)
        self.perm_ctrl.Bind(wx.EVT_MOTION, self.update_help)

        self.nperm_txt = wx.StaticText(self, label='Number of null model permutations')
        self.nperm_ctrl = wx.Slider(self, value=50, minValue=1, maxValue=500,
                                     style=wx.SL_HORIZONTAL | wx.SL_LABELS)
        self.nperm_ctrl.Bind(wx.EVT_MOTION, self.update_help)

        self.cpu_txt = wx.StaticText(self, label='Number of CPU cores to use:')
        self.core_slider = wx.Slider(self, value=2, minValue=1, maxValue=os.cpu_count(),
                                     style=wx.SL_HORIZONTAL | wx.SL_LABELS)
        self.core_slider.Bind(wx.EVT_MOTION, self.update_help)

        self.pval_txt = wx.StaticText(self, label='Multiple testing correction')
        self.pval_btn = wx.RadioBox(self, style=wx.RA_SPECIFY_ROWS,
                                          choices=['bonferroni',
                                                   'sidak',
                                                   'holm-sidak',
                                                   'holm',
                                                   'simes-hochberg',
                                                   'hommel',
                                                   'fdr_bh',
                                                   'fdr_by',
                                                   'fdr_tsbh',
                                                   'fdr_tsbky'])
        self.pval_btn.Bind(wx.EVT_MOTION, self.update_help)

        # Logger
        self.logbox = wx.TextCtrl(self, value='', size=boxsize, style=wx.TE_MULTILINE)
        self.logbox.Bind(wx.EVT_MOTION, self.update_help)
        self.logbox.Bind(EVT_WX_LOG_EVENT, self.log_event)

        handler = LogHandler(ctrl=self.logbox)
        logger.addHandler(handler)
        self.logbox.SetForegroundColour(wx.WHITE)
        self.logbox.SetBackgroundColour(wx.BLACK)

        self.leftsizer.Add(self.alg_txt, flag=wx.ALIGN_LEFT)
        self.leftsizer.Add(self.alg_btn, flag=wx.ALIGN_LEFT)
        self.leftsizer.AddSpacer(10)
        self.leftsizer.Add(self.net_btn, flag=wx.ALIGN_LEFT)
        self.leftsizer.AddSpacer(10)
        self.leftsizer.Add(self.net_txt, flag=wx.ALIGN_LEFT)
        self.leftsizer.Add(self.network_list, flag=wx.ALIGN_LEFT)
        self.leftsizer.AddSpacer(20)

        self.rightsizer.AddSpacer(50)
        self.rightsizer.Add(self.run_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.rightsizer.AddSpacer(10)
        self.rightsizer.Add(self.cpu_txt, flag=wx.ALIGN_LEFT)
        self.rightsizer.Add(self.core_slider, flag=wx.ALIGN_CENTER_HORIZONTAL)

        # manta settings
        self.mantaleftsizer.Add(self.choice_btn, flag=wx.ALIGN_LEFT)
        self.mantaleftsizer.Add(self.rob_btn, flag=wx.ALIGN_LEFT)
        self.mantaleftsizer.AddSpacer(10)
        self.mantaleftsizer.Add(self.minmax_txt, flag=wx.ALIGN_LEFT)
        self.mantaleftsizer.Add(self.min_slider, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.mantaleftsizer.Add(self.max_slider, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.mantaleftsizer.Add(self.clus_slider, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.mantaleftsizer.AddSpacer(10)
        self.mantaleftsizer.Add(self.sub_txt, flag=wx.ALIGN_LEFT)
        self.mantaleftsizer.Add(self.sub_slider, flag=wx.ALIGN_CENTER_HORIZONTAL)

        self.mantarightsizer.Add(self.perm_manta_txt, flag=wx.ALIGN_LEFT)
        self.mantarightsizer.Add(self.perm_slider, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.mantarightsizer.AddSpacer(10)
        self.mantarightsizer.Add(self.ratio_txt, flag=wx.ALIGN_LEFT)
        self.mantarightsizer.Add(self.ratio_slider, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.mantarightsizer.AddSpacer(10)
        self.mantarightsizer.Add(self.scale_txt, flag=wx.ALIGN_LEFT)
        self.mantarightsizer.Add(self.scale_slider, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.mantarightsizer.AddSpacer(10)
        self.mantarightsizer.AddSpacer(10)
        self.mantarightsizer.Add(self.rel_txt, flag=wx.ALIGN_LEFT)
        self.mantarightsizer.Add(self.rel_slider, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.mantarightsizer.Add(self.e_txt, flag=wx.ALIGN_LEFT)
        self.mantarightsizer.Add(self.error_slider, flag=wx.ALIGN_CENTER_HORIZONTAL)

        # anuran settings
        self.anuranleftsizer.Add(self.weight_btn, flag=wx.ALIGN_LEFT)
        self.anuranleftsizer.Add(self.sample_btn, flag=wx.ALIGN_LEFT)
        self.anuranleftsizer.Add(self.central_btn, flag=wx.ALIGN_LEFT)
        self.anuranleftsizer.Add(self.network_btn, flag=wx.ALIGN_LEFT)
        self.anuranleftsizer.AddSpacer(10)
        self.anuranleftsizer.Add(self.fraction_txt, flag=wx.ALIGN_LEFT)
        self.anuranleftsizer.Add(self.fraction_ctrl, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.anuranleftsizer.AddSpacer(10)
        self.anuranleftsizer.Add(self.sample_txt, flag=wx.ALIGN_LEFT)
        self.anuranleftsizer.Add(self.sample_ctrl, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.anuranleftsizer.AddSpacer(10)
        self.anuranleftsizer.Add(self.n_txt, flag=wx.ALIGN_LEFT)
        self.anuranleftsizer.Add(self.n_box, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.anuranleftsizer.AddSpacer(10)
        self.anuranleftsizer.Add(self.core_txt, flag=wx.ALIGN_LEFT)
        self.anuranleftsizer.Add(self.core_ctrl, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.anuranleftsizer.AddSpacer(10)
        self.anuranleftsizer.Add(self.prev_txt, flag=wx.ALIGN_LEFT)
        self.anuranleftsizer.Add(self.prev_ctrl, flag=wx.ALIGN_CENTER_HORIZONTAL)

        self.anuranrightsizer.Add(self.perm_txt, flag=wx.ALIGN_LEFT)
        self.anuranrightsizer.Add(self.perm_ctrl, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.anuranrightsizer.AddSpacer(10)
        self.anuranrightsizer.Add(self.nperm_txt, flag=wx.ALIGN_LEFT)
        self.anuranrightsizer.Add(self.nperm_ctrl, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.anuranrightsizer.AddSpacer(10)
        self.anuranrightsizer.Add(self.pval_txt, flag=wx.ALIGN_LEFT)
        self.anuranrightsizer.Add(self.pval_btn, flag=wx.ALIGN_CENTER_HORIZONTAL)
        self.anuranrightsizer.AddSpacer(10)

        self.bottomsizer.AddSpacer(10)
        self.bottomsizer.Add(self.logbox, flag=wx.ALIGN_CENTER)

        self.mantasizer.Add(self.mantaleftsizer, flag=wx.ALIGN_CENTER)
        self.mantasizer.AddSpacer(50)
        self.mantasizer.Add(self.mantarightsizer, flag=wx.ALIGN_CENTER)

        self.anuransizer.Add(self.anuranleftsizer, flag=wx.ALIGN_CENTER)
        self.anuransizer.AddSpacer(50)
        self.anuransizer.Add(self.anuranrightsizer, flag=wx.ALIGN_CENTER)

        self.topsizer.Add(self.leftsizer, flag=wx.ALIGN_CENTER_VERTICAL)
        self.topsizer.AddSpacer(50)
        self.topsizer.Add(self.rightsizer, flag=wx.ALIGN_CENTER_VERTICAL)

        self.fullsizer.Add(self.topsizer, flag=wx.ALIGN_CENTER)
        self.fullsizer.Add(self.mantasizer, flag=wx.ALIGN_CENTER)
        self.fullsizer.Add(self.anuransizer, flag=wx.ALIGN_CENTER)
        self.fullsizer.Add(self.bottomsizer, flag=wx.ALIGN_CENTER)
        # add padding sizer
        # add padding sizer
        self.paddingsizer.Add(self.fullsizer, 0, wx.EXPAND | wx.ALL, 30)
        self.anuransizer.ShowItems(show=False)
        self.SetSizerAndFit(self.paddingsizer)
        self.Fit()

        # help strings for buttons
        self.buttons = {self.alg_btn: 'Show settings for specific algorithm.',
                        self.run_btn: 'Run algorithm with displayed settings.',
                        self.net_btn: 'Get list of networks in database.',
                        self.network_list: 'Select networks to include in sets.',
                        self.choice_btn: 'Treat edge weights as -1 and 1.',
                        self.rob_btn: 'Estimate cluster robustness.',
                        self.min_slider: 'Set minimum cluster number.',
                        self.max_slider: 'Set maximum cluster number.',
                        self.clus_slider: 'Set minimum cluster size as % of network.',
                        self.perm_slider: 'Number of partial iterations.',
                        self.sub_slider: 'Percentage of edges for subsetting if the input graph is not balanced.',
                        self.ratio_slider: 'Percentage of scores that need to be positive or negative for stability.',
                        self.scale_slider: 'Threshold for weak cluster assignments; '
                                           'larger threshold gets a larger cluster.',
                        self.rel_slider: 'Number of permutation iterations for reliability estimates.',
                        self.error_slider: 'Fraction of edges to rewire for reliability tests.',
                        self.weight_btn: 'If selected, signs of edge weights are not taken into account.',
                        self.sample_btn: 'Resample your networks to observe the impact of increasing network number',
                        self.central_btn: 'If selected, compares observed centrality rankings to null models.',
                        self.network_btn: 'If selected, compares observed network properties to null models.',
                        self.fraction_ctrl: 'Percentages for partial intersections. Separate by ;',
                        self.sample_ctrl: 'Maximum number of resamplings across increasing network number.',
                        self.n_box: 'Numbers of networks to test during resampling. Separate by ;',
                        self.core_ctrl: 'Size of core in true positive model. Specify as fractions separated by ;',
                        self.prev_ctrl: 'Prevalence of core in true positive model. Specify as fractions separated by ;',
                        self.perm_ctrl: 'Number of null models generated per input network.',
                        self.nperm_ctrl: 'Number of combinations of null models used in tests.',
                        self.core_slider: 'Number of CPU cores to use for anuran.',
                        self.pval_btn: 'Choose a multiple-testing method.',
                        self.logbox: 'Logging information for mako.',
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

    def show_alg(self, event):
        """
        Shows buttons with filenames
        for tab-delimited files.
        """
        if self.alg_btn.GetString(self.alg_btn.GetSelection()) == 'anuran':
            self.mantasizer.ShowItems(show=False)
            self.anuransizer.ShowItems(show=True)
        else:
            self.mantasizer.ShowItems(show=True)
            self.anuransizer.ShowItems(show=False)
        self.Layout()

    def get_networks(self, event):
        eg = ThreadPoolExecutor()
        worker = eg.submit(query, self.settings, 'MATCH (n) WHERE n:Network OR n:Set RETURN n')
        del_values = _get_unique(worker.result(), key='n')
        self.network_list.Set(list(del_values))

    def run_wrapper(self, event):
        self.logbox.AppendText("Starting operation...\n")
        # get parameters for running wrapper
        algorithm = self.alg_btn.GetString(self.alg_btn.GetSelection())
        self.settings['networks'] = [self.network_list.GetString(i) for i in self.network_list.GetSelections()]
        if algorithm == 'manta':
            self.settings.pop('anuran')
            self.settings['b'] = self.choice_btn.GetValue()
            self.settings['cr'] = self.rob_btn.GetValue()
            self.settings['edgescale'] = self.scale_slider.GetValue()/100
            self.settings['error'] = self.error_slider.GetValue()/100
            self.settings['manta'] = True
            self.settings['max'] = self.max_slider.GetValue()
            self.settings['min'] = self.min_slider.GetValue()
            self.settings['ms'] = self.clus_slider.GetValue()/100
            self.settings['perm'] = self.perm_slider.GetValue()
            self.settings['ratio'] = self.ratio_slider.GetValue()/100
            self.settings['rel'] = self.rel_slider.GetValue()
            self.settings['subset'] = self.sub_slider.GetValue()/100
        elif algorithm == 'anuran':
            self.settings.pop('manta')
            self.settings['anuran'] = True
            self.settings['centrality'] = self.central_btn.GetValue()
            self.settings['core'] = self.core_slider.GetValue()
            self.settings['cs'] = get_number_list(numberstring=self.core_ctrl.GetValue())
            self.settings['n'] = get_number_list(numberstring=self.n_box.GetValue())
            self.settings['graph'] = self.network_btn.GetValue()
            self.settings['nperm'] = self.nperm_ctrl.GetValue()
            self.settings['perm'] = self.perm_ctrl.GetValue()
            self.settings['prev'] = get_number_list(numberstring=self.prev_ctrl.GetValue())
            self.settings['sample'] = self.sample_btn.GetValue()
            self.settings['sign'] = self.weight_btn.GetValue()
            self.settings['stats'] = self.pval_btn.GetString(self.pval_btn.GetSelection())
        eg = Thread(target=start_wrapper, args=(self.settings,))
        eg.start()
        eg.join()


def get_number_list(numberstring):
    fracs = None
    if numberstring:
        fracs = [float(x) for x in numberstring.split(';')]
    return fracs


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