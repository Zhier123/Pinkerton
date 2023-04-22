from ryu.base import app_manager
from ryu.base.app_manager import lookup_service_brick
from ryu.ofproto import ofproto_v1_3
from ryu.controller.handler import set_ev_cls
from ryu.controller.handler import MAIN_DISPATCHER, CONFIG_DISPATCHER, DEAD_DISPATCHER, HANDSHAKE_DISPATCHER
from ryu.controller import ofp_event
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet, arp
from ryu.lib import hub
from ryu.topology import event
from ryu.topology.api import get_host, get_link, get_switch
from ryu.topology.switches import LLDPPacket
from ryu.lib.packet import ether_types

import networkx as nx
import copy
import time


GET_TOPOLOGY_INTERVAL = 2
SEND_ECHO_REQUEST_INTERVAL = .05
GET_DELAY_INTERVAL = 2


class NetworkAwareness(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(NetworkAwareness, self).__init__(*args, **kwargs)
        self.switch_info = {}  # dpid: datapath
        self.link_info = {}  # (s1, s2): s1.port
        self.port_link={} # s1,port:s1,s2
        self.port_info = {}  # dpid: (ports linked hosts)
        self.topo_map = nx.Graph()
        self.topo_thread = hub.spawn(self._get_topology)
        self.lldp_delay = {}
        self.delay = {}
        self.switches = None
        self.weight = 'delay'
        self.controller_switch_delay = {}


    def add_flow(self, datapath, priority, match, actions):
        dp = datapath
        ofp = dp.ofproto
        parser = dp.ofproto_parser

        inst = [parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=dp, priority=priority, match=match, instructions=inst)
        dp.send_msg(mod)

    #delete
    def delete_flow(self, datapath, match):
        ofp = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = []

        echo_req = parser.OFPFlowMod(datapath, 0, 0, 0, ofp.OFPFC_DELETE, 0, 0, 0, ofp.OFP_NO_BUFFER, ofp.OFPP_ANY, ofp.OFPG_ANY, ofp.OFPFF_SEND_FLOW_REM, match, inst)
        datapath.send_msg(echo_req)


    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath
        ofp = dp.ofproto
        parser = dp.ofproto_parser

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)]
        self.add_flow(dp, 0, match, actions)
    
    #add
    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        if msg.reason in [ofproto.OFPPR_ADD, ofproto.OFPPR_MODIFY]:
            datapath.ports[msg.desc.port_no] = msg.desc
            self.topo_map.clear()

            for dpid in self.port_info.keys():
                for port in self.port_info[dpid]:
                    match = parser.OFPMatch(in_port = port)
                    self.delete_flow(self.switch_info[dpid], match)
        elif msg.reason == ofproto.OFPPR_DELETE:
            datapath.ports.pop(msg.desc.port_no, None)
        else:
            return
        self.send_event_to_observers(ofp_event.EventOFPPortStateChange(datapath, msg.reason, msg.desc.port_no), datapath.state)

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def state_change_handler(self, ev):
        dp = ev.datapath
        dpid = dp.id

        if ev.state == MAIN_DISPATCHER:
            self.switch_info[dpid] = dp

        if ev.state == DEAD_DISPATCHER:
            del self.switch_info[dpid]
    
    @set_ev_cls(ofp_event.EventOFPEchoReply, [MAIN_DISPATCHER, CONFIG_DISPATCHER, HANDSHAKE_DISPATCHER])
    def echo_reply_handler(self, ev):
        now = time.time()
        try:
            echo = now - eval(ev.msg.data)
            self.controller_switch_delay[ev.msg.datapath.id] = echo * 1000
        except:
            return
    
    def send_echo_request(self, switch):
        datapath = switch.dp
        parser = datapath.ofproto_parser
        time_now = "%.6f" %time.time()
        data = time_now.encode()
        echo = parser.OFPEchoRequest(datapath, data = data)
        datapath.send_msg(echo)
        #time.sleep(0.5)

        
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath
        ofp = dp.ofproto
        parser = dp.ofproto_parser
        dpid = dp.id

        pkt = packet.Packet(msg.data)
        eth_pkt = pkt.get_protocol(ethernet.ethernet)
        arp_pku = pkt.get_protocol(arp.arp)
        pkt_type = eth_pkt.ethertype
        try:
            if pkt_type == ether_types.ETH_TYPE_LLDP:
                src_dpid, src_port_no = LLDPPacket.lldp_parse(msg.data)

                if self.switches is None:
                    self.switches = lookup_service_brick('switches')
                
                for port in self.switches.ports.keys():
                    if src_dpid == port.dpid and src_port_no == port.port_no:
                        self.lldp_delay[(src_dpid, dpid)] = self.switches.ports[port].delay * 1000
        except:
            return

    
    def _get_topology(self):
        _hosts, _switches, _links = None, None, None
        while True:
            hosts = get_host(self)
            switches = get_switch(self)
            links = get_link(self)

            # update topo_map when topology change
            if [str(x) for x in hosts] == _hosts and [str(x) for x in switches] == _switches and [str(x) for x in links] == _links:
                continue
            _hosts, _switches, _links = [str(x) for x in hosts], [str(x) for x in switches], [str(x) for x in links]

            for switch in switches:
                self.port_info.setdefault(switch.dp.id, set())
                # record all ports
                for port in switch.ports:
                    self.port_info[switch.dp.id].add(port.port_no)
                    self.send_echo_request(switch)
                    hub.sleep(SEND_ECHO_REQUEST_INTERVAL)

            for host in hosts:
                # take one ipv4 address as host id
                if host.ipv4:
                    self.link_info[(host.port.dpid, host.ipv4[0])] = host.port.port_no
                    self.topo_map.add_edge(host.ipv4[0], host.port.dpid, hop=1, delay=0, is_host=True)
            for link in links:
                # delete ports linked switches
                self.port_info[link.src.dpid].discard(link.src.port_no)
                self.port_info[link.dst.dpid].discard(link.dst.port_no)

                # s1 -> s2: s1.port, s2 -> s1: s2.port
                self.port_link[(link.src.dpid,link.src.port_no)]=(link.src.dpid, link.dst.dpid)
                self.port_link[(link.dst.dpid,link.dst.port_no)] = (link.dst.dpid, link.src.dpid)

                self.link_info[(link.src.dpid, link.dst.dpid)] = link.src.port_no
                self.link_info[(link.dst.dpid, link.src.dpid)] = link.dst.port_no
               # self.topo_map.add_edge(link.src.dpid, link.dst.dpid, hop=1, is_host=False)

                delay_src_dst = 0
                delay_dst_src = 0
                delay_cont_src = 0
                delay_cont_dst = 0

                if (link.src.dpid, link.dst.dpid) in self.lldp_delay:
                    delay_src_dst = self.lldp_delay[(link.src.dpid, link.dst.dpid)]

                if (link.dst.dpid, link.src.dpid) in self.lldp_delay:
                    delay_dst_src = self.lldp_delay[(link.dst.dpid, link.src.dpid)]

                if link.src.dpid in self.controller_switch_delay:
                    delay_cont_src = self.controller_switch_delay[link.src.dpid]
                    
                if link.dst.dpid in self.controller_switch_delay:
                    delay_cont_dst = self.controller_switch_delay[link.dst.dpid]

                delay = (delay_dst_src + delay_src_dst - delay_cont_dst - delay_cont_src) / 2
                if delay < 0:
                    delay = 0
                
                self.delay[(link.src.dpid, link.dst.dpid)] = delay
                self.topo_map.add_edge(link.src.dpid, link.dst.dpid, hop = 1, delay = delay, is_hosy = False)


            if self.weight == 'hop':
                self.show_topo_map()
            else:
                self.show_delay_map()

            hub.sleep(GET_TOPOLOGY_INTERVAL)

    def shortest_path(self, src, dst, weight='hop'):
        try:
            paths = list(nx.shortest_simple_paths(self.topo_map, src, dst, weight=weight))
            self.logger.info('path: {} -> {}'.format(src, dst))
            return paths[0]
        except:
            self.logger.info('host not find/no path')

    def show_topo_map(self):
        self.logger.info('topo-hop map:')
        self.logger.info('{:^10s}  ->  {:^10s}'.format('node', 'node'))
        for src, dst in self.topo_map.edges:
            self.logger.info('{:^10s}      {:^10s}'.format(str(src), str(dst)))
        self.logger.info('\n')
    
    def show_delay_map(self):
        self.logger.info('topo-delay map:')
        self.logger.info('{:^10s}  ->  {:^10s}  {}'.format('node', 'node', 'delay'))
        for src, dst in self.topo_map.edges:
            self.logger.info('{:^10s}      {:^10s}'.format(str(src), str(dst)) + '%.2f'%self.topo_map.edges[src, dst]['delay'] + 'ms')
        self.logger.info('\n')

