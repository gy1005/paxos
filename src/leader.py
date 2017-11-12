
from threading import Thread, Condition
from scout import Scout
from commander import Commander
from utils import *


class Leader(Thread):
    def __init__(self, process, id, num_server):
        Thread.__init__(self)
        self.id = id
        self.recv_queue = []
        self.recv_cv = Condition()
        self.ballot_num = BallotNum(0, self.id)
        self.active = False
        self.proposals = {}
        self.process = process
        self.num_server = num_server
        self.scout_id = 0
        self.commander_id = 0
        self.commanders = {}
        self.scouts = {}
        self.thread_lock = Lock()

    def run(self):
        new_scout_id = ScoutID(self.id, self.scout_id)
        scout_thread = Scout(self, new_scout_id, self.num_server, self.ballot_num)
        scout_thread.start()
        self.thread_lock.acquire()
        self.scouts[new_scout_id.scout_id] = scout_thread
        self.thread_lock.release()
        self.scout_id += 1
        while True:
            self.recv_cv.acquire()
            while len(self.recv_queue) == 0:
                self.recv_cv.wait()
            recv_msg = self.recv_queue.pop(0)
            self.recv_cv.release()
            assert recv_msg.type == 'propose' or recv_msg.type == 'adopted' or recv_msg.type == 'preempted'
            if recv_msg.type == 'propose':
                if recv_msg.slot not in self.proposals:
                    self.proposals[recv_msg.slot] = Proposal(recv_msg.slot, recv_msg.command)
                    if self.active:
                        pvalue = Pvalue(self.ballot_num, recv_msg.slot, recv_msg.command)
                        new_commander_id = CommanderID(self.id, self.commander_id)
                        commander_thread = Commander(self, new_commander_id, self.num_server, pvalue)
                        commander_thread.start()
                        self.thread_lock.acquire()
                        self.commanders[new_commander_id.commander_id] = commander_thread
                        self.thread_lock.release()
                        self.commander_id += 1
            elif recv_msg.type == 'adopted':
                new_pvalues = pmax(recv_msg.pvalues)
                for slot, pvalue in new_pvalues.iteritems():
                    new_proposal = Proposal(pvalue.slot, pvalue.command)
                    self.proposals[slot] = new_proposal

                for slot, proposal in self.proposals.iteritems():

                    pvalue = Pvalue(self.ballot_num, proposal.slot, proposal.command)
                    new_commander_id = CommanderID(self.id, self.commander_id)
                    commander_thread = Commander(self, new_commander_id, self. num_server, pvalue)
                    commander_thread.start()
                    self.thread_lock.acquire()
                    self.commanders[new_commander_id.commander_id] = commander_thread
                    self.thread_lock.release()
                    self.commander_id += 1
                self.active = True
            else:
                if recv_msg.ballot_num > self.ballot_num:
                    self.active = False
                    self.ballot_num.round = recv_msg.ballot_num.round + 1
                    new_scout_id = ScoutID(self.id, self.scout_id)
                    scout_thread = Scout(self, new_scout_id, self.num_server, self.ballot_num)
                    scout_thread.start()
                    self.thread_lock.acquire()
                    self.scouts[new_scout_id.scout_id] = scout_thread
                    self.thread_lock.release()
                    self.scout_id += 1
