
from threading import Thread, Lock, Condition
from message import *
from utils import *

WINDOW = 5

class Replica(Thread):
    def __init__(self, process, id, leader, init_state = 0):
        Thread.__init__(self)
        self.id = id
        self.slot_in = 1
        self.slot_out = 1
        self.init_state = init_state
        self.requests = []
        self.proposals = {}
        self.decisions = {}
        self.recv_queue = []
        self.recv_cv = Condition()
        self.leader = leader
        self.process = process


    def propose(self):
        while self.slot_in < self.slot_out + WINDOW and len(self.requests) > 0:
            request = self.requests[0]
            if self.slot_in not in self.decisions:
                self.requests.pop(0)
                assert self.slot_in not in self.proposals
                self.proposals[self.slot_in] = Proposal(self.slot_in, request)
                msg_to_leader = ProposeMessage(self.slot_in, request)
                self.leader.recv_cv.acquire()
                self.leader.recv_queue.append(msg_to_leader)
                if len(self.leader.recv_queue) == 1:
                    self.leader.recv_cv.notify()
                self.leader.recv_cv.release()
            self.slot_in += 1

    def perform(self, command):
        flag = False
        for slot, decision in self.decisions.iteritems():
            if decision.slot < self.slot_out and decision.command == command:
                flag = True
                break
        if flag:
            self.slot_out += 1
        else:
            screen_lock.acquire()
            # print 'perform'
            screen_lock.release()
            command_contents = command.split(' ')
            if command_contents[0] == 'msg':
                self.process.thread_lock.acquire()
                # assert len(self.process.chat_log) == int(command_contents[1])
                if int(command_contents[1]) not in self.process.chat_log:
                    new_message = Message(str(len(self.process.chat_log)), command_contents[2])
                    self.process.chat_log[int(command_contents[1])] = new_message
                if int(command_contents[1]) in self.process.msg_wait_for_resp:
                    del self.process.msg_wait_for_resp[int(command_contents[1])]
                    self.process.master_conn.sendall('ack ' + command_contents[1] + ' '
                                                     + str(len(self.process.chat_log) - 1))
                self.slot_out += 1
                self.process.thread_lock.release()






    def run(self):
        while True:
            self.recv_cv.acquire()
            while len(self.recv_queue) == 0:
                self.recv_cv.wait()
            msg = self.recv_queue.pop(0)

            assert msg.type == 'request' or msg.type == 'decision'
            if msg.type == 'request':
                self.requests.append(msg.command)
            else:
                screen_lock.acquire()
                # print "decision received"

                screen_lock.release()
                decision = Decision(msg.slot, msg.command)
                self.decisions[msg.slot] = decision
                # print self.decisions
                # print self.slot_in, self.slot_out
                while self.slot_out in self.decisions:
                    decision = self.decisions[self.slot_out]
                    if self.slot_out in self.proposals:
                        proposal = self.proposals.pop(self.slot_out, None)
                        if decision.command != proposal.command:
                            self.requests.append(proposal.command)
                    # print (decision)
                    self.perform(decision.command)
            self.recv_cv.release()
            self.propose()