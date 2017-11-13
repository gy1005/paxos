import socket
from threading import Thread, Lock, Condition
from message import *
from utils import *
import pickle


class Scout(Thread):
    def __init__(self, leader, id, num_server, ballot_num):
        Thread.__init__(self)
        self.id = id
        self.leader = leader
        self.num_server = num_server
        self.ballot_num = ballot_num
        self.waitfor = list(xrange(num_server))
        self.pvalues = {}
        self.recv_queue= []
        self.recv_cv = Condition()
        self.crash_p1a = False
        self.crash_set = []




    def run(self):
        p1a_message = P1aMessage(self.id, self.ballot_num)
        p1a_message_str = pickle.dumps(p1a_message)
        for i in range(self.num_server):
            if self.crash_p1a:
                if len(self.crash_set) == 0:
                    self.leader.process.crash()
            send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address = ('localhost', PROCESS_PAXOS_PORT_START + i)
            try:
                send_sock.connect(server_address)
                send_sock.sendall(p1a_message_str + ' end_of_message ')
                send_sock.close()
            except socket.error:
                # TODO: socket error handler
                pass
            if self.crash_p1a:
                if i in self.crash_set:
                    self.crash_set.remove(str(i))

        while True:
            self.recv_cv.acquire()
            while len(self.recv_queue) == 0:
                self.recv_cv.wait(RECV_TIMEOUT)
                if len(self.recv_queue) == 0:
                    for i in self.waitfor:
                        send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        server_address = ('localhost', PROCESS_PAXOS_PORT_START + i)
                        p1a_message = P1aMessage(self.id, self.ballot_num)
                        p1a_message_str = pickle.dumps(p1a_message)
                        try:
                            send_sock.connect(server_address)
                            send_sock.sendall(p1a_message_str + ' end_of_message ')
                            send_sock.close()
                        except socket.error:
                            # TODO: socket error handler
                            pass
            recv_msg = self.recv_queue.pop(0)
            self.recv_cv.release()

            assert recv_msg.type == 'p1b' or recv_msg.type == 'crashP1a'
            if recv_msg.type == 'p1b':
                if recv_msg.ballot_num == self.ballot_num:
                    for slot, accepted_pvalues in recv_msg.accepted.iteritems():
                        for accepted_pvalue in accepted_pvalues:
                            if accepted_pvalue.slot not in self.pvalues:
                                self.pvalues[accepted_pvalue.slot] = [accepted_pvalue]
                            else:
                                self.pvalues[accepted_pvalue.slot].append(accepted_pvalue)
                    self.waitfor.remove(recv_msg.acceptor_id)
                    if len(self.waitfor) < self.num_server / 2.0:
                        adopted_message = AdoptedMessage(self.ballot_num, self.pvalues)
                        self.leader.recv_cv.acquire()
                        self.leader.recv_queue.append(adopted_message)
                        if len(self.leader.recv_queue) == 1:
                            self.leader.recv_cv.notify()
                        self.leader.recv_cv.release()
                        # self.leader.thread_lock.acquire()
                        # self.leader.scouts.pop(self.id.scout_id)
                        # self.leader.thread_lock.release()
                        exit(0)
                else:
                    preempted_message = PreemptedMessage(recv_msg.ballot_num)
                    self.leader.recv_cv.acquire()
                    self.leader.recv_queue.append(preempted_message)
                    if len(self.leader.recv_queue) == 1:
                        self.leader.recv_cv.notify()
                    self.leader.recv_cv.release()
                    # self.leader.thread_lock.acquire()
                    # self.leader.scouts.pop(self.id.scout_id)
                    # self.leader.thread_lock.release()
                    exit(0)
            if recv_msg.type == 'crashP1a':
                self.crash_p1a = True
                self.crash_set = recv_msg.set



