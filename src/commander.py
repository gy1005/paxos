import socket
from threading import Thread, Lock, Condition
from utils import *
from message import *
import pickle



class Commander(Thread):
    def __init__(self, leader, id, num_server, pvalue):
        Thread.__init__(self)
        self.leader = leader
        self.num_server = num_server
        self.pvalue = pvalue
        self.waitfor = list(xrange(num_server))
        self.id = id
        self.recv_queue = []
        self.recv_cv = Condition()

    def run(self):
        p2a_message = P2aMessage(self.id, self.pvalue)
        p2a_message_str = pickle.dumps(p2a_message)
        for i in range(self.num_server):
            send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address = ('localhost', PROCESS_PAXOS_PORT_START + i)
            try:
                send_sock.connect(server_address)
                send_sock.sendall(p2a_message_str + ' end_of_message ')
                send_sock.close()
            except socket.error:
                # TODO: socket error handler
                pass
        while True:
            self.recv_cv.acquire()
            while len(self.recv_queue) == 0:
                self.recv_cv.wait(RECV_TIMEOUT)
                # for i in self.waitfor:
                #     send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                #     server_address = ('localhost', PROCESS_PAXOS_PORT_START + i)
                #     p2a_message = P2aMessage(self.id, self.pvalue)
                #     p2a_message_str = pickle.dumps(p2a_message)
                #     try:
                #         send_sock.connect(server_address)
                #         send_sock.sendall(p2a_message_str + ' end_of_message ')
                #         # send_sock.close()
                #     except socket.error:
                #         # TODO: socket error handler
                #         pass
            recv_msg = self.recv_queue.pop(0)
            self.recv_cv.release()
            assert recv_msg.type == 'p2b'
            if recv_msg.ballot_num == self.pvalue.ballot_num:
                self.waitfor.remove(recv_msg.accepter_id)
                if len(self.waitfor) < self.num_server / 2.0:
                    decision_message = DecisionMessage(self.pvalue.slot, self.pvalue.command)
                    decision_message_str = pickle.dumps(decision_message)
                    for i in range(self.num_server):
                        send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        server_address = ('localhost', PROCESS_PAXOS_PORT_START + i)
                        try:
                            send_sock.connect(server_address)
                            send_sock.sendall(decision_message_str + ' end_of_message ')
                            send_sock.close()
                        except socket.error:
                            # TODO: socket error handler
                            pass
                    # self.leader.thread_lock.acquire()
                    # self.leader.commanders.pop(self.id.commander_id)
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
                # self.leader.commanders.pop(self.id.commander_id)
                # self.leader.thread_lock.release()
                exit(0)
