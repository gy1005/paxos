import socket
from threading import Thread, Lock, Condition
from message import *
from utils import *
import pickle

class Accepter(Thread):
    def __init__(self, id):
        Thread.__init__(self)
        self.id = id
        self.ballot_num = BallotNum(0, -1)
        self.accepted = {}
        self.recv_queue = []
        self.recv_cv = Condition()

    def run(self):
        while True:
            self.recv_cv.acquire()
            while len(self.recv_queue) == 0:
                self.recv_cv.wait()
            recv_msg = self.recv_queue.pop(0)
            self.recv_cv.release()
            assert recv_msg.type == 'p1a' or recv_msg.type == 'p2a'
            if recv_msg.type == 'p1a':
                if recv_msg.ballot_num > self.ballot_num:
                    self.ballot_num = recv_msg.ballot_num
                p1b_message = P1bMessage(self.id, self.ballot_num, self.accepted, recv_msg.scout_id)
                p1b_message_str = pickle.dumps(p1b_message)
                send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_address = ('localhost', PROCESS_PAXOS_PORT_START + recv_msg.scout_id.leader_id)
                try:
                    send_sock.connect(server_address)
                    send_sock.sendall(p1b_message_str + ' end_of_message ')
                    send_sock.close()
                except socket.error:
                    # TODO: socket error handler
                    pass
            else:
                if recv_msg.pvalue.ballot_num == self.ballot_num:
                    if recv_msg.pvalue.slot not in self.accepted:
                        self.accepted[recv_msg.pvalue.slot] = [recv_msg.pvalue]
                    else:
                        self.accepted[recv_msg.pvalue.slot].append(recv_msg.pvalue)

                p2b_message = P2bMessage(self.id, self.ballot_num, recv_msg.commander_id)
                p2b_message_str = pickle.dumps(p2b_message)
                send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server_address = ('localhost', PROCESS_PAXOS_PORT_START + recv_msg.commander_id.leader_id)
                try:
                    send_sock.connect(server_address)
                    send_sock.sendall(p2b_message_str + ' end_of_message ')
                    send_sock.close()
                except socket.error:
                    # TODO: socket error handler
                    pass