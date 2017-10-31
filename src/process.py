#!/usr/bin/env python

import signal
import sys
import socket
from threading import Thread, Lock
from accepter import Accepter
from leader import Leader
from replica import Replica
from utils import *
import pickle
from message import *



class Process(Thread):
    def __init__(self, process_id, num_server, port):
        Thread.__init__(self)
        self.process_id = process_id
        self.num_server = num_server
        self.port = port
        self.chat_log = {}
        self.chat_log_lock = Lock()
        self.master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.paxos_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.accepter = Accepter(self.process_id)
        self.leader = Leader(self, self.process_id, self.num_server)
        self.replica = Replica(self, self.process_id, self.leader, 0)
        self.master_conn = None
        self.msg_wait_for_resp = {}
        self.thread_lock = Lock()

    # def master_conn_listener(self):
    #     pass

    def paxos_conn_listener(self):
        self.paxos_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.paxos_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.paxos_socket.bind(('localhost', PROCESS_PAXOS_PORT_START + self.process_id))
        self.paxos_socket.listen(5)

        while True:

            conn, addr = self.paxos_socket.accept()
            paxos_handler = Thread(target=self.paxos_recv_handler, args=(conn,))
            paxos_handler.start()

    def chash(self):
        exit(0)

    def paxos_recv_handler(self, conn):
        request_msg = ''
        while True:
            buf = conn.recv(4096)
            if buf == '':
                # conn.close()
                break
            else:
                request_msg += buf


        requests = request_msg.split(' end_of_message ')

        # print requests
        for request_str in requests:
            if request_str != '':
                # screen_lock.acquire()
                # print request_str
                # screen_lock.release()
                request = pickle.loads(request_str)
                # screen_lock.acquire()
                # print request
                # screen_lock.release()
                assert request.type == 'p1a' or request.type == 'p1b' or request.type == 'p2a' \
                       or request.type == 'p2b' or request.type == 'propose' or request.type == 'decision'
                if request.type == 'p1a' or request.type == 'p2a':
                    self.accepter.recv_cv.acquire()
                    self.accepter.recv_queue.append(request)
                    if len(self.accepter.recv_queue) == 1:
                        self.accepter.recv_cv.notify()
                    self.accepter.recv_cv.release()
                elif request.type == 'p1b':
                    self.leader.scouts[request.dest.scout_id].recv_cv.acquire()
                    self.leader.scouts[request.dest.scout_id].recv_queue.append(request)
                    if len(self.leader.scouts[request.dest.scout_id].recv_queue) == 1:
                        self.leader.scouts[request.dest.scout_id].recv_cv.notify()
                    self.leader.scouts[request.dest.scout_id].recv_cv.release()
                elif request.type == 'p2b':
                    self.leader.commanders[request.dest.commander_id].recv_cv.acquire()
                    self.leader.commanders[request.dest.commander_id].recv_queue.append(request)
                    if len(self.leader.commanders[request.dest.commander_id].recv_queue) == 1:
                        self.leader.commanders[request.dest.commander_id].recv_cv.notify()
                    self.leader.commanders[request.dest.commander_id].recv_cv.release()
                elif  request.type == 'propose':
                    self.leader.recv_cv.acquire()
                    self.leader.recv_queue.append(request)
                    if len(self.leader.recv_queue) == 1:
                        self.leader.recv_cv.notify()
                    self.leader.recv_cv.release()
                else:
                    # print "decision"
                    self.replica.recv_cv.acquire()
                    self.replica.recv_queue.append(request)
                    if len(self.replica.recv_queue) == 1:
                        self.replica.recv_cv.notify()
                    self.replica.recv_cv.release()

    def server_recv_handler(self):
        while True:
            buf = self.master_conn.recv(1024)
            if buf == '':
                break
            requests = buf.split('\n')
            for request in requests:
                if request[0:3] == 'msg':
                    request_contents = request.split(' ')
                    self.thread_lock.acquire()
                    self.msg_wait_for_resp[int(request_contents[1])] = request
                    self.thread_lock.release()
                    request_message = RequestMessage(request)
                    self.replica.recv_cv.acquire()
                    self.replica.recv_queue.append(request_message)
                    if len(self.replica.recv_queue) == 1:
                        self.replica.recv_cv.notify()
                    self.replica.recv_cv.release()
                elif request == 'get chatLog':
                    send_msg = ''
                    self.chat_log_lock.acquire()
                    for i in range(len(self.chat_log)):
                        if i == 0:
                            send_msg += self.chat_log[i].data
                        else:
                            send_msg += ',' + self.chat_log[i].data
                    self.chat_log_lock.release()
                    self.master_conn.sendall(send_msg)
                elif request == 'crash':
                    self.chash()



    def run(self):
        self.accepter.start()
        self.leader.start()
        self.replica.start()

        paxos_listener = Thread(target=self.paxos_conn_listener)
        paxos_listener.start()

        self.master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.master_socket.bind(('localhost', self.port))
        self.master_socket.listen(5)

        while True:
            self.master_conn, addr = self.master_socket.accept()
            client_handler = Thread(target=self.server_recv_handler)
            client_handler.start()

if __name__ == '__main__':
    assert (len(sys.argv) > 3)
    process_id = int(sys.argv[1])
    num_server = int(sys.argv[2])
    port = int(sys.argv[3])

    process = Process(process_id, num_server, port)
    process.start()