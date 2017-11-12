class Message:
    def __init__(self, seq_id, data):
        self.seq_id = seq_id
        self.data = data

class ProposeMessage:
    def __init__(self, slot, c):
        self.type = "propose"
        self.slot = slot
        self.command = c

class ResponseMessage:
    def __init__(self, cid, result):
        self.type = "response"
        self.cid = cid
        self.result = result

class RequestMessage:
    def __init__(self, command):
        self.type = "request"
        self.command = command

class DecisionMessage:
    def __init__(self, slot, command):
        self.type = "decision"
        self.slot = slot
        self.command = command


class P1aMessage:
    def __init__(self, scout_id, ballot_num):
        self.type = "p1a"
        self.scout_id = scout_id
        self.ballot_num = ballot_num


class P1bMessage:
    def __init__(self, accepter_id, ballot_num, accepted, dest):
        self.type = "p1b"
        self.accepter_id = accepter_id
        self.ballot_num = ballot_num
        self.accepted = accepted
        self.dest = dest

class P2aMessage:
    def __init__(self, commander_id, pvalue):
        self.type = "p2a"
        self.commander_id = commander_id
        self.pvalue = pvalue

class P2bMessage:
    def __init__(self, accepter_id, ballot_num, dest):
        self.type = "p2b"
        self.accepter_id = accepter_id
        self.ballot_num = ballot_num
        self.dest = dest

class AdoptedMessage:
    def __init__(self, ballot_num, pvalues):
        self.type = 'adopted'
        self.ballot_num = ballot_num
        self.pvalues = pvalues

class PreemptedMessage:
    def __init__(self, ballot_num):
        self.type = 'preempted'
        self.ballot_num = ballot_num

class CrashMessage:
    def __init__(self, type, set):
        self.type = type
        self.set = set
