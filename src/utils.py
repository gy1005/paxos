from threading import Lock


screen_lock = Lock()
PROCESS_PAXOS_PORT_START = 20000
RECV_TIMEOUT = 3

class Decision:
    def __init__(self, slot, command):
        self.slot = slot
        self.command = command


class ScoutID:
    def __init__(self, leader_id, scout_id):
        self.leader_id = leader_id
        self.scout_id = scout_id

class CommanderID:
    def __init__(self, leader_id, commander_id):
        self.leader_id = leader_id
        self.commander_id = commander_id

class Proposal:
    def __init__(self, slot, command):
        self.slot = slot
        self.command = command

class Pvalue:
    def __init__(self, ballot_num, slot, command):
        self.ballot_num = ballot_num
        self.slot = slot
        self.command = command


class BallotNum:
    def __init__(self, round = 0, id = -1):
        self.round = round
        self.id = id
    def __eq__(self, other):
        return self.round == other.round and self.id == other.id
    def __ne__(self, other):
        return self.round != other.round or self.id != other.id
    def __lt__(self, other):
        return (self.round < other.round) or (self.round == other.round and self.id < other.id)
    def __le__(self, other):
        return (self.round < other.round) or (self.round == other.round and self.id <= other.id)
    def __gt__(self, other):
        return (self.round > other.round) or (self.round == other.round and self.id > other.id)
    def __ge__(self, other):
        return (self.round > other.round) or (self.round == other.round and self.id >= other.id)


def pmax(pvalues):
    new_pvalues = {}
    for slot, pvalue_list in pvalues.iteritems():
        ballot_num_max = BallotNum(0, -1)
        for pvalue in pvalue_list:
            if pvalue.ballot_num > ballot_num_max:
                new_pvalues[slot] = pvalue
                ballot_num_max = pvalue.ballot_num
    return new_pvalues
