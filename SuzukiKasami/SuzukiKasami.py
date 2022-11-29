import random
import sys
import threading
from collections import deque
from datetime import datetime
from threading import Thread
from time import sleep

from mpi4py import MPI

# Here using MPI to basically communicate among the various sites
# The code can be run by mpiexec -n <no.ofsites to execute> python SuzukuKasami.py

# - Globally declaring few variables needed for getting the current executing site or MPI rank
# - N - the total no. of sites running here
# - all the lock variables to be used by various processes in MPI, while accessing and using the
#   shared global variables
comm = MPI.COMM_WORLD
tid = comm.Get_rank()
N = comm.Get_size()

cs_lock, token_lock, rn_lock, release_lock, request_lock, send_lock = threading.Lock(), threading.Lock(), threading.Lock(), threading.Lock(), threading.Lock(), threading.Lock()

# Queue , RN, LN variables required for the SuzukiKasami algorithm
# and other variables to keep hold of flag indicating which site has the token or not
Q = deque()
has_token, in_cs, waiting_for_token = 0, 0, 0

RN, LN = [], []

# initially initializing the algorithm
for i in range(0, N): LN.append(0)
for i in range(0, N): RN.append(0)    

# giving a token to start the process 0
if tid == 0:
    print("%s: I'm %d and have a startup token." % (datetime.now().strftime('%M:%S'), tid))
    sys.stdout.flush()
    has_token = 1
RN[0] = 1

# Helps to receive the request for the given current site
# It gets input from any source site, in recive mode
# updates the RN value , as max(existing RN, received sn'th execution value)
# if the current site , has the token , is not executing the critical section currently, and 
# the site sends the token to the requesting site for critical section execution
def receive_request():
    global LN
    global RN
    global Q
    global in_cs
    global waiting_for_token
    global has_token
    while True:
        message = comm.recv(source=MPI.ANY_SOURCE)
        if message[0] == 'RN':
            with rn_lock:
                requester_id = message[1]
                cs_value = message[2]
                RN[requester_id] = max([cs_value, RN[requester_id]])
                if cs_value < RN[requester_id]:
                    print(
                        "%s: Request from %d expired." % (datetime.now().strftime('%M:%S'), requester_id))
                    sys.stdout.flush()

                if (has_token == 1) and (in_cs == 0) and (RN[requester_id] == (LN[requester_id] + 1)):
                    has_token = 0
                    send_token(requester_id)

        elif message[0] == 'token':
            with token_lock:
                print("%s: I'm %d and I got a token." % (datetime.now().strftime('%M:%S'), tid))
                sys.stdout.flush()
                has_token = 1
                waiting_for_token = 0
                LN = message[1]
                Q = message[2]
                critical_section()


# Helps to send the request for the current site to execute the critical section
# except the current site, to all other site in mpi, the request message is sent
def send_request(message):
    for i in range(N):
        if tid != i:
            to_send = ['RN', tid, message]
            comm.send(to_send, dest=i)


# Helps to send the token to the given receipient
def send_token(recipent):
    global Q
    with send_lock:
        print("%s: I'm %d and sending the token to %d." % (datetime.now().strftime('%M:%S'), tid, recipent))
        sys.stdout.flush()
        global in_cs
        to_send = ['token', LN, Q]
        comm.send(to_send, dest=recipent)


# Helps to request token to get into the critical section
# Everytime while requesting the token to execute the CS, RN[i] value would be 
# incremented and send_request would be sent
def request_cs():
    global RN
    global in_cs
    global waiting_for_token
    global has_token
    with request_lock:
        if has_token == 0:
            RN[tid] += 1
            print("%s: I'm %d and want a token for the %d time." % (datetime.now().strftime('%M:%S'), tid, RN[tid]))
            sys.stdout.flush()
            waiting_for_token = 1
            send_request(RN[tid])


# Helps to release the critical section
# While releasing helps to check whether the other elements which are requesting in the queue 
# are there, if so, the top would be popped out, the the token would be sent to it
def release_cs():
    global in_cs
    global LN
    global RN
    global Q
    global has_token
    with release_lock:
        LN[tid] = RN[tid]
        for k in range(N):
            if k not in Q:
                if RN[k] == (LN[k] + 1):
                    Q.append(k)
                    print("%s: I'm %d and it adds %d to the queue. Queue after adding:%s." % (
                        datetime.now().strftime('%M:%S'), tid, k, str(Q)))
                    sys.stdout.flush()
        if len(Q) != 0:
            has_token = 0
            send_token(Q.popleft())


# Helps to execute the critical section
# After executing , the critical section is released
def critical_section():
    global in_cs
    global has_token
    with cs_lock:
        if has_token == 1:
            in_cs = 1
            print("%s: I'm %d and doing %d CS." % (datetime.now().strftime('%M:%S'), tid, RN[tid]))
            sys.stdout.flush()
            sleep(random.uniform(2, 5))
            print("%s: I'm %d and finished %d CS." % (datetime.now().strftime('%M:%S'), tid, RN[tid]))
            sys.stdout.flush()
            in_cs = 0
            release_cs()


try:
    thread_receiver = Thread(target=receive_request)
    thread_receiver.start()
except:
    print("Error: unable to start thread!   ")
    sys.stdout.flush()

while True:
    if has_token == 0:
        sleep(random.uniform(1, 3))
        request_cs()
    elif in_cs == 0:
        critical_section()
    while waiting_for_token:
        sleep(0.5)

