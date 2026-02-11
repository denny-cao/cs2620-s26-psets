# Lab notebook
Consider using this file to track the results of your performance experiments.
For each important commit, including the handout, record (1) the commit hash,
(2) a description of the commit (e.g., any changes you made), (3) the
environment you ran the code, and (4) a snapshot of overall RPC performance,
plus any thoughts.

c640b74: initial
sent 100000 RPCs in 13.376013339 sec
sent 7476 RPCs per sec

4930d31: async window
sent 100000 RPCs in 5.106272836 sec
sent 19584 RPCs per sec

f66a651: remove compression and reduce string copies. similar performance as before.
sent 100000 RPCs in 5.292199336 sec
sent 18896 RPCs per sec

637f3a7: switch to rpclib. large speed up! more efficient library implementation?
sent 100000 RPCs in 0.862952834 sec
sent 115881 RPCs per sec

e21ed7a: implement batching as suggested from ed. largest speed up! most likely due to overhead per request. 
sent 100000 RPCs in 0.097522833 sec
sent 1025401 RPCs per sec
