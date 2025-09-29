# Ga7-MP2
Aditi Sadwelkar, Ary Ramchandran
## Running the system
1) All VMs to query must be running membership.py\
``python membership.py --vm-id <id>``

Example:\
``python membership.py --vm-id 7``

2) Controlling machine should run command_functions.py\
``python command_functions.py``
From the controller machine, the user can then target specific VMs and run commands (join, leave, list_mem, display_protocol, display_suspects, switch gossip/ping suspect/nosuspect, drop_rate <value>)

## Citations
Skeleton code adapted from Beej's Guide to Network Programming and Concepts: 
https://beej.us/guide/
