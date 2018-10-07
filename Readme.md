# Readme

## 1 - Install 

> This realization of this part has already been done by us.

1. Download project from Gitlab to VMs

```bash
git clone https://gitlab.engr.illinois.edu/xinranw5/MachineProgramming.git
```

- The latest version of codes is on master branch


2. Locate projects on /home of each VM, and move files

```bash
// go to /home/MachineProgramming/
cd /home/MachineProgramming/

// change the first line of mp.config to the number of each vm
// make sure that this file is in /home/MachineProgramming/src
sudo vi mp.config  
```


## 2 - Compile & Run

We use gradle in this MP to help us build and run the programs.

1.  Run Detector (MP2)
We define a task named Detector to execute the main program of MP2.

```bash
cd /home/MachineProgramming/
// Run Detector 
// Gradle will compile and run this task automatically
gradle Detector
```

2. Run Logger (MP1)
For MP1, we can also use the same way to run Server and Client

```bash
cd /home/MachineProgramming/
// Run Client 
gradle Client
// Run Server
gradle Server
// make sure that /home/mp1/ contains vm_x.log file
```


## 3 - User Input

The Detector will continuously receive user input, until the input is "quit" or the process is killed.
The program will deal with those input:
1. quit
Detector will quit.
2. join
This node will join into the group(If the introducer is active).
3. leave
This node will leave the group.
4. show
Show the id of this node, its membership list and all the members in this group.

