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

1.  Run MP4 (& MP2 & MP3)
    We use the same task name to run the main program of MP3

```bash
cd /home/MachineProgramming/
// Run Detector 
// Gradle will compile and run this task automatically
gradle Detector
```

2.  Run Logger (MP1)
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
5. master
   Set the current machine as master
6. store
   See what files are stored in this machine
7. put localFile sdfsFile
   Put files into SDFS
8. get sdfsFile localFile
   Read a file from SDFS
9. delete sdfsFile
   Delete a file from SDFS
10. get_version sdfsFile num_version localFile
   Get k versions of a file into localFile			
11. ls sdfsFile
   List where a file is stored			
12. msshow
   Show the file list on master			
   13. msversion			
       Show the file versions on master
13. cranemaster
   Set the current machine as crane's master
14. crane filter sdfsFile filterword
   Run filter of crane 
15. crane wordcount sdfsFile
   Run wordcount to count sdfsFile
16. crane join sdfsFile localFilepath
   Run join to join sdfsFile and localFilepath


​				

## 4. Spark Example

The 3 applications for Spark are located in spark_python/ directory.

