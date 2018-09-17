# Readme

## 1 - Install 

> This realization of this part has already been done by us.

1. Download project from Gitlab to VMs

```bash
git clone https://gitlab.engr.illinois.edu/xinranw5/MachineProgramming.git
```

2. Locate projects on /home of each VM, and move files

```bash
// go to /home/MachineProgramming/
cd /home/MachineProgramming/

// change the first line of mp.config to the number of each vm
// make sure that this file is in /home/MachineProgramming/src
sudo vi mp.config  

// make sure that /home/mp1/ contains vm_x.log file
```

2. Compile all the ".java" file into executable files

```bash
// go to /home/MachineProgramming/src/
cd /home/MachineProgramming/src/

//complie
sudo javac -cp ../junit-4.10.jar:../grep4j-with-dependencies.jar:../hamcrest-core-1.3.jar team/cs425/g54/*.java
```



## 2 - Run

1.  Run Server/Client on different VMs

```bash
cd /home/MachineProgramming/src/
// Run Server
sudo java -cp .:../junit-4.10.jar:../grep4j-with-dependencies.jar:../hamcrest-core-1.3.jar team.cs425.g54.Server

// Run Client
sudo java -cp .:../junit-4.10.jar:../grep4j-with-dependencies.jar:../hamcrest-core-1.3.jar team.cs425.g54.Client
```



## 3 - Result

1. All results are sent back to VM running client, and results from different vm would  be output into corresponding "vm_x.txt", which would be located in *"/home/MachineProgramming/src/"*.
2. The "vm_x.txt" file would be override everytime the client starts a new command that accesses the txt file.



## 4 - Evaluation

1. In the output .txt file, the format would be like the following part:

```
vm 1  // or vm 2,3,4...10
line_number match pattern line
line_number match pattern line
line_number match pattern line
		.
		.
		.
Total lines: xxx   // if client cannot fetch the whole file, the number of total lines would be 0
```

## 5 - Branch

- The code version that includes  unit test is on master branch
- The code version that includes main function only is on develop branch



## 6 - File Description

