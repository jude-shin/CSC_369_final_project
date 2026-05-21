# CSC_369_final_project

## project ideas:
- We can try and analyze the frequency of particular words depending on the
    time frame that the book was written in.

## Running on CSL Servers
Log into the ambari servers, then execute the following code:

    make run

Running "make run" will pull the latest changes from github, then build the 
scala binaries, and run the code on the HDFS.
TODO: run throuh HDFS, right now, the scala code is run on one node

### Changing the username in Makefile
You will need to change the USERNAME in the Makefile before you run the code on 
the servers. It might be useful to make the change, and then stash that change 
popping it every time you need to run the code on the csl.
