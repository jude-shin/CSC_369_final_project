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

## Dataset (2023) - Video_Games

https://amazon-reviews-2023.github.io/

### Curl Reviews and Metadata to Servers

Use the following commands to curl the reviews and the metadata

    curl -o reviews.gz https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Video_Games.jsonl.gz
    curl -o metadata.gz https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Video_Games.jsonl.gz

You can also use the following shell command to decompress the files

    gzip -d reviews.gz
    gzip -d metadata.gz

### Preprocessed Data

IMPORTANT: to save some time on the rating prediction, we can use the data in
processed. These are tuples of the form (user\_id, (rating, average\_rating, 
price, parent\_asin)). Each user now has some sort of "identity" based on their
previous ratings.

## Delegation

### Jude

- Most k similar users (based on average ratings, and average review length)
- KNN-like product rating prediction

### Will

### Grace

- Find top N users with most reviews (from those users find their average 
rating, average # of tokens)
- Find top N users with most helpful reviews

### Corey

## Findings:

When using Spark for the RDD processing, there were two join methods; one was
using the .join() function, and the other was using the .cartesian() with the
.filter() function.

The .join() method took a total of 41.1178 minutes, whereas the .cartesian()
method took only 16.9457 minutes.

The .join() method had two extra map cases, and the json files were long. I
think this is the reason the .join() method was so much slower than the
.cartesian() method.

## Presentation (Slides)

- Describe the data
- Why is it important?
- implement some sort of algorithm (explain the algorithm)
- No need for source code
- Results (figures or pictures of results)
- Main struggles of the project
