PACKAGE = example
CLASS = App

.PHONY: all hadoop clean scala run copyHDFS cleanHDFS

all: scala

scala:
	git pull
	sbt package

run: scala
	spark-submit --class $(PACKAGE).$(CLASS) --master yarn ./target/scala-2.11/$(PACKAGE)_2.11-0.1.jar

dataset:
	# get the dataset
	curl -o reviews.gz https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Video_Games.jsonl.gz
	curl -o metadata.gz https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Video_Games.jsonl.gz
	# Unzip the files
	gzip -d reviews.gz
	gzip -d metadata.gz
	# Remove the unzipped files
	rm reviews.gz
	rm metadata.gz


copyHDFS:
	
	-hadoop fs -rm -r /user/jshin53/input/final
	hadoop fs -mkdir /user/jshin53/input/final
	
	hadoop fs -put ./local_inputs/q1_integers /user/jshin53/input/asgn05/q1_integers/q1_integers

cleanHDFS:
	hadoop fs -rm -r /user/jshin53/input/final


clean:
	rm -rf project target
