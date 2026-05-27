PACKAGE = example
CLASS = App

.PHONY: all hadoop clean scala run copyHDFS cleanHDFS

all: scala

scala:
	# Pull the latest changes
	git pull
	# Build the project
	sbt package

run: scala
	# Clean the output from HDFS
	-hadoop fs -rm -r /user/jshin53/output
	# Run the program with spark 
	spark-submit --class $(PACKAGE).$(CLASS) --master yarn ./target/scala-2.11/$(PACKAGE)_2.11-0.1.jar

dataset:
	# Curl the dataset
	curl -o reviews.gz https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Video_Games.jsonl.gz
	curl -o metadata.gz https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Video_Games.jsonl.gz
	# Unzip the files
	gzip -d reviews.gz
	gzip -d metadata.gz

copyHDFS: cleanHDFS
	# Create the directory if it does not already exist
	-hadoop fs -mkdir /user/jshin53/input/final
	# Copy the reviews and the metadata from local to HDFS
	hadoop fs -mkdir /user/jshin53/input/final/reviews/
	hadoop fs -put ./reviews /user/jshin53/input/final/reviews/reviews
	hadoop fs -mkdir /user/jshin53/input/final/metadata/
	hadoop fs -put ./metadata /user/jshin53/input/final/metadata/metadata
	# Remove local files
	rm reviews
	rm metadata 

cleanHDFS:
	# Remove any old HDFS files
	-hadoop fs -rm -r /user/jshin53/input/final
	-hadoop fs -rm -r /user/jshin53/output
clean:
	rm -rf project target
