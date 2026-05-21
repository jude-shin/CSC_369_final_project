PACKAGE = example
CLASS = App

.PHONY: all hadoop clean scala run

all: scala

scala:
	git pull
	sbt package

run: scala
	spark-submit --class $(PACKAGE).$(CLASS) --master yarn ./target/scala-2.11/$(PACKAGE)_2.11-0.1.jar /user/jshin53/input /user/jshin53/output

clean:
	rm -rf project target
