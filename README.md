# README.md

## CONTENTS

</br>

 * __Introduction__
 * __Process Mining Pipeline: Workflow__
 * __Stack & Installation/Configuration Requirements__
 * __Maintainers__
 * __Backlog (Shared Doc Link)__
 * __Troubleshooting__



## <span style="color:red"> INTRODUCTION </span>



- This is a Proof of Concept (PoC) for distributed Process Mining (PM) data pipelines. It relies on the theoretical concepts of: 

</br>

1. graphs:
	- Directly-Follows Graph (DFG)
	- Directed Acyclic Graph (DAG)
2. database:
	- Neo4J graph db (via database .jar connector)
3. data analysis:
	- distributed analytical engine
		- interface: Dask
	- PM4Py Process Mining algorithms (Python-based package)


## PROCESS MINING PIPELINE: WORKFLOW

- log files, i.e., *.csv* files
	- Pass it to Dask Dataframe
		- data partitioning (by case ID) and time windowing (by timestamps)
			- Directly-Follows Graph schema, i.e., *predecessor(s)* and *successor(s)* nodes
				- WRITE queries to a Neo4J graph database
					- format conversion for algorithmic analysis, i.e., Parent/Child nodes and their frequency
						- derive Petri net objects and put them within dataframes
							- PM4Py analysis by applying - in parallel to DFGs - Process Mining algorithms, i.e.,   
								1. α-miner
								2. heuristic miner
								3. inductive miner
									- plotting of evaluation metrics via comparative charts from the dataframes - derived by converting Resilient Distributed Database (RDD)s to Pandas
 

## HOW TO SETUP AND RUN THE ENVIRONMENT LOCALLY

1. First you need to build the docker image on your end and make it up and running
```
docker-compose up -d
```
2. In case you need to open Anaconda in your browser, please find the url that starts with **127.0.0.1:8888** from the logs and click on it to open it in your browser.
you can find the logs through the following command:
```
docker-compose logs anaconda
```
3. Run the python file inside the anaconda service of the docker
```
cat dask-pipeline.py | docker-compose exec -T anaconda python
```
4. This step is bonus if you only want to interpret python files from pyCharm with the python of the docker image, just follow this [url](https://www.jetbrains.com/help/pycharm/using-docker-as-a-remote-interpreter.html#run)

## CONTRIBUTORS/MAINTAINERS


- Prof. Dr. Ahmed Awad	ahmed.awad@ut.ee 
- Belal Mohammad: 	B.Mohammed@nu.edu.eg

## BACKLOG (SHARED DOC LINK)

[Backlog (Shared Doc)](https://docs.google.com/document/d/10l2CjnfpslwH4a8O3JncVBYHZEJd9KoXMQYM8iqlrGI/edit)


</br>



