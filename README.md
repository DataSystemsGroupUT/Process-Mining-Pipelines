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
	- distributed analytical engine, i.e., Apache Spark
		- interface: PySpark
	- PM4Py Process Mining algorithms (Python-based package)


## PROCESS MINING PIPELINE: WORKFLOW

- log files, i.e., *.csv* files
	- Apache Spark via PySpark interface
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
 

## STACK & INSTALLATION/CONFIGURATION REQUIREMENTS

### System config:

- pySpark vs. 2.4.6
- Java JVM vs. 8
- Scala vs. 2.13

### YAML file for setting Conda and Jupyter notebook environment:

<pre>

name: pyspark-vs2
channels:
  - defaults
dependencies:
  - pip=20.2.4
  - python=3.7.9
  - pip:
    - matplotlib==3.3.4
    - numpy==1.20.0
    - pandas==1.2.1
    - pm4py==2.2.5
    - py4j==0.10.9.1
    - pydotplus==2.0.2
    - pyspark==2.4.7
    - python-graphviz==0.8.4

</pre>

</br>

#### NOTE: if some packages are missing in the required version, try installing from `pip` or from Conda-forge: `$ conda install -c conda-forge [pkg_name]`


## CONTRIBUTORS/MAINTAINERS


- Prof. Dr. Ahmed Awad	ahmed.awad@ut.ee 
- Belal Mohammad: 	B.Mohammed@nu.edu.eg
- Fabiano Spiga:	fabiano.spiga@ut.ee

## BACKLOG (SHARED DOC LINK)

[Backlog (Shared Doc)](https://docs.google.com/document/d/10l2CjnfpslwH4a8O3JncVBYHZEJd9KoXMQYM8iqlrGI/edit)


</br>



