# About 

This repository was made for the "Capstone Project" project by Udacity for their Data Engineer program.

------------------------

# Purpose 

The goal of this project to build an ETL pipleline for the I94 Immigration, U.S. City Demographic and World Temperature data datasets with the result being an analytical database that can be use to extract insight.

------------------------

# Repository Structure

 - capstone_project\data: a dir that contains the data need for the project
 - capstone_project\drivers: a dir for the needed postgresql driver.
 - capstone_project\images: contains the DB Diagram
 - capstone_project\Capstone Project.ipynb: notebook that was used for building the ETL pipeline.
 - capstone_project\etl_functions.py: Python code that is used for cleaing and transforming data in this project.
 - capstone_project\sql_queries.py: Python code that contains all the SQL statements used within this project.
 - capstone_project\utilities.py: Utility Python code that is used in this project.
 - docker-compose.yml: docker-compose file to setup the postgresql database.
 - .env: postgresql database secrets
 - README.md: this file

------------------------

 # Requirements
- Docker
- Python3 with the following packages:
	- Psycopg2
	- dotenv
	- pyspark
- "postgresql-42.2.18.jar" from [here](https://jdbc.postgresql.org/download.html) and place in capstone_project\drivers

------------------------

# How to Run
```
	docker-compose up -d
	jupyter notebook
```

------------------------
# Project Write Up
* The rationale for the choice of tools and technologies for the project.
    * Apache spark:
        * It can handle large files in multiple formats
        * Sparks does analytics fast and efficient even at scale 
        * Easy to use
* Propose how often the data should be updated and why.
    * monthly, because I94 Immigration is partitioned by month.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
     * Increase the number of spark nodes to handle the new demand   
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
     * I would use an ETL scheduler like Apache airflow or Prefect to build pipelines. 
 * The database needed to be accessed by 100+ people.
     * I would move our database to the cloud. More specifically AWS's redshift to to take advantage of its auto-scaling capabilities to handle the new demand.