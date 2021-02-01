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