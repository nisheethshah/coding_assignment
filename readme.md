# Coding Assignment

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline that reads member data from a CSV file, transforms it, and loads it into a MongoDB database. The project is containerized using Docker, with a Dockerfile and docker-compose.yml provided for easy setup and execution.

## Prerequisites
- Docker
- Docker Compose

## Project Structure
.
├── Dockerfile
├── docker-compose.yml
├── main.py
├── test_main.py
├── member-data.csv
├── requirements.txt
└── README.md

Tests test_main.py in IDE terminal:
python -m unittest test_main.py


Dockerfile
Defines the Docker image for the ETL pipeline:

Sets up the Python environment.
Installs required packages.
Copies the application code.
Runs the ETL script.
docker-compose.yml
Defines the services required for the ETL pipeline:

mongo: The MongoDB service.
etl: The ETL service that runs the Python script.

Build the Docker Image:
docker build -t my-python-app .

Save the Docker Image to a tar file:
docker save -o my-python-app.tar my-python-app


Notes
Ensure that the member-data.csv file is present in the project directory.
The MongoDB URI is configured using an environment variable (MONGO_URI) in the docker-compose.yml file.