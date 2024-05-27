# ETL Pipeline Project

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
├── member-data.csv
├── requirements.txt
├── test_main.py
└── README.md

Build the Docker image:
docker-compose build


Run the ETL pipeline:
docker-compose up


Notes
Ensure that the member-data.csv file is present in the project directory.
The MongoDB URI is configured using an environment variable (MONGO_URI) in the docker-compose.yml file.