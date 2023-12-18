# Traffic Data Warehouse Project

## Overview

This project involves creating a scalable data warehouse for a startup that collects data from various sensors deployed in businesses. The objective is to organize and manage the data efficiently for downstream projects, particularly focusing on analyzing vehicle trajectory data collected by swarm drones for a city's traffic department.

## Business Need

The startup aims to assist organizations in acquiring critical intelligence based on both public and private data they collect and organize. Specifically, the project involves creating a scalable data warehouse to host vehicle trajectory data collected by swarm drones and static roadside cameras for the city's traffic department.

## Data

The project utilizes the pNEUMA dataset, a large-scale collection of naturalistic trajectories of half a million vehicles collected in Athens, Greece. The data is obtained from swarm drones and contains detailed vehicle trajectory information. Additionally, references and GitHub packages are provided for data visualization and interaction.

## Getting Started

1. Clone this repository.
2. Install Docker and Docker Compose.
3. Run `docker-compose up` to initiate the tech stack.
4. Follow individual task instructions provided in respective directories.

## Directory Structure

- `/airflow`: Contains Airflow DAGs and configurations.
- `/dbt`: Holds dbt transformations and documentation.
- `/redash`: Includes configurations for redash reporting.

## Resources

- [pNEUMA Dataset](https://open-traffic.epfl.ch/index.php/downloads/#1599047632394-7ca81bff-5221)
