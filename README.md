# SGCarForecast: Predictive Pricing for Singaporean Vehicles 

## Description

IS3107 Final Project

## Repository Structure

### Airflow Docker

- **Purpose**: Contains the Docker Compose file for setting up the Airflow environment and will store all the Directed Acyclic Graph (DAG) scripts for data processing and orchestration.
- **Contents** (Not updated):
  - `docker-compose.yaml`: Docker Compose file to set up Airflow services.
  - `dags/`: Directory for storing Airflow DAG scripts.
- **Setup**:
    1. Make sure directory is set to AirflowDocker
        ```cd AirflowDocker```
    2. Initialize the environment with the docker-compose.yaml file
        ```bash
        mkdir -p ./dags ./logs ./plugins ./config
        echo -e "AIRFLOW_UID=$(id -u)" > .env
        ```
    3. Start the Airflow Environment (1st time only)
        ```docker compose up airflow-init```
    4. Start all services
        ```docker compose up```
    5. Access Airflow UI at `http://localhost:8080/`

    Default credentials   
    Username: airflow  
    Password: airflow  

### machine_learning

- Contains jupyter notebook  `ml_pipeline` for traning the machine learning pipeline.
- Contains jupyter notebook `ml_prediction` for retrival of the trained model and cleaned dataset from GCS, and using Grid Search to tune the hyperparameters.
- Contains pickle file `modelRF.pkl`. This is the trained model fetched from GCS.
- Contains csv file `dataset.csv`. This is the cleaned dataset fetched from GCS.

