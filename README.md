# staging-pipeline-airflow

# SAP HANA to PostgreSQL ETL with Airflow

## Overview
This project automates data extraction from SAP HANA, transformation, and loading into PostgreSQL using Apache Airflow. The DAG ensures data is processed efficiently and scheduled as needed.

## DAG Workflow
1. **Extract:** Fetches data from SAP HANA.
2. **Transform:** Cleans and processes the extracted data.
3. **Load:** Inserts the transformed data into PostgreSQL.
4. **Monitoring:** Tracks DAG execution status and logs errors.

## Airflow Tasks
- **extract_task:** Queries SAP HANA and retrieves data.
- **transform_task:** Applies cleaning and transformation rules.
- **load_task:** Loads the final data into PostgreSQL.
- **notify_task:** Sends notifications on DAG completion or failure.

## Configuration
- Update SAP HANA and PostgreSQL connection settings in `config.py`.
- Adjust scheduling parameters in `dag.py` if needed.

## Dependencies
- Apache Airflow (Taskflow API)
- pandas
- SQLAlchemy
- psycopg2

## License
This project is licensed under the MIT License.
