HubSpot to PostgreSQL ETL Pipeline
📄 Project Overview
This project serves as a demonstration of my ability to design and build end-to-end data pipelines. The goal is to extract data from a HubSpot CRM, perform basic transformations, and load it into a PostgreSQL database, making it ready for use in a data warehouse or business intelligence tools.

🎯 Problem Statement and Goal
The Problem: Valuable data residing in the HubSpot CRM was unstructured and not readily available for immediate analysis.

The Goal: To create an automated and reliable pipeline to migrate data from HubSpot to a PostgreSQL database, ensuring the data is clean, organized, and easily accessible for analytical purposes or reporting.

🏛️ Project Architecture
The process is designed as an ETL (Extract, Transform, Load) pipeline, managed entirely by Apache Airflow.

Extraction: A Python task within the DAG calls the HubSpot API to pull Product and Contact data.

Transformation: The extracted data is cleaned and columns are renamed in Python to be more descriptive and readable. For example, internal API names (e.g., hs_object_id) are changed to standard names (e.g., id).

Loading: The transformed data is loaded into specific tables within a PostgreSQL database.

Orchestration: All of these steps are managed as a single DAG within Apache Airflow to ensure they execute correctly and in a defined sequence.

🛠️ Technologies and Tools Used
Python: To write the ETL logic.

requests for API connectivity.

psycopg2 for PostgreSQL interaction.

Apache Airflow: To orchestrate all pipeline tasks.

Astronomer CLI: To easily run and manage a local Airflow environment with Docker.

Docker Desktop: To provide a containerized environment for running Airflow and PostgreSQL.

PostgreSQL: To host the destination database.

HubSpot API: As the data source.

🔄 Transformation Details
To ensure data clarity and usability, columns were renamed. For example, for the Contacts table, the following transformations were applied:

# Example of contacts data transformation
transformed_contacts = [
    {
        "id": c['id'],
        "firstname": c['properties'].get('firstname'),
        "lastname": c['properties'].get('lastname'),
        "email": c['properties'].get('email'),
        "phone": c['properties'].get('phone'),
        "createdate": c['properties'].get('createdate'),
        "lastmodifieddate": c['properties'].get('lastmodifieddate')
    }
    for c in contacts_data
]

🚀 How to Run the Project
Prerequisites:

Docker Desktop

Astronomer CLI

Python 3.x

Setup and Execution Steps:

Clone the repository:

git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name

Start the local Airflow environment:

astro dev start

Note: This command will create the necessary Docker containers for Airflow and PostgreSQL. You can access the Airflow UI at http://localhost:8080.

Set up Connections:

Once Airflow is running, create new connections in the UI for both the HubSpot API and PostgreSQL.

Run the DAG:

From the Airflow UI, find the project's DAG and trigger it manually.

(Note: The DAG's schedule is paused to avoid excessive resource consumption, and should be triggered manually as needed).

📈 Results and Achievements
Successfully built a complete, functional ETL data pipeline.

Demonstrated the ability to extract data from an external API and orchestrate tasks using Airflow.

Structured and organized data to be ready for analysis.

Proven capability to use Docker for creating reliable, isolated working environments.

💡 Future Improvements
Adding data quality checks.

Automating the pipeline schedule.

Extending the project to include additional data sources.

🤝 Contact Information
[https://www.linkedin.com/in/abobakar-suliman-b86049221/]
