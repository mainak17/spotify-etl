# Spotify Data Pipeline

This project is a data pipeline that extracts information about the songs played on Spotify by a user, transforms the data, and loads it into a PostgreSQL database. The pipeline runs daily and stores the data locally in a table named **spotify_tracks_master**

## Architecture
The Spotify Data Pipeline consists of four main components:

**Spotify API**: The Spotify API is used to extract data about the songs played by a user. The API returns data in JSON format.

**Data Validation**: The data extracted from the Spotify API is validated to ensure that it is complete and correct. The validation checks include checking for duplicate data, null values, and timestamps from the correct day.

**PostgreSQL Database**: The data is stored in a PostgreSQL database. The database schema includes a table for each day's data.

**Python Script**: A Python script is used to run the pipeline. The script calls the Spotify API, validates the data, and loads it into the PostgreSQL database.

## Tech Stack
The tech stack for this project includes:
- **Programming language**: Python
- **Libraries**: Pandas, Requests, psycopg2
- **Database**: PostgreSQL

## Getting Started
To get started with this project, you will need to:

- Clone the repository
- Install the dependencies
- Set up a PostgreSQL database and update the database credentials in the **config.ini** file
- Update the **USER_ID** and **TOKEN** in the **config.ini** file with your Spotify API user ID and token
- Run the **main.py** script
## Observation over 7 Days
![image](https://user-images.githubusercontent.com/26038097/226666696-bb7e1b8f-a104-400d-8683-62c956fca975.png)
**PS** : Please dont judge my music taste🤣
## Future Work

- [Done] Add a dashboard to visualize the data
- [Done]Schedule the pipeline to run automatically using a tool like Airflow
- Use a cloud-based database service for easier management.
