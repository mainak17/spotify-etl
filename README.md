# Spotify Data Pipeline

This project is a data pipeline that extracts information about the songs played on Spotify by a user, transforms the data, and loads it into a PostgreSQL database. The pipeline runs daily and stores the data in tables with a naming convention of "spotify_tracks_yyyymmdd", where "yyyymmdd" represents the date the data was extracted.

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
- **ORM**: SQLAlchemy

## Getting Started
To get started with this project, you will need to:

- Clone the repository
- Install the dependencies
- Set up a PostgreSQL database and update the database credentials in the **config.ini** file
- Update the **USER_ID** and **TOKEN** in the **config.ini** file with your Spotify API user ID and token
- Run the main.py script

## Future Work

- Add a dashboard to visualize the data
- Schedule the pipeline to run automatically using a tool like Airflow or Cron
- Add support for other data sources, such as Apple Music or YouTube Music
- Use a cloud-based database service like Amazon RDS or Google Cloud SQL for easier scalability and management.
