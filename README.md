# Automated-Weather-Data-Pipeline-with-PostgreSQL

**Main goal** of this project: Create a project that emulates what a data engineer would do in their job.

**Result**: Extracted weather data from the Open Meteo API, ingested it into an ETL pipeline, and loaded it into a PostgreSQL database. The entire workflow is automated using Prefect and is executed every midnight. 


# Explaning the contents of the hourly_data_etl_pipline folder
- This folder has 4 files, extract.py, transform.py, load.py, flows.py

When I extracted the raw data, the data looked like this: 

| date                      | temperature_2m |
|---------------------------|----------------|
| 2020-01-01 00:00:00+00:00 | -28.486500     |

As you can see, the date and time are combined into a single column, and the time is in UTC format. The transform.py file converts this column into two separate columns called time and year, with the timezone converted to Pacific Time. The temperature_2m column is keept the same. 

| temperature_2m |   time   |   year    |
|----------------|----------|-----------|
|     26.1135    | 04:59AM  | 2025-07-03 |

Now the load file simply loads these rows into the PostgreSQL database. Note: Every time the load function is executed by Prefect, many rows are being loaded into the database. 

The flows.py file is used to automate this workflow. It will execute this pipeline every midnight, loading new data from two days before midnight (the start date) to one day before midnight (the end date). The reason I did not set the start date to one day before midnight and the end date to midnight is because the API will not have the weather data for the hours between those dates.


# Below is the workflow diagram
![Workflow Diagram](https://github.com/abhikarthi2529/Automated-Weather-Data-Pipeline-with-PostgreSQL/blob/main/Workflow%20Diagram%20.png?raw=true)

The historical_data_elt_pipeline follows the same workflow, however, it is not automated. It simply extracts 5 years worth of data from 2020 to 2025 and loads it into db.



  


  



