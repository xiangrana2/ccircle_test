# Coffee Circle Test (DE)
Source code for extracting, transforming and loading the data from the provided CSV file into a Data Warehouse (BigQuery), for it's analysis and use in a reporting tool (Data Studio). <br/> <br/> 
This project makes use of Apache Beam and Cloud Dataflow for transforming and loading the data in an efficient way into BigQuery (Data Warehouse).<br/><br/> 
The final DataStudio report that uses the information from the Data Warehouse can be found at: https://datastudio.google.com/open/1Xbo0wXbT1DK2wldrra-ZPwH01XijRsiw

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development:
1. Download the following JSON file into your machine https://storage.cloud.google.com/coffeecircle/CoffeeCircle-244250c8e3de.json
2. Set in your machine the environment variable **GOOGLE_APPLICATION_CREDENTIALS** to your local file path of the JSON downloaded in step 1
3. Install the project dependencies (Python3.6) found in the file **requirements.txt**
4. Execute the Python script **/ccircle_test/run_pipeline.py** in order to load the information from the **data.csv** file into BigQuery 

### Prerequisites

This project was developed using Python 3.6

### And coding style tests

This project follows the Python style guide PEP8


## Built With

* [Google Cloud BigQuery](https://cloud.google.com/bigquery/) - Big Data analytics web service for processing very large read-only data sets. Data warehouse used in this solution.
* [Apache Beam](https://beam.apache.org/) -  Open source unified programming model to define and execute data processing pipelines, including ETL, batch and stream. Used as main dependency for creating the data pipeline and deploying it into Dataflow
* [Cloud Dataflow](https://cloud.google.com/dataflow/) -   Cloud-based data processing service for both batch and real-time data streaming applications. Used to deploy and execute the data pipeline for loading the information into BigQuery
* [Data Studio](https://developers.google.com/datastudio/) - Google's reporting solution used to create the dashboards for this test.


## Authors

* **Christian Granados** - [LinkedIn](https://www.linkedin.com/in/christiangranadose/)<br/> <br/> 
If I can be of assistance, please do not hesitate to contact me. 