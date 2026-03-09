# Module 6 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2025-11 data from the official website:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```

## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/06-batch/setup/)

> **Answer:** **4.1.1**
>
> ```
> $ sudo apt install openjdk-17-jdk-headless
> ...
> $ curl -LsSf https://astral.sh/uv/install.sh | sh
> ...
> $ uv --version
> uv 0.10.9
> $ mkdir sparklab && cd sparklab
> $ uv init
> Initialized project `sparklab`
> $ uv add pyspark
> ...
> $ code test-pyspark.py
> ...
> $ uv run test-pyspark.py 2> /dev/null | grep version
> Spark version: 4.1.1
> ```

## Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB

> **Answer:** **25MB**
>
> ```
> $ ./download.sh
> ...
> $ uv run ./question2.py
> ...
> $ ls -l ../data/output_yellow_2025_11
> total 103768
> -rw-r--r-- 1 codespace codespace        0 Mar  9 01:18 _SUCCESS
> -rw-r--r-- 1 codespace codespace 26575777 Mar  9 01:18 part-00000-a24da03d-fa83-446b-adb6-24e7685afd3f-c000.snappy.parquet
> -rw-r--r-- 1 codespace codespace 26554692 Mar  9 01:18 part-00001-a24da03d-fa83-446b-adb6-24e7685afd3f-c000.snappy.parquet
> -rw-r--r-- 1 codespace codespace 26550812 Mar  9 01:18 part-00002-a24da03d-fa83-446b-adb6-24e7685afd3f-c000.snappy.parquet
> -rw-r--r-- 1 codespace codespace 26565758 Mar  9 01:18 part-00003-a24da03d-fa83-446b-adb6-24e7685afd3f-c000.snappy.parquet
> ```

## Question 3: Count records

How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.

- 62,610
- 102,340
- 162,604
- 225,768

> **Answer:** **162,604**
>
> ```
> $ uv run ./question3.py
> ...
> Viajes el 15 de Noviembre: 162604
> ```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 22.7
- 58.2
- 90.6
- 134.5

> **Answer:** **90.6**
>
> ```
> $ uv run ./question4.py
> ...
> +-------------------+
> |max(duration_hours)|
> +-------------------+
> |  90.64666666666666|
> +-------------------+
> ```

## Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

> **Answer:** **4040**
>
> ```
> $ uv run ./question5.py
> ...
> tcp        LISTEN       0        1                  *:4040                   *:*
> ...
> Press <ENTER> to session exit...
> ```

## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

If multiple answers are correct, select any

> **Answer:** **Governor's Island/Ellis Island/Liberty Island** and/or **Arden Heights**
>
> ```
> $ uv run ./question6.py
> ...
> +---------------------------------------------+-----+
> |Zone                                         |count|
> +---------------------------------------------+-----+
> |Governor's Island/Ellis Island/Liberty Island|1    |
> |Eltingville/Annadale/Prince's Bay            |1    |
> |Arden Heights                                |1    |
> |Port Richmond                                |3    |
> |Rikers Island                                |4    |
> +---------------------------------------------+-----+
> only showing top 5 rows
> ```

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw6
- Deadline: See the website

## Learning in Public

We encourage everyone to share what they learned. This is called "learning in public".

Read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

### Example post for LinkedIn

```
🚀 Week 6 of Data Engineering Zoomcamp by @DataTalksClub complete!

Just finished Module 6 - Batch Processing with Spark. Learned how to:

* Set up PySpark and create Spark sessions
* Read and process Parquet files at scale
* Repartition data for optimal performance
* Analyze millions of taxi trips with DataFrames
* Use Spark UI for monitoring jobs

Processing 4M+ taxi trips with Spark - distributed computing is powerful! 💪

Here's my homework solution: https://github.com/tnotstar/data-engineering-zoomcamp-2026-06-batch

Following along with this amazing free course - who else is learning data engineering?

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

### Example post for Twitter/X

```
⚡ Module 6 of Data Engineering Zoomcamp done!

- Batch processing with Spark 🔥
- PySpark & DataFrames
- Parquet file optimization
- Spark UI on port 4040

My solution: <LINK>

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```
