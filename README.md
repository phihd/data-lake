# Project: Data Lake
The project is a part of Udacity Data Engineering Nanodegree, details can be found here: https://www.udacity.com/course/data-engineer-nanodegree--nd027

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The project aims to create an ETL pipeline that extracts the data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. Fact and dimiension tables are defined in the form of a star schema for a particular analytic focus. The data are also partitioned by coresponding columns to avoid data skewness when Spark processes the data with multiple clusters.

Keywords: Spark, S3, Python.

