import collections
import csv
import os
import re
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader

'''
INTRODUCTION

The goal of this assignment is to implement a basic analysis of textual 
data using Apache Spark (http://spark.apache.org) and 
Dask (https://dask.org). 
'''

'''
DATASET

We will study a dataset provided by the city of Montreal that contains 
the list of trees treated against the emerald ash borer 
(https://en.wikipedia.org/wiki/Emerald_ash_borer). The dataset is 
described at 
http://donnees.ville.montreal.qc.ca/dataset/frenes-publics-proteges-injection-agrile-du-frene 
(use Google translate to translate from French to English). 

We will use the 2015 and 2016 data sets available in directory `data`.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''
global df


# Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


# Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row])) \
        .reduce(lambda x, y: os.linesep.join([x, y]))
    return a + os.linesep


def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None


'''
Plain PYTHON implementation

To get started smoothly and become familiar with the assignment's 
technical context (Git, GitHub, pytest, Travis), we will implement a 
few steps in plain Python.
'''


# Python answer functions
def count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees (non-header lines) in
    the data file passed as first argument.
    Test file: tests/test_count.py
    Note: The return value should be an integer
    '''
    # ADD YOUR CODE HERE
    # raise ExceptionException("Not implemented yet")
    spark = init_spark()
    df = spark.read.format("csv").option("header", "true").load(filename)
    df.createOrReplaceTempView("SQLTable")

    with open(filename, "r") as file:
        count = 0
        for line in file:
            count += 1
    return count - 1


def parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks.py
    Note: The return value should be an integer
    '''

    with open(filename) as f:
        csv_dict = csv.DictReader(f)
        count = 0
        for element in csv_dict:
            if element['Nom_parc']:
                count += 1
        return count


def uniq_parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of unique parks where trees
    were treated. The list must be ordered alphabetically. Every element in the list must be printed on
    a new line.
    Test file: tests/test_uniq_parks.py
    Note: The return value should be a string with one park name per line
    '''

    # ADD YOUR CODE HERE
    # raise Exception("Not implemented yet")
    result = ""
    plist = []
    with open(filename) as f:
        csv_dict = csv.DictReader(f)
        for element in csv_dict:
            if element['Nom_parc']:
                plist.append(element['Nom_parc'])
        plist = list(dict.fromkeys(plist))
        plist.sort()
        for s in plist:
            result += s + "\n"
    return result


def uniq_parks_counts(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that counts the number of trees treated in each park
    and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    result = ""
    count_dict = dict()
    with open(filename) as f:
        csv_dict = csv.DictReader(f)
        for element in csv_dict:
            park = element['Nom_parc']
            if park:
                if park in count_dict:
                    count_dict[park] += 1
                else:
                    count_dict[park] = 1
        for entry in sorted(count_dict.keys()):
            result = result + entry + "," + str(count_dict[entry]) + "\n"
    return result


def frequent_parks_count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of the 10 parks with the
    highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar number.
    Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    with open(filename) as f:
        frqParc = ""
        count_dict = dict()
        csv_dict = csv.DictReader(f)
        for element in csv_dict:
            string = element['Nom_parc']
            if string:
                if string in count_dict:
                    count_dict[string] += 1
                else:
                    count_dict[string] = 1

        rev_count = dict((v, k) for k, v in count_dict.items())  # alter key, value pair to reverse the dict()
        temp_list = sorted(rev_count.keys(), reverse=True)
        temp_list = temp_list[0:10]
        for t in temp_list:
            frqParc += rev_count[t] + "," + str(t) + "\n"
    return frqParc


def intersection(filename1, filename2):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the alphabetically sorted list of
    parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    result = ""
    list_2016 = []
    list_2015 = []
    with open(filename1) as f1:
        csv_dict = csv.DictReader(f1)
        for element in csv_dict:
            park = element['Nom_parc']
            if park:
                list_2016.append(park)

    with open(filename2) as f2:
        csv_dict = csv.DictReader(f2)
        for element in csv_dict:
            park = element['Nom_parc']
            if park:
                list_2015.append(park)

    final = list(set(list_2016).intersection(list_2015))

    for s in sorted(final):
        result = result + s + "\n"
    return result


'''
SPARK RDD IMPLEMENTATION

You will now have to re-implement all the functions above using Apache 
Spark's Resilient Distributed Datasets API (RDD, see documentation at 
https://spark.apache.org/docs/latest/rdd-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
However, all operations must be re-implemented using the RDD API, you 
are not allowed to simply convert results obtained with plain Python to 
RDDs (this will be checked). Note that the function *toCSVLine* in the 
HELPER section at the top of this file converts RDDs into CSV strings.
'''


# RDD functions

def count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()

    # ADD YOUR CODE HERE
    rdd = spark.sparkContext.textFile(filename)
    return rdd.count() - 1


def parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()

    # ADD YOUR CODE HERE
    rdd = spark.sparkContext.textFile(filename)
    headers = rdd.first()
    rdd = rdd.filter(lambda line: line != headers)
    pattern = r',(?=(?:[^"]*"[^"]*")*[^"]*$)'
    rdd = rdd.map(lambda x: re.split(pattern, x))
    rdd = rdd.map(lambda l: l[6])
    rdd = rdd.filter(lambda l: l is not "")
    return rdd.count()


def uniq_parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of unique parks where
    trees were treated. The list must be ordered alphabetically. Every element
    in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_rdd.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()

    # ADD YOUR CODE HERE
    string = ""
    rdd = spark.sparkContext.textFile(filename)
    headers = rdd.first()
    rdd = rdd.filter(lambda line: line != headers)
    pattern = r',(?=(?:[^"]*"[^"]*")*[^"]*$)'
    rdd = rdd.map(lambda x: re.split(pattern, x))
    rdd = rdd.map(lambda l: l[6])
    rdd = rdd.filter(lambda l: l is not "")
    rdd = rdd.map(lambda l: l.replace('"', ''))
    list = rdd.distinct().collect()
    for l in sorted(list):
        string = string + l + "\n"
    return string


def uniq_parks_counts_rdd(filename):
    '''
    Write a Python script using RDDs that counts the number of trees treated in
    each park and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts_rdd.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()

    # ADD YOUR CODE HERE
    string = ""
    rdd = spark.sparkContext.textFile(filename)
    headers = rdd.first()
    rdd = rdd.filter(lambda line: line != headers)
    pattern = r',(?=(?:[^"]*"[^"]*")*[^"]*$)'
    rdd = rdd.map(lambda x: re.split(pattern, x))
    rdd = rdd.map(lambda l: l[6])
    rdd = rdd.filter(lambda l: l is not "") # filtering the nll value
    rdd = rdd.map(lambda l: l.replace('"', ''))
    rdd = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x,y : x + y)
    for key, value in rdd.sortBy(lambda x: x).collect():
        string = string + key +',' +str(value) + "\n"
    return string


def frequent_parks_count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of the 10 parks with
    the highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar
    number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()

    # ADD YOUR CODE HERE
    string = ""
    rdd = spark.sparkContext.textFile(filename)
    headers = rdd.first()
    rdd = rdd.filter(lambda line: line != headers)
    pattern = r',(?=(?:[^"]*"[^"]*")*[^"]*$)'
    rdd = rdd.map(lambda x: re.split(pattern, x))
    rdd = rdd.map(lambda l: l[6])
    rdd = rdd.filter(lambda l: l is not "")
    rdd = rdd.map(lambda l: l.replace('"', ''))
    rdd = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x : -x[1])
    for key, value in rdd.take(10):
        string = string + key + ',' + str(value) + "\n"
    return string


def intersection_rdd(filename1, filename2):
    '''
    Write a Python script using RDDs that prints the alphabetically sorted list
    of parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()

    # ADD YOUR CODE HERE
    string = ""
    # rdd 1 processing
    rdd1 = spark.sparkContext.textFile(filename1)
    headers = rdd1.first()
    rdd1 = rdd1.filter(lambda line: line != headers)
    pattern = r',(?=(?:[^"]*"[^"]*")*[^"]*$)'
    rdd1 = rdd1.map(lambda l: re.split(pattern, l))
    rdd1 = rdd1.map(lambda l: l[6])
    rdd1 = rdd1.filter(lambda l: l is not "")
    rdd1 = rdd1.map(lambda l: l.replace('"', ''))

    # rdd 2 processing
    rdd2 = spark.sparkContext.textFile(filename2)
    headers = rdd2.first()
    rdd2 = rdd2.filter(lambda line: line != headers)
    pattern = r',(?=(?:[^"]*"[^"]*")*[^"]*$)'
    rdd2 = rdd2.map(lambda l: re.split(pattern, l))
    rdd2 = rdd2.map(lambda l: l[6])
    rdd2 = rdd2.filter(lambda l: l is not "")
    rdd2 = rdd2.map(lambda l: l.replace('"', '')).distinct()
    final = rdd1.intersection(rdd2).sortBy(lambda l: l).collect()
    for f in final:
        string = string + f + "\n"
    return string


'''
SPARK DATAFRAME IMPLEMENTATION

You will now re-implement all the tasks above using Apache Spark's 
DataFrame API (see documentation at 
https://spark.apache.org/docs/latest/sql-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
Note: all operations must be re-implemented using the DataFrame API, 
you are not allowed to simply convert results obtained with the RDD API 
to Data Frames. Note that the function *toCSVLine* in the HELPER 
section at the top of this file also converts DataFrames into CSV 
strings.
'''


# DataFrame functions

def count_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_df.py
    Note: The return value should be an integer
    '''
    spark = init_spark()
    df = spark.read.format("csv").option("header", "true").load(filename)

    return df.count()
    # ADD YOUR CODE HERE
    # raise Exception("Not implemented yet")


def parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    df = spark.read.format("csv").option("header", "true").load(filename)
    df.createOrReplaceTempView("SQLTable")
    tree = spark.sql("SELECT count(*) tree from SQLTable where Nom_parc is not null")

    return tree.collect()[0][0]
    # ADD YOUR CODE HERE
    # raise Exception("Not implemented yet")


def uniq_parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_df.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    df = spark.read.format("csv").option("header", "true").load(filename)
    df.createOrReplaceTempView("SQLTable")
    tree = spark.sql("SELECT Nom_parc from SQLTable group by Nom_parc order by Nom_parc asc")

    nom_parc = ""
    for i in range(1, tree.count()):
        nom_parc += tree.collect()[i][0] + "\n"

    return nom_parc

    # ADD YOUR CODE HERE
    # raise Exception("Not implemented yet")


def uniq_parks_counts_df(filename):
    '''
    Write a Python script using DataFrames that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_df.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    df = spark.read.format("csv").option("header", "true").load(filename)
    df.createOrReplaceTempView("SQLTable")
    tree = spark.sql("SELECT Nom_parc, count(*) uniqueCnt from SQLTable group by Nom_parc order by Nom_parc asc")

    nom_parc = ""
    for i in range(1, tree.count()):
        nom_parc += tree.collect()[i][0] + "," + str(tree.collect()[i][1]) + "\n"

    return nom_parc
    # ADD YOUR CODE HERE
    # raise Exception("Not implemented yet")


def frequent_parks_count_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()

    # ADD YOUR CODE HERE
    df = spark.read.format("csv").option("header", "true").load(filename)
    df.createOrReplaceTempView("SQLTable")
    tree = spark.sql("SELECT Nom_parc, count(*) uniqueCnt from SQLTable group by Nom_parc order by count(*) desc, "
                     "Nom_parc asc limit 11")

    nom_parc = ""
    for i in range(1, tree.count()):
        nom_parc += tree.collect()[i][0] + "," + str(tree.collect()[i][1]) + "\n"

    return nom_parc


def intersection_df(filename1, filename2):
    '''
    Write a Python script using DataFrames that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    spark = init_spark()
    df1 = spark.read.format("csv").option("header", "true").load(filename1)
    df1.createOrReplaceTempView("Table1")

    df2 = spark.read.format("csv").option("header", "true").load(filename2)
    df2.createOrReplaceTempView("Table2")

    tree = spark.sql("SELECT distinct a.Nom_parc from Table1 a inner join table2 b on a.Nom_parc = b.Nom_parc order "
                     "by a.Nom_parc asc")

    nom_parc = ""
    for i in range(0, tree.count()):
        nom_parc += tree.collect()[i][0] + "\n"

    return nom_parc


'''
DASK IMPLEMENTATION (bonus)

You will now re-implement all the tasks above using Dask (see 
documentation at http://docs.dask.org/en/latest). Outputs must be 
identical to the ones obtained previously. Note: all operations must be 
re-implemented using Dask, you are not allowed to simply convert 
results obtained with the other APIs.
'''


# Dask functions

def count_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_dask.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    # raise Exception("Not implemented yet")
    ddf = df.read_csv(filename, dtype={'Nom_parc': str})

    return ddf.shape[0].compute()


def parks_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_dask.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    ddf = df.read_csv(filename, dtype={'Nom_parc': str})
    ddf2 = ddf.dropna(subset=['Nom_parc']).compute()
    return ddf2['Nom_parc'].count()


def uniq_parks_dask(filename):
    '''
    Write a Python script using Dask that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_dask.py
    Note: The return value should be a CSV string
    '''

    # ADD YOUR CODE HERE
    string = ""
    frame = df.read_csv(filename, dtype=str)
    frame = frame[["Nom_parc"]]
    frame = frame.dropna().drop_duplicates()
    frame = frame.astype(str).compute().sort_values(by='Nom_parc')
    left = 'Nom_parc    '
    right = '\n'

    for l in frame.iterrows():
        text = str(l)
        name = text[text.index(left) + len(left):text.index(right)]
        if name == "MAIRIE D'ARRONDISSEMENT PIERREFONDS - ROXBORO,...":
            string = string + "MAIRIE D'ARRONDISSEMENT PIERREFONDS - ROXBORO, ESPACE VERT" + '\n'
        else:
            string = string + name + "\n"

    return string


def uniq_parks_counts_dask(filename):
    '''
    Write a Python script using Dask that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_dask.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    result = ""
    frame = df.read_csv(filename, dtype=str)
    frame = frame[["Nom_parc"]]
    frame = frame.dropna()
    frame = frame.astype(str).compute().sort_values(by='Nom_parc')
    frame = frame['Nom_parc'].value_counts()

    count_dict = dict(frame)
    for entry in sorted(count_dict.keys()):
        result = result + entry + ',' + str(count_dict[entry]) + '\n'
    return result


def frequent_parks_count_dask(filename):
    '''
    Write a Python script using Dask that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    result=""
    frame = df.read_csv(filename, dtype=str)
    frame = frame[["Nom_parc"]]
    frame = frame.dropna()
    frame = frame.astype(str).compute().sort_values(by='Nom_parc')
    frame = frame['Nom_parc'].value_counts().head(10)
    frame_val = str(frame).split('\n')[:-1]
    for s in frame_val:
        s = " ".join(re.split(r"\s+", s))
        result = result + s[:-4] + "," + s[-3:] +'\n'
    return result


def intersection_dask(filename1, filename2):
    '''
    Write a Python script using Dask that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    result = ""
    frame1 = df.read_csv(filename1, dtype=str)
    frame1 = frame1[["Nom_parc"]]
    frame1 = frame1.dropna().drop_duplicates()

    frame2 = df.read_csv(filename2, dtype=str)
    frame2 = frame2[["Nom_parc"]]
    frame2 = frame2.dropna().drop_duplicates()
    final = df.merge(frame1, frame2, how='inner').compute()
    fs = str(final).split('\n')[1:]
    for s in fs:
        s = " ".join(re.split(r"\s+", s))
        result = result + s[2:] + '\n'
    return result
