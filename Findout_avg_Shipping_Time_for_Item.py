from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [(1, "Standard", "2023-01-01", "2023-01-05"),
 (2, "Express", "2023-01-02", "2023-01-03"),
 (3, "Standard", "2023-01-03", "2023-01-08"),
 (4, "Overnight", "2023-01-04", "2023-01-05"),
 (5, "Express", "2023-01-05", "2023-01-06"),
 (6, "Standard", "2023-01-06", "2023-01-10"),
 (7, "Express", "2023-01-07", "2023-01-08"),
 (8, "Standard", "2023-01-08", "2023-01-12"),
 (9, "Overnight", "2023-01-09", "2023-01-10"),
 (10, "Express", "2023-01-10", "2023-01-11"),
 (11, "Standard", "2023-01-11", "2023-01-15"),
 (12, "Express", "2023-01-12", "2023-01-13"),
 (13, "Overnight", "2023-01-13", "2023-01-14"),
 (14, "Standard", "2023-01-14", "2023-01-18"),
 (15, "Express", "2023-01-15", "2023-01-16")]
cols = ['o_id','ship_method','o_date','s_date']

# Generate the data in the file

genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/LENOVO/AWSBootCamp/AWSBootCamp/YT_Ankit_Bansal/input.csv')
df.show()
df.createOrReplaceTempView('temp')

spark.sql(" WITH CTE1 as (select *, datediff(day,o_date,s_date) ship_in_days from temp) \
           Select ship_method, round(avg(ship_in_days)) avg_time_to_ship from CTE1 group by ship_method \
            order by round(avg(ship_in_days)) desc ").show()

##################################################################################
#            Output                                                              #
##################################################################################

# Input data

# +----+-----------+----------+----------+
# |o_id|ship_method|    o_date|    s_date|
# +----+-----------+----------+----------+
# |   1|   Standard|2023-01-01|2023-01-05|
# |   2|    Express|2023-01-02|2023-01-03|
# |   3|   Standard|2023-01-03|2023-01-08|
# |   4|  Overnight|2023-01-04|2023-01-05|
# |   5|    Express|2023-01-05|2023-01-06|
# |   6|   Standard|2023-01-06|2023-01-10|
# |   7|    Express|2023-01-07|2023-01-08|
# |   8|   Standard|2023-01-08|2023-01-12|
# |   9|  Overnight|2023-01-09|2023-01-10|
# |  10|    Express|2023-01-10|2023-01-11|
# |  11|   Standard|2023-01-11|2023-01-15|
# |  12|    Express|2023-01-12|2023-01-13|
# |  13|  Overnight|2023-01-13|2023-01-14|
# |  14|   Standard|2023-01-14|2023-01-18|
# |  15|    Express|2023-01-15|2023-01-16|
# +----+-----------+----------+----------+

# Output data

# +-----------+----------------+
# |ship_method|avg_time_to_ship|
# +-----------+----------------+
# |   Standard|             4.0|
# |    Express|             1.0|
# |  Overnight|             1.0|
# +-----------+----------------+