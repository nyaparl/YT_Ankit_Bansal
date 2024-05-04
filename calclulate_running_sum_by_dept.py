from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [("James", "Sales", 3000),
 ("Michael", "Sales", 4600),
 ("Robert", "Sales", 4100),
 ("Maria", "Finance", 3000),
 ("James", "Sales", 3000),
 ("Scott", "Finance", 3300),
 ("Jen", "Finance", 3900),
 ("Jeff", "Marketing", 3000),
 ("Kumar", "Marketing", 2000),
 ("Saif", "Sales", 4100) ]
cols = ['ename','dept','sal']

# Generate the data in the file

genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/LENOVO/AWSBootCamp/AWSBootCamp/YT_Ankit_Bansal/input.csv')
df.show()
df.createOrReplaceTempView('temp')

spark.sql(" select *, sum(sal) over(partition by dept \
 order by row_number() over(partition by dept order by sal)) dept_running_sum from temp").show()

#################################################################################33
#        OUTPUT
#################################################################################33

# Input data
# +-------+---------+----+
# |  ename|     dept| sal|
# +-------+---------+----+
# |  James|    Sales|3000|
# |Michael|    Sales|4600|
# | Robert|    Sales|4100|
# |  Maria|  Finance|3000|
# |  James|    Sales|3000|
# |  Scott|  Finance|3300|
# |    Jen|  Finance|3900|
# |   Jeff|Marketing|3000|
# |  Kumar|Marketing|2000|
# |   Saif|    Sales|4100|
# +-------+---------+----+

# Output data
# +-------+---------+----+----------------+
# |  ename|     dept| sal|dept_running_sum|
# +-------+---------+----+----------------+
# |  Maria|  Finance|3000|            3000|
# |  Scott|  Finance|3300|            6300|
# |    Jen|  Finance|3900|           10200|
# |  Kumar|Marketing|2000|            2000|
# |   Jeff|Marketing|3000|            5000|
# |  James|    Sales|3000|            3000|
# |  James|    Sales|3000|            6000|
# | Robert|    Sales|4100|           10100|
# |   Saif|    Sales|4100|           14200|
# |Michael|    Sales|4600|           18800|
# +-------+---------+----+----------------+

