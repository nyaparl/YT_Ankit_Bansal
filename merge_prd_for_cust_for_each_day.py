from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [(1, '2024-02-18', 101),
(1, '2024-02-18', 102),
(1, '2024-02-19', 101),
(1, '2024-02-19', 103),
(2, '2024-02-18', 104),
(2, '2024-02-18', 105),
(2, '2024-02-19', 101),
(2, '2024-02-19', 106) ]
cols = ['cid','dte','pid']

# Generate the data in the file

genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/LENOVO/AWSBootCamp/AWSBootCamp/YT_Ankit_Bansal/input.csv')
df.show()
df.createOrReplaceTempView('temp')

spark.sql(" WITH CTE1 as (select cid,dte,concat_ws(',',collect_list(pid)) pid from temp \
            group by cid,dte order by dte) \
          select C.dte,C.pid from CTE1 C \
           UNION select dte,pid from temp order by dte").show()

##############################################################################
#           Output
##############################################################################

# Input data
# +---+----------+---+
# |cid|       dte|pid|
# +---+----------+---+
# |  1|2024-02-18|101|
# |  1|2024-02-18|102|
# |  1|2024-02-19|101|
# |  1|2024-02-19|103|
# |  2|2024-02-18|104|
# |  2|2024-02-18|105|
# |  2|2024-02-19|101|
# |  2|2024-02-19|106|
# +---+----------+---+

# Output data
# +----------+-------+
# |       dte|    pid|
# +----------+-------+
# |2024-02-18|104,105|
# |2024-02-18|101,102|
# |2024-02-18|    105|
# |2024-02-18|    102|
# |2024-02-18|    104|
# |2024-02-18|    101|
# |2024-02-19|101,103|
# |2024-02-19|101,106|
# |2024-02-19|    103|
# |2024-02-19|    101|
# |2024-02-19|    106|
# +----------+-------+
