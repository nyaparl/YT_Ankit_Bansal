from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [(1,100,'2022-01-01',2000),(2,200,'2022-01-01',2500),(3,300,'2022-01-01',2100),
(4,100,'2022-01-02',2000),(5,400,'2022-01-02',2200),(6,500,'2022-01-02',2700),
(7,100,'2022-01-03',3000),(8,400,'2022-01-03',1000),(9,600,'2022-01-03',3000)]
cols = ['o_id','c_id','o_date','o_amt']

# Generate the data in the file

genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/LENOVO/AWSBootCamp/AWSBootCamp/YT_Ankit_Bansal/input.csv')
df.show()
df.createOrReplaceTempView('temp')

spark.sql("with first_visit (select c_id,min(o_date) first_ord_date from temp group by c_id) \
          select co.o_date,  \
          sum(CASE WHEN co.o_date = fv.first_ord_date then 1 else 0 end) new_cust, \
          sum(CASE WHEN co.o_date != fv.first_ord_date then 1 else 0 end) repeat_cust \
           from first_visit fv join temp co on fv.c_id = co.c_id \
          group by co.o_date order by co.o_date").show()

################################################################################
#        OUTPUT                                                                #
################################################################################

# Input Data
# +----+----+----------+-----+
# |o_id|c_id|    o_date|o_amt|
# +----+----+----------+-----+
# |   1| 100|2022-01-01| 2000|
# |   2| 200|2022-01-01| 2500|
# |   3| 300|2022-01-01| 2100|
# |   4| 100|2022-01-02| 2000|
# |   5| 400|2022-01-02| 2200|
# |   6| 500|2022-01-02| 2700|
# |   7| 100|2022-01-03| 3000|
# |   8| 400|2022-01-03| 1000|
# |   9| 600|2022-01-03| 3000|
# +----+----+----------+-----+

# Output data
# +----------+--------+-----------+
# |    o_date|new_cust|repeat_cust|
# +----------+--------+-----------+
# |2022-01-01|       3|          0|
# |2022-01-02|       2|          1|
# |2022-01-03|       1|          2|
# +----------+--------+-----------+

