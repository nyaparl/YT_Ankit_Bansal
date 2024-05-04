from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [('A','Bangalore','A@gmail.com',1,'CPU'),('A','Bangalore','A1@gmail.com',1,'CPU'),('A','Bangalore','A2@gmail.com',2,'DESKTOP')
,('B','Bangalore','B@gmail.com',2,'DESKTOP'),('B','Bangalore','B1@gmail.com',2,'DESKTOP'),('B','Bangalore','B2@gmail.com',1,'MONITOR') ]
cols = ['name','addr','email','flor','resource']

# Generate the data in the file

genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/LENOVO/AWSBootCamp/AWSBootCamp/YT_Ankit_Bansal/input.csv')
df.show()
df.createOrReplaceTempView('temp')

spark.sql(" With CTE1 as (select name, count(*) tot_visited, concat_ws(',',collect_set(resource)) \
                          resource from temp group by name), \
                 CTE2 as (select name,flor, count(*) most_visited, \
                  rank() over(partition by name order by count(*) desc) rn from temp group by name,flor) \
                  select c2.name,c1.tot_visited,c2.flor most_visited_flor,c1.resource From CTE2 C2 join CTE1 C1 on \
                   c1.name = c2.name where rn = 1 order by c2.name").show()

