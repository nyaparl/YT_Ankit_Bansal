from sessionCreation import *

# Spark session created.
spark = sparkSession()

l1 = [('India','SL','India'),
      ('SL','Aus','Aus'),
      ('SA','Eng','Eng'),
      ('Eng','NZ','NZ'),
      ('Aus','India','India')]
cols = ['team_1','team_2','winner']

# Generate the data in the file

genearateInputFile(l1,cols)

df = spark.read.format('csv').option('header','true') \
                             .option('inferSchema','true') \
                             .load('C:/Users/LENOVO/AWSBootCamp/AWSBootCamp/YT_Ankit_Bansal/input.csv')

df.show()
df.createOrReplaceTempView('temp')

spark.sql("WITH CTE1 as (select team_1 team, \
          CASE WHEN team_1 = winner then 1 else 0 \
          END as win_count from temp \
          UNION ALL \
            select team_2 team, \
            CASE WHEN team_2 = winner then 1 else 0 \
            END as win_count from temp) \
          select team, count(*) tot_matches_played, sum(win_count) no_of_wins, \
          count(*)-sum(win_count) no_of_losses from CTE1 group by team order by no_of_wins desc ").show()

###################################################################################33
#             OUTPUT                                                                #
###################################################################################33

#Input data

# +------+------+------+
# |team_1|team_2|winner|
# +------+------+------+
# | India|    SL| India|
# |    SL|   Aus|   Aus|
# |    SA|   Eng|   Eng|
# |   Eng|    NZ|    NZ|
# |   Aus| India| India|
# +------+------+------+

# Output data

# +-----+------------------+----------+------------+
# | team|tot_matches_played|no_of_wins|no_of_losses|
# +-----+------------------+----------+------------+
# |India|                 2|         2|           0|
# |  Eng|                 2|         1|           1|
# |  Aus|                 2|         1|           1|
# |   NZ|                 1|         1|           0|
# |   SL|                 2|         0|           2|
# |   SA|                 1|         0|           1|
# +-----+------------------+----------+------------+