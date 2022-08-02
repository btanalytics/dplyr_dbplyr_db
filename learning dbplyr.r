library(dplyr)
library(dbplyr)



df1 <- data.frame(awb = c('a', 'b', 'c'), 
                  dt = c('d1','d2', 'd3'))

df2 <- data.frame(awb = c('a', 'b', 'f'), 
                  dt = c('d1','d4', 'd6'))

rmt_tbl1 = tbl_lazy(df1 , con = simulate_mssql()) # the simulation
rmt_tbl2 = tbl_lazy(df2 , con = simulate_mssql()) # the simulation


df1 |> full_join(df2) 

rmt_tbl1 |> full_join(rmt_tbl2) |> show_query()


rmt_tbl1 |> distinct() |> show_query()

# GOING LIVE - DBI
# using a simple sqlite database
# create or load a database
my_con <- DBI::dbConnect(RSQLite::SQLite(), "D:/Project/R/dplyr_dbplyr_db/test_db.sqlite")
DBI::dbListTables(my_con) # List Tables

# write in df1 into the database
DBI::dbWriteTable(conn=my_con, name="tbl_df1", value=df1, overwrite = F, append = F) 

tbl(my_con, 'tbl_df1')
#   awb   dt   
#   <chr> <chr>
# 1 a     d1   
# 2 b     d2   
# 3 c     d3 


# EXPERIMENT 1 
# try the append new df to an existing table
# Result: Failed - 
#       (i) append data without removing duplicate
#       (ii) if column is unknown in the db.table query fails 
# ATTEMPT TO INSERT df2
DBI::dbWriteTable(conn=my_con, name="tbl_df1", value=df1, overwrite = T, append = F)  # setup
DBI::dbWriteTable(conn=my_con, name="tbl_df1", value=df2, append = T)  # FAILED AS
tbl(my_con, 'tbl_df1')
#   awb   dt   
#   <chr> <chr>
# 1 a     d1   
# 2 b     d2   
# 3 c     d3   
# 4 a     d1
# 5 b     d4
# 6 f     d6

# EXPERIMENT 2 
# https://stackoverflow.com/questions/9231575/removing-duplicates-before-inserting-into-database
# Using a temporary table with 
my_con <- DBI::dbConnect(RSQLite::SQLite(), "D:/Project/R/dplyr_dbplyr_db/test_db.sqlite")
DBI::dbListTables(my_con) # List Tables
DBI::dbWriteTable(conn=my_con, name="tbl_df1", value=df1, overwrite = T, append = F)  # setup
tbl(my_con, 'tbl_df1')

# make a temporary table 
copy_to(my_con, df = df2 , "temp_df",
  temporary = T, 
  overwrite = T,
  indexes = list(
    "awb", "dt"
  )
)

tbl(my_con, 'tbl_df1')
tbl(my_con, 'temp_df')


update_q <- "INSERT INTO tbl_df1 (awb, dt)
SELECT awb, dt
FROM temp_df AS t
WHERE NOT EXISTS
(
  SELECT 1 FROM tbl_df1
    WHERE awb = t.awb
    AND dt = t.dt
);"

DBI::dbExecute(
  my_con,
  update_q
)

tbl(my_con, 'tbl_df1')
tbl(my_con, 'temp_df')

# DISCONNECT
DBI::dbDisconnect(my_con)
