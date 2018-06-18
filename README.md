# PartitionerMapReduce
This project shows implementation of Partitioner in MapReduce

A custom partitioner allows you to run different number of reducers based on a user's condition.
 A partitioner ensures that only one reducer receives all the records for that particular key.
 
 
# Use Case

Aim is to divide a reducer into multiple Departments in an Employee Details dataset and find out the highest paid employee from every department.

# Sample Data

1001,Abhiram,Program Department,Jr Software Engineer,45000,abhiram@instict.com
1002,Sam,Admin Department,Jr Network Engineer,30000,sam@instict.com
1003,Salman,Accounts Department,Asst Acccounts Manager,100000,salman@instinct.com
1004,Vihan,Program Department,Sr Software Engineer,90000,vihan@instinct.com
1006,Kiran,Program Department,Sr Software Engineer,90000,kiran@instinct.com
1009,Omkar,Admin Department,Admin Manager,110000,omkar@instict.com
1005,Prateek Baba,Program Department,Sr Software Engineer,90000,prateek@instinct.com
1007,Adam,Accounts Department,Manager,150000,adam@instinct.com
1008,Sofia,Program Department,Sr Software Engineer,90000,chris@instinct.com
1010,Rishi,Admin Department,System Admin,45000,rishi@instict.com

# Output

[kanika@kanika jars]$ hadoop fs -cat /Partitioneroutput1/part-r-00000
Program Department	1008	Sofia	Sr Software Engineer	90000
[kanika@kanika jars]$ hadoop fs -cat /Partitioneroutput1/part-r-00001
Admin Department	1009	Omkar	Admin Manager	110000
[kanika@kanika jars]$ hadoop fs -cat /Partitioneroutput1/part-r-00002
Accounts Department	1007	Adam	Manager	150000
