[SQL Portfolio Project - Data Bank-2 (1).pdf](https://github.com/Qurat05/DataBank-SQL/files/14497903/SQL.Portfolio.Project.-.Data.Bank-2.1.pdf)
# DataBank-SQL (Portfolio Project)

# Introduction
There is a new innovation in the financial industry called Neo-Banks: new aged digital only banks without physical branches.

Danny thought that there should be some sort of intersection between these new age banks, cryptocurrency and the data world…so he decides to launch a new initiative - Data Bank!

Data Bank runs just like any other digital bank - but it isn’t only for banking activities, they also have the world’s most secure distributed data storage platform!

Customers are allocated cloud data storage limits which are directly linked to how much money they have in their accounts. There are a few interesting caveats that go with this business model, and this is where the Data Bank team need your help!

This file contain sql code for all the relevant questions that Data Bank Team had. Following were questions as per sections were answered using MySql in WorkBench.

# Customer Nodes Count
How many unique nodes are there on the Data Bank system?
What is the number of nodes per region?
How many customers are allocated to each region?
How many days on average are customers reallocated to a different node?
What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
B. Customer Transactions
What is the unique count and total amount for each transaction type?
What is the average total historical deposit counts and amounts for all customers?
For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
What is the closing balance for each customer at the end of the month?
What is the percentage of customers who increase their closing balanceby more than 5%?
C. Data Allocation Challenge
To test out a few different hypotheses - the Data Bank team wants to run an experiment where different groups of customers would be allocated data using 3 different options:

● Option 1: data is allocated based off the amount of money at the end ofthe previous month.

● Option 2: data is allocated on the average amount of money kept in theaccount in the previous 30 days.

● Option 3: data is updated real-time.

For this multi-part challenge question - you have been requested to generate the following data elements to help the Data Bank team estimate how much data will need to be provisioned for each option:

● running customer balance column that includes the impact each transaction.

● customer balance at the end of each month.

● minimum, average and maximum values of the running balance for each customer.

Using all of the data available - how much data would have been required for each option on a monthly basis?

# D. Extra Challenge
Data Bank wants to try another option which is a bit more difficult to implement - they want to calculate data growth using an interest calculation, just like in a traditional savings account you might have with a bank.

If the annual interest rate is set at 6% and the Data Bank team wants to reward its customers by increasing their data allocation based off the interest calculated on a daily basis at the end of each day, how much data would be required for this option on a monthly basis?

# Special notes:

● Data Bank wants an initial calculation which does not allow for compounding interest, however they may also be interested in a daily compounding interest calculation so you can try to perform this calculation if you have the stamina!
