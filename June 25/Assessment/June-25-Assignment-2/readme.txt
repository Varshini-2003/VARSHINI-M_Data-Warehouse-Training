order_id,customer_id,amount
1,1001,250
2,,300
3,1003,


replace the above in the orders.csv to see the error output in the log

output:
Null values found:
customer_id    1
amount         1


proper csv (successfull run)

order_id,customer_id,amount
1,1001,250
2,1002,300
3,1003,180