### Author Note
This question are answered based on my limited skills and experiences. Although I believed there are a better practices and approach in solving each steps with different motivation or intention, all the approach here are explained according to my understanding as well as concern. Nevertheless, I thank for people that takes the time to go through this simple work. :smiley:

### Extraction


```python
from tabula import read_pdf
import pandas as pd
import numpy as np
import sqlite3
import os
```


```python
RES = os.path.join(os.getcwd(), 'res')

DATA = os.path.join(RES, 'Pre-interview Questionnaire.pdf')
BOOKSTORE = os.path.join(RES, 'bookstore.db')
```


```python
# read data
df = read_pdf(DATA, pages='all', multiple_tables=True)
df
```




    [   id         name                   email           tel           created_at  \
     0   1  Irfan Bakti     irfan88@example.com  6.012346e+10  2019-08-07 08:13:21   
     1   2  Jack Smmith     jack.smmith@acme.io  6.013246e+10  2019-08-07 08:13:21   
     2   3        Nazir                     NaN  6.011854e+11  2019-08-07 08:13:21   
     3   4      Faiz Ma       faiz.ma@jholow.cn  6.019772e+09  2019-08-07 08:13:21   
     4   5   Isham Rais        isham@pmo.gov.my  6.013548e+10  2019-08-07 08:13:21   
     5   6  Shanon Teoh  shahnon.teoh@st.com.sg           NaN  2019-08-07 08:13:21   
     
                 updated_at  
     0  2019-08-07 08:13:21  
     1  2019-08-07 08:13:21  
     2  2019-08-07 08:13:21  
     3  2019-08-07 08:13:21  
     4  2019-08-07 08:13:21  
     5  2019-08-07 08:13:21  ,
        id    number  sub_total  tax_total  total  customer_id  \
     0   1  20190001       30.0        0.0   30.0            1   
     1   2  20190002      150.0        0.0  150.0            2   
     2   3  20190003       30.0        0.0   30.0            2   
     3   4  20190004       55.0        0.0   55.0            3   
     4   5  20190005      450.0        0.0    0.0            6   
     
                 created_at           updated_at  
     0  2019-08-07 08:13:21  2019-08-07 08:13:21  
     1  2019-08-07 08:13:21  2019-08-07 08:13:21  
     2  2019-09-15 08:13:21  2019-09-15 08:13:21  
     3  2019-09-15 08:13:21  2019-09-15 08:13:21  
     4  2019-09-15 08:13:21  2019-09-15 08:13:21  ,
        id description  unit_price  quantity  sub_total  tax_total  total tax_id  \
     0   1     Book #1        30.0         1       30.0        0.0   30.0   NULL   
     1   2     Book #2        25.0         4      100.0        0.0  100.0   NULL   
     2   3     Book #3        50.0         1       50.0        0.0   50.0   NULL   
     3   4     Book #1        30.0         1       30.0        0.0   30.0   NULL   
     4   5     Book #1        30.0         1       30.0        0.0   30.0   NULL   
     5   6     Book #2        25.0         1       25.0        0.0   25.0   NULL   
     6   7     Book #1        30.0         5      150.0        0.0  150.0   NULL   
     7   8     Book #3        50.0         6      300.0        0.0  300.0   NULL   
     
        sku_id  invoice_id  
     0       1           1  
     1       2           2  
     2       3           2  
     3       1           3  
     4       1           4  
     5       2           4  
     6       1           5  
     7       3           5  ]




```python
len(df) # so the len of df is 3 equivalent to 3 tables
```




    3




```python
tables = [
    'customers',
    'invoices',
    'invoice_lines'
]
```

## Loading


```python
# create local SQL from dataframe
def df_to_sqlite3(dataframe, tablesName, database):
    conn = sqlite3.connect(database)
    
    # simple sanity check
    assert len(dataframe)==len(tablesName), 'Check extracted table or table names'
    
    for i in range(len(dataframe)):
        dataframe[i].to_sql(tablesName[i], conn, if_exists='replace', index=False)

    conn.close()
```


```python
df_to_sqlite3(df, tables, BOOKSTORE)
```

## Environment
Make sure you run this in jupyter notebook instead of VScode notebook, this SQL environment will only applicable. If error rise you may need further setting for ipython-sql settings. Either 'pip' or 'conda' depending on your own environment configuration.


```python
# SQL to run in jupyter notebook
%load_ext sql
%sql sqlite:///res/bookstore.db
```




    'Connected: @res/bookstore.db'



## Question 1

To my best knowledge and understanding, Data Engineer roles are as the job name implies is to deals with data. Based on my limited experiences, data engineer do three main things, extraction, transform and loading (for usage). While I believed in large traffic, data engineer also deals with optimization for speed or velocity of incoming and outcoming of data. In an essence, data engineer is the people whom will deals with the data pipeline and maintaining its operation. While I believed that data should be secured, data security is also a concern in this upcoming era.

## Question 2 - a


```sql
%%sql

SELECT name
FROM sqlite_schema
```

     * sqlite:///res/bookstore.db
    Done.
    




<table>
    <thead>
        <tr>
            <th>name</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>customers</td>
        </tr>
        <tr>
            <td>invoices</td>
        </tr>
        <tr>
            <td>invoice_lines</td>
        </tr>
    </tbody>
</table>



## Question 2 - b


```sql
%%sql

SELECT c.name, il.quantity
FROM customers AS c
LEFT JOIN invoices AS i
ON c.id = i.customer_id
RIGHT JOIN invoice_lines AS il
ON i.id = il.invoice_id
```

     * sqlite:///res/bookstore.db
    Done.
    




<table>
    <thead>
        <tr>
            <th>name</th>
            <th>quantity</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Irfan Bakti</td>
            <td>1</td>
        </tr>
        <tr>
            <td>Jack Smmith</td>
            <td>4</td>
        </tr>
        <tr>
            <td>Jack Smmith</td>
            <td>1</td>
        </tr>
        <tr>
            <td>Jack Smmith</td>
            <td>1</td>
        </tr>
        <tr>
            <td>Nazir</td>
            <td>1</td>
        </tr>
        <tr>
            <td>Nazir</td>
            <td>1</td>
        </tr>
        <tr>
            <td>Shanon Teoh</td>
            <td>5</td>
        </tr>
        <tr>
            <td>Shanon Teoh</td>
            <td>6</td>
        </tr>
    </tbody>
</table>




```sql
%%sql

SELECT *
FROM (
    SELECT c.name, SUM(il.quantity) AS total_quantity
    FROM customers AS c
    LEFT JOIN invoices AS i
    ON c.id = i.customer_id
    RIGHT JOIN invoice_lines AS il
    ON i.id = il.invoice_id
    GROUP BY c.name
)

WHERE total_quantity > 5
```

     * sqlite:///res/bookstore.db
    Done.
    




<table>
    <thead>
        <tr>
            <th>name</th>
            <th>total_quantity</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Jack Smmith</td>
            <td>6</td>
        </tr>
        <tr>
            <td>Shanon Teoh</td>
            <td>11</td>
        </tr>
    </tbody>
</table>



## Question 2 - c


```sql
%%sql

SELECT c.name, il.quantity
FROM customers AS c
FULL JOIN invoices AS i
ON c.id = i.customer_id
FULL JOIN invoice_lines AS il
ON i.id = il.invoice_id
```

     * sqlite:///res/bookstore.db
    Done.
    




<table>
    <thead>
        <tr>
            <th>name</th>
            <th>quantity</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Irfan Bakti</td>
            <td>1</td>
        </tr>
        <tr>
            <td>Jack Smmith</td>
            <td>4</td>
        </tr>
        <tr>
            <td>Jack Smmith</td>
            <td>1</td>
        </tr>
        <tr>
            <td>Jack Smmith</td>
            <td>1</td>
        </tr>
        <tr>
            <td>Nazir</td>
            <td>1</td>
        </tr>
        <tr>
            <td>Nazir</td>
            <td>1</td>
        </tr>
        <tr>
            <td>Faiz Ma</td>
            <td>None</td>
        </tr>
        <tr>
            <td>Isham Rais</td>
            <td>None</td>
        </tr>
        <tr>
            <td>Shanon Teoh</td>
            <td>5</td>
        </tr>
        <tr>
            <td>Shanon Teoh</td>
            <td>6</td>
        </tr>
    </tbody>
</table>




```sql
%%sql

SELECT *
FROM (
    SELECT c.name, SUM(il.quantity) AS total_quantity
    FROM customers AS c
    FULL JOIN invoices AS i
    ON c.id = i.customer_id
    FULL JOIN invoice_lines AS il
    ON i.id = il.invoice_id

    GROUP BY c.name
)

WHERE total_quantity IS NULL
```

     * sqlite:///res/bookstore.db
    Done.
    




<table>
    <thead>
        <tr>
            <th>name</th>
            <th>total_quantity</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Faiz Ma</td>
            <td>None</td>
        </tr>
        <tr>
            <td>Isham Rais</td>
            <td>None</td>
        </tr>
    </tbody>
</table>



## Question 2 - d


```sql
%%sql

SELECT c.name, il.description
FROM customers AS c
LEFT JOIN invoices AS i
ON c.id = i.customer_id
RIGHT JOIN invoice_lines AS il
ON i.id = il.invoice_id
```

     * sqlite:///res/bookstore.db
    Done.
    




<table>
    <thead>
        <tr>
            <th>name</th>
            <th>description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Irfan Bakti</td>
            <td>Book #1</td>
        </tr>
        <tr>
            <td>Jack Smmith</td>
            <td>Book #2</td>
        </tr>
        <tr>
            <td>Jack Smmith</td>
            <td>Book #3</td>
        </tr>
        <tr>
            <td>Jack Smmith</td>
            <td>Book #1</td>
        </tr>
        <tr>
            <td>Nazir</td>
            <td>Book #1</td>
        </tr>
        <tr>
            <td>Nazir</td>
            <td>Book #2</td>
        </tr>
        <tr>
            <td>Shanon Teoh</td>
            <td>Book #1</td>
        </tr>
        <tr>
            <td>Shanon Teoh</td>
            <td>Book #3</td>
        </tr>
    </tbody>
</table>



## Question 3

I believed that all the proposed DQL proven to work as to my best understanding for the questions given

## Question 4 - a

The following will be answered based to my limited experiences, for Real time API in XML which I assumed to be an online store will be based on the feedback or events whether 'selling' action is occurs. Then,a JSON file for a particular item will be feed into the main data. The concern in this method is when multiple people are triggring the event which may overloads the server and then leads to breaking the pipeline. While I am not sure what's the best practice, but supposed breaking the channeling data into several channel will make the system able to handle larger data at a higher cost and while cost is a concern for most business, an appropriate 'projected sales' may needed to built an optimal pipeline.

Platform (JSON) &rarr; Request data for every 'events' - Request API &rarr; Update to main database

## Question 4 - b

Based on Google search, File Transfer Protocol (FTP) are used for the transfer of computer files from a server to a client on a computer network. For example in counting the number of egg produced, a counter will be used to collect the real time data locally and then synchronize into the server. I think the important part in this system is to make sure the data are stable during the synchronize phase such as isolation between ongoing data and synchronize data. This also may offer cheaper option compared to real time data as the velocity is constants.

Digitalization or get farm data via sensor &rarr; Data collection through computer &rarr; Hourly batch file generation in FTP server

## Question 4 - c

Download the excel file from website, extract, transform and lastly feed into the main data. Personally I would just type a script to download it automatically from the website and extract the information as well as deleted the download file. This extra steps will saves a little bit of spaces as excel files will be deleted. While there are services that offer this kind of automation, GitAction offer a free tier services.

Download excel &rarr; Extract file &rarr; Feed to main database
