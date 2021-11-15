## Stack Overflow questions datapipeline
### 1. Data sources:
[Kaggle StackLite](https://www.kaggle.com/stackoverflow/stacklite)

There are 2 main tables:
1. questions.csv
2. question_tags.csv
- Sample data of questions.csv

![question_raw_table](https://github.com/antonizero99/SO-questions-aws-pipeline/raw/master/images/questions_raw.jpg)

- Sample data of question_tags.csv

question_tag_raw.jpg
### 2. Overview architecture

![architecture](https://github.com/antonizero99/SO-questions-aws-pipeline/raw/master/images/architecture.jpg)

- Data files are downloaded by calling Google API on HTTPS protocol.
- Raw data is stored as zip file in Airflow cluster storage.
- Raw data and processed data are pushed to S3 bucket
- Fact and Dim tables are copied to Redshift as predefined data model

### 3. Data flow processing:
1. Download data as zip files and store in cluster storage
2. Adding hash key column in the raw table, this column is a hashing number of combination of all columns in each row. This column is used to track changes of data sources to implement incremental load

![question_raw_table_with_hash](https://github.com/antonizero99/SO-questions-aws-pipeline/raw/master/images/questions_raw_hash.jpg)
![question_tag_raw_table_with_hash](https://github.com/antonizero99/SO-questions-aws-pipeline/raw/master/images/questions_tag_raw_hash.jpg)

3. Create dimDate table by combining 3 datetime columns in **questions** data source
4. Update table factQuestion by adding date-only columns, status column
5. Create dimTag table by extracting and de-duplicating tag data in **question_tags** data source
6. Update factQuestionTag by joining with dimTag and get their according ID as foreign key

### 4. Target data model

![erd](https://github.com/antonizero99/SO-questions-aws-pipeline/raw/master/images/erd.jpg)

### 5. Final notes
- Hash_key columns are created for future use, incremental load is not implemented in this version
- Pushing data into S3 authenticated by secret key due to lack of AWS permission
- To get the project run in development, only first 10,000 rows used in the data pipeline