## Stack Overflow questions datapipeline

### Project description
This data pipeline pulls data from 2 datasource:
1. Stack Overflow questions data
2. Stack Overflow question tags data

The first dataset describe following information about **Stack Overflow Questions**:
- CreationDate: Date and time when the question is posted on Stack Overflow
- ClosedDate: Date and time when the question is closed by creator or by Stack Overflow admin/moderator
- DeletionDate: Date and time when the question is deleted by creator or by Stack Overflow admin/moderator
- Score: Value of score of each question, scoring by Stack Overflow users
- OwnerUserId: User ID of the question's creator
- AnswerCount: Total number of answers in each question

The second dataset describes value of **tag** of each question. A tag describes the topic of that question is asking about. One question can have more than one tag.

From two above datasets, the pipeline will create 2 fact tables and 2 dim tables as following:
1. dimDate: List out all date value exists in the dataset
2. dimTag: List out all unique value of **tag**
3. factQuestion: List out all questions from time to time in Stack Overflow
4. factQuestionTag: List out all tag values of each question in factQuestion table

Evaluation of tech stack selection:
1. MWAA (Amazon Managed Workflow for Apache Airflow): This is a fully managed service of Airflow on AWS. It supports scaling out and scaling in seamlessly in case of large datasets and complex calculations 
2. S3: An object storage without limitation on how large of the objects. We can use it as a data lake in many scenario
3. RedShift: A managed data warehouse with ability of elastically scale out and scale in to adapt increase of connections, data size or users

Schedule of running:
- This dataset is coming from Stack Overflow data source which is used worldwide. So this pipeline might pull data from a staging data source which is usually updated daily. Propose this data pipeline run daily at 1:00AM UTC.

### 1. Data sources:

2 data sources:

1. questions.csv
2. question_tags.csv
- Sample data of questions.csv

![question_raw_table](https://github.com/antonizero99/SO-questions-aws-pipeline/raw/master/images/questions_raw.jpg)

- Sample data of question_tags.csv

![question_raw_table](https://github.com/antonizero99/SO-questions-aws-pipeline/raw/master/images/questions_tag_raw.jpg)

- questions.csv profiling
![question_profiling](https://github.com/antonizero99/SO-questions-aws-pipeline/raw/master/images/questions_profiling.jpg)
~17.2 million records

- question_tags.csv profiling
![question_tags_profiling](https://github.com/antonizero99/SO-questions-aws-pipeline/raw/master/images/question_tags_profiling.jpg)
~50.5 million records

Data source description: [Kaggle StackLite](https://www.kaggle.com/stackoverflow/stacklite)

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

#### Data Dictionary:
| Table | Column | Data type | Description |
| ------ | ------ | ------ | ------ |
| dimTag | TagID | Number | Primary key of tag |
| dimTag | Tag | Text | Tag text value |
| ------ | ------ | ------ | ------ |
| dimDate | Date | Date | Primary key, all date data in **questions** dataset |
| dimDate | Day | Number | Value of day in date column |
| dimDate | Week | Number | Value of week in date column |
| dimDate | Month | Number | Value of month in date column |
| dimDate | Quarter | Number | Value of quarter in date column |
| dimDate | Year | Number | Value of year in date column |
| ------ | ------ | ------ | ------ |
| factQuestion | ID | Number | Primary key, ID of question |
| factQuestion | OwnerUserID | Number | ID of owner user of the question |
| factQuestion | Score | Number | Value of score of the question |
| factQuestion | AnswerCount | Number | Total number of answer the the question |
| factQuestion | Status | Text | Current status of the question (Closed, Open, Deleted) |
| factQuestion | CreationDateTime | Datetime | Date and time when the question is created |
| factQuestion | CreationDate | Date | Date when the question is created |
| factQuestion | ClosedDateTime | Datetime | Date and time when the question is closed |
| factQuestion | ClosedDate | Date | Date when the question is closed |
| factQuestion | DeletionDateTime | Datetime | Date and time when the question is deleted |
| factQuestion | DeletionDate | Date | Date when the question is deleted |
| factQuestion | Hash_key | Number | Hash value of all column, this column is used to data change tracking |
| ------ | ------ | ------ | ------ |
| factQuestionTag | QuestionID | Number | Foreign key, ID of question |
| factQuestion | TagID | Number | Foreign key, ID of the tag value |
| factQuestion | Hash_key | Number | Hash value of all column, this column is used to data change tracking |

### 5. Scenarios
1. The data was increased by 100x
- This data pipeline is hosted in AWS. Specifically MWAA (Managed Airflow service), S3 and RedShift. When the data scale up to 100x, we can adapt that change quickly by scale out our cluster in RedShift and MWAA.
- Consider using EMR and Spark to process big data
2. The pipelines would be run on a daily basis by 7 am every day
- We can config this requirement by adjusting dag **schedule_interval** argument
3. The database needed to be accessed by 100+ people
- Similar as scenario 1, we can scale out the cluster of RedShift to meeting that requirement about large number of users

### 6. Final notes
- Hash_key columns are created for future use, incremental load is not implemented in this version
- Pushing data into S3 authenticated by secret key due to lack of AWS permission
- To get the project run in development, only first 1000,000 rows used in the data pipeline