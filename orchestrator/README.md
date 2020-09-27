# Airflow

While there are a number of ways to manage schedule jobs (eg. basic crontabs, Kubernetes Cron Jobs), Airflow is well regarded as one of the the most customizable and reliable method to orchestrate these tasks in the data engineering field. Jobs are defined as dags located within the *dags* folder and additional scheduled job specific utilities functions can be found here as well.

Most of the jobs are heavily integrated with business logic and removed, but the structure of the project remains and could be helpful in creating new projects. The Apache Airflow project also have a number of example dags availble in their official GitHub repo. 

For a full feature list of Airflow see http://airflow.apache.org/. But personally, the ability to easily backfill or repeat historical jobs and monitor performance of tasks over time have been a huge help in improving the reliability and identifying the weaknesses in existing code. 

For the most part, the instance hosting Airflow itself does not do much work in each given job, rather the processing rely on various AWS serverless services, such as Athena, Fargate, Lambda, etc. This significantly reduces the cost of data processing as Airflow itself can be hosted within a fairly small instance and serverless services only incur cost when used, the total cost is often significantly cheaper than hosting persistent data processing applications in large instances. 