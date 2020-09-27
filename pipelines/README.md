# Pipeline

The data pipeline heavily rely on a message system (in this case SQS) to manage data processing and workflow. This event based approach allow a clear view of what is being processed at any given time and easy recovery in cases of pipeline failure (ie. to reprocess a set of data, simply replay the message into the queue). 

Most of the code here are heaily integrated with business logic and therefore removed, but the integration with SQS remains, as well as various functions to ensure validity of data in pipeline, such as data duplication detection and data batch validation. 