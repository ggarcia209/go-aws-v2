# V1
V1 supports the following services:
- DynamoDb
- S3
- SQS
- SNS
- SES

The V1 package has been deprecated and will no longer be maintained. Please use the latest V2 release.

V1 lacks proper test coverage but has been integrated into application backends and demonstrated reliable, robust performance. Test all integrations thorougly, use at own risk


## DynamoDB
go-dynamo is a project in development to implement wrapper functions around the AWS DynamoDB APIs for ease of use. 
It is intended to to enable any Go struct to be added as a DynamoDB Table item by passing the structs as interfaces 
to the wrapper functions. 

A new DynamoDB session must be initialized using locally stored AWS credentials before the functions can be called.
Tables can be created with user-defined primary & sort key names and types by using the Table object. Tables can also be deleted.
- Note: Secondary indexes are not supported at this time.

Items are read/wrote from/to the table by passing the struct object(s) and the Table object representing the DB table to the corresponding functions.

This project is open-source and may the code may be used according to the Apache License.
