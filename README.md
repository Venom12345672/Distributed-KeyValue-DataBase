# Distributed-KeyValue-DataBase


## Description
A distributed key value database where we have a centeralized server which can read two commands from client "put" ande "get" command

## Commands Description
### put command
When client sends a put command, it will send key,value pair as well and server will put it in its database.
### get command
When client sends a get command, it will send key as well and server in reply will return it corresponding latest value.

Server will also allow concurent read writes. which is handled all perfectly in the project.

##Technologies Used
Go-Lang
