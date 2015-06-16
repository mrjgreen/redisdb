## Interfaces

You can connect to the server in the following ways:

#### HTTP

There is a web api which is enabled by default.

You can send commands by sending JSON data to endpoints matching the standard command types:
 
  * http://localhost:8084/write
  * http://localhost:8084/delete
  * http://localhost:8084/read
  * http://localhost:8084/stream
  * http://localhost:8084/status
  
  
#### TCP

There is a tcp api which is enabled by default.

You can open a TCP socket to localhost:6578 and JSON send commands in the following format:

  * write {"name" : "events", "values" : {...}, "time" : unixstamp.12112312}
  * delete {"name" : "events", "tags" : {...}}
  * read  {"name" : "events", "between" : {...}}
  * stream {"drop" : ["events"]}
  * status