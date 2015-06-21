## Interfaces

You can connect to the server in the following ways:

#### HTTP

There is a web api which is enabled by default.

You can send commands by sending JSON data to endpoints matching the standard command types:

##### Writing Data

 * POST http://localhost:8084/series/{:series_name}/data
    * Write data to a series

  * GET http://localhost:8084/series/{:series_name}/data
    * Read data from a series

  * DELETE http://localhost:8084/series/{:series_name}/data
  * POST http://localhost:8084/series/{:series_name}/data/delete
    * Delete data from a series


##### Series

  * GET http://localhost:8084/series?filter=my_series*
    * List all series - optionally filter with a glob style pattern

  * GET http://localhost:8084/series/{:series_name}
      * List information about a series

  * DELETE http://localhost:8084/series/{:series_name}
    * Drop a series


##### Retention Policies

  * GET http://localhost:8084/retention
    * List all retention policies

  * GET http://localhost:8084/retention/{:policy_name}
        * List information about a retention policy

  * POST http://localhost:8084/retention/{:policy_name}
    * Create a retention policy

  * DELETE http://localhost:8084/retention/{:policy_name}
    * Delete a retention policy


##### Retention Policies

  * GET http://localhost:8084/query
    * List all continuous queries

  * GET http://localhost:8084/query/{:query_name}
    * List information about a query policy

  * POST http://localhost:8084/query/{:query_name}
    * Create a continuous query

  * DELETE http://localhost:8084/query/{:query_name}
    * Delete a continuous query


##### Server Status

  * http://localhost:8084/status