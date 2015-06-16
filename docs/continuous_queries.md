## Continuous Queries

A continuous query will be ran on an interval to

#### List Queries

To list all continuous queries send a "<query>" command with no data:

#### Create a Query

To create a continuous queries send a standard read command "<query>"

~~~JSON
{
    "query" : "events_10min_211_c55", // The result will be stored here
    "name" : "events",
    "values" : {
        "sumvalue" : "SUM(value)"
        "distinct_values" : "COUNT(value)"
        "count" : "COUNT()" // No count field required to get a full count of all rows
    },
    "tags" : {
        "campaign" : [55]
        "event" : [211]
    },
    // Can be omitted to return all results
    "between" : {
        "start" : "now", // omit or 0 for start
        "end" : "now" // omit or -1 for end
    },
    "group" : {
        "time" : "10m", // E.g 30s 15m 6h 30d 12w - if omitted the create will fail
        "tags" : ["campaign"]
    }
}
~~~

#### Drop a Query

To remove a continuous query send a "<query>" command with data in the following format:

~~~JSON
{
    "drop" : [
        "events_10min",
        "events_1hr",
    ]
}
~~~

 > NOTE: This cannot be undone