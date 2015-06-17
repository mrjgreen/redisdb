## Reads

#### Query

To read data from a stream send a "<read command>" with data in the following format:

Query:
~~~JSON
{
    "name" : "events",
    "values" : {
        "sumvalue" : "SUM(value)"
        "distinct_values" : "COUNT(value)"
        "count" : "COUNT()" // No count field required to get a full count of all rows
        "campaign" : "campaign"
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
        "time" : "10m", // E.g 30s 15m 6h 30d 12w
        "values" : ["campaign"]
    }
}
~~~


Result:
~~~JSON
[
    {
        "values" : {
            "sumvalue" : 100
            "countvalue" : 10
        },
        "id" : "287ifh2362h2",
        "time" : unixstamp.12112312,
        "tags" : {
            "campaign" : 55 // Tags from the group by
        },
    }
]
~~~

Empty Result or Query Against Missing Stream:
~~~JSON
[ ]
~~~