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
    }
    // Can be omitted to return all results
    "between" : {
        "start" : "now", // omit or 0 for start
        "end" : "now" // omit or -1 for end
    },
    //"group" : true, // to group entire result set
    "group" : ["campaign"] // to group on a column or set of columns
}
~~~


Result:
~~~JSON
[
    {
        "values" : {
            "sumvalue" : 100,
            "countvalue" : 10,
            "campaign" : 55
        },
        "id" : "287ifh2362h2",
        "time" : unixstamp.12112312
    },
    {
        "values" : {
            "sumvalue" : 100,
            "countvalue" : 10,
            "campaign" : 56
        },
        "id" : "287ifh2362h2",
        "time" : unixstamp.12112312
    }
]
~~~

Empty Result or Query Against Missing Stream:
~~~JSON
[ ]
~~~