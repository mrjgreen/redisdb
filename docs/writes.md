## Writes

#### Add Data to a Stream

Streams do not need to be explicitly created

To add data to a stream send a "<write command>" with data in the following format:

~~~JSON
{
    "name" : "events",
    "values" : {
        "value" : 10
    },
    "tags" : {
        "campaign" : 55
        "event" : 211
    },
    "time" : unixstamp.12112312
}
~~~

You will receive a response containing an array of IDs for each inserted row

#### Update an Existing Row in a Stream

Stream data can be updated by sending an "<update command>" with data in the following format, where the id is the id received from an insert:

~~~JSON
{
    "name" : "events",
    "id" : "cuihvuna23feasd",
    "values" : {
        "value" : 10
    },
    "tags" : {
        "campaign" : 55
        "event" : 211
    },
    "time" : unixstamp.12112312
}
~~~


#### Delete Data from a Stream

To delete data from a stream send a "<delete command>" with data in the following format:

**By ID:**
~~~JSON
{
    "name" : "events",
    "id" : [
        "123vfav3",
        "287if2h2",
        "h23634fs"
    ]
}
~~~

**By Tags:**
~~~JSON
{
    "name" : "events",
    "tags" : {
        "campaign" : [55]
        "event" : [211]
    }
}
~~~

**By Range:**
~~~JSON
{
    "name" : "events",
    "between" : {
        "start" : "unixstamp.2324234", // omit or 0 for start
        "end" : "unixstamp.3422325" // omit or -1 for end
    },
}
~~~


**Combination:**
~~~JSON
{
    "name" : "events",
    "tags" : {
        "campaign" : [55,35,353]
        "event" : [211,201,203]
    },
    "between" : {
        "start" : "unixstamp.2324234", // omit or 0 for start
        "end" : "unixstamp.3422325" // omit or -1 for end
    },
}
~~~