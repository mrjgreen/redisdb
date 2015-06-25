## Writes

#### Add Data to a Stream

Streams do not need to be explicitly created

To add data to a stream send a "/write/seriesname" command with data in the following format:

~~~JSON
{
    "value" : 10,
    "campaign" : 55,
    "event" : 211
    "time" : unixstamp.12112312 // Optionally leave out to use current time
}
~~~

You will receive a response containing an array of IDs for each inserted row


#### Delete Data from a Stream

To delete data from a stream send a "/delete/series" with data in the following format:

**By Range:**
~~~JSON
{
    "between" : {
        "start" : "unixstamp.2324234", // omit or 0 for start
        "end" : "unixstamp.3422325" // omit or -1 for end
    },
    "where" : {
        "something"
    }
}
~~~