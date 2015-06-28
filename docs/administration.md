## Stream Administration

#### Get Status

To list the status of the server send a "<status>" command with data in the following format:


#### List Streams

To list all streams send an "/stream" command with no data

#### Drop a Stream

To remove a stream a stream send a "<stream>" command with data in the following format:

~~~JSON
{
    "drop" : [
        "events"
    ]
}
~~~

 > NOTE: This cannot be undone

#### Set Retention Time on Streams

Data from a stream can be automatically removed once the timestamp is older than a configured age.

To set a retention time on a stream send an "<stream>" command with data in the following format:

~~~JSON
{
    "retention" : {
        "events" : "10m"
    }
}
~~~
