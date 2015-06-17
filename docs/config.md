## Configuration

#### Default Config

~~~TML

###
### Redis Database
###
[redis]
  #host = "localhost:6379,localhost:6380,localhost:6381"
  host = "localhost:6379"
  sentinel = false
  #auth = "2aaiua43u2bfb2"
  key-prefix = "reduxdb:"

###
### Controls logging
###
[log]
  enabled = true
  level = "info"
  log = /var/log/redux.log
  
###
### Controls the enforcement of retention policies for evicting data.
###
[retention]
  check-interval = "5s"

###
### Controls the continuous query manager.
###
[continuous_queries]
  compute-interval = "5s"

###
### Controls how the HTTP endpoints are configured.
###
[http]
  enabled = false
  port = "8086"
  
###
### Controls how the TCP endpoints are configured.
###
[tcp]
  enabled = true
  port = "6086"
~~~