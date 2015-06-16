## Configuration

#### Default Config

~~~TML

###
### Redis Database
###
[redis]
  host = "localhost:6379,localhost:6380,localhost:6381"
  sentinel = false
  auth = "2aaiua43u2bfb2"
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
  check-interval = "10m"

###
### Controls the continuous query manager.
###
[continuous_queries]
  compute-interval = "10s"

###
### Controls how the HTTP endpoints are configured.
###
[http]
  enabled = false
  bind-address = ":8086"
  
###
### Controls how the TCP endpoints are configured.
###
[tcp]
  enabled = true
  bind-address = ":6086"
~~~