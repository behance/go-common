[![Build Status](https://travis-ci.org/behance/go-common.svg?branch=master)](https://travis-ci.org/behance/go-common)
[![Coverage Status](https://coveralls.io/repos/github/behance/go-common/badge.svg?branch=travis-ci)](https://coveralls.io/github/behance/go-common?branch=travis-ci)

# go-common

# Features

* KV-Wrapper wraps the go-etcd client so it can implement the KVWrapper interface
* KVWrapper is an interface that any Key Value Store (etcd, consul) needs to implement when used by flight director.
* KVWrapper currently supports etcd-v2 and partially supports etcd-v3, limited to the existing interface
* In particular, the limitation means that there is no way to modify a key/value with a ttl, to remove the ttl completely.  In v2 that happens by modifying the ttl value to 0; however, v3 handles ttl, under the hood, with leases, and the initial implementation doesn't keep track of the leases. It is important to note, that there is no existinguse case that depends on this capability.
* Log is a wrapper for go-logrus forked from [logrus](https://github.com/Sirupsen/logrus) It serves 2 main purposes:
  - It eliminates the need for awkward .WithFields calls by intelligently creating fields
  based on the number and positions of parameters to the Warn, Error, Fatal and Info calls.
  - It adds stack info to ever call for easier debugging
* HTTPClient with retries and exponential backoff
