package kvwrapper_etcd_v3

import (
	"context"
	"strings"
	"time"

	"github.com/behance/go-common/kvwrapper"
	log "github.com/behance/go-logging/log"
	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
)

// EtcdWrapper wraps the go-etcd client so it can implement the KVWrapper interface
type EtcdV3Wrapper struct {
	kapi etcdv3.KV
	cli  etcdv3.Client
}

// NewKVWrapper returns a new kvwrapper_etcd as a KVWrapper
func (e EtcdV3Wrapper) NewKVWrapper(servers []string, username, password string) kvwrapper.KVWrapper {
	config := etcdv3.Config{
		Endpoints:   servers,
		Username:    username,
		Password:    password,
		DialTimeout: 5 * time.Second,
	}
	client, err := etcdv3.New(config)
	if err != nil {
		// even though this is a critical error, wedon't want to issue log.Fatal, since that would os.Exit(1) from within the lib
		log.Warn("Could not instantiate etcd V3 client.", "err", err)
		return nil
	}

	return EtcdV3Wrapper{kapi: etcdv3.NewKV(client), cli: *client}
}

// Set sets the key = val with a ttl of ttl. If key is a path, it will be created.
// The ttl will be handled via a lease
// Leases are given in increments of seconds, and leases are granted in increments of seconds,
// hence there is no need to convert to time.Duration (which is in nanoseconds) as is done in version 2
// note! The API assumes that a ttl of 0, implies a wish that the key/value pair not expire
// implementing that behavior in v3, by not acquiring a lease
func (e EtcdV3Wrapper) Set(key string, val string, ttl uint64) error {
	// acquire a lease to which we'll attach the new key/value pair
	// lease.grant takes an int64 as the ttl, hence casting is necessary
	log.Debug("called Set with key/value: ", key, val, " and ttl ", ttl)
	if ttl == 0 {
		_, put_err := e.kapi.Put(context.Background(), key, val)
		if put_err != nil {
			log.Warn("Could not set key in etcd.", "key", key, "err", put_err)
			return put_err
		}
	} else {
		lease, lease_err := e.cli.Grant(context.Background(), int64(ttl))
		if lease_err != nil {
			log.Fatal(lease_err)
			return lease_err
		}

		log.Debug("go-common: Set - got lease", lease.ID, " with a ttl of ", lease.TTL)
		// Insert key with a lease of ttl second TTL
		_, put_err := e.kapi.Put(context.Background(), key, val, etcdv3.WithLease(lease.ID))
		if put_err != nil {
			log.Warn("Could not set key in etcd.", "key", key, "err", put_err)
			// if the Put op failed, clean up the lease
			_, revoke_err := e.cli.Revoke(context.Background(), lease.ID)
			if revoke_err != nil {
				log.Warn("Attempt to revoke lease failed with error ", revoke_err, " for lease.ID ", lease.ID)
			}
			return put_err
		}
	}
	return nil
}

// GetVal returns a single KeyValue found at key
func (e EtcdV3Wrapper) GetVal(key string) (*kvwrapper.KeyValue, error) {
	// by default no sorting nor range expansion is performed
	log.Debug("entering GetVal with key ", key)

	r, err := e.kapi.Get(context.Background(), key)
	if err != nil {
		if err == context.Canceled {
			log.Warn("Context Cancelled could not retrieve key ", key, " receiving error ", err)
			return nil, err
			// else if handles case grpc.ErrClientConnClosing by checking if message contains "client"
		} else if strings.Contains(err.Error(), "client") {
			log.Warn("Client Connection Closed - could not retrieve key ", key, " receiving error ", err)
			return nil, err
		}
	}
	if err != nil || r.Kvs == nil || len(r.Kvs) < 1 {
		log.Info("could not retrieve key ", key, " receiving error ", err)
		if err == rpctypes.ErrKeyNotFound || r.Kvs == nil || len(r.Kvs) == 0 {
			return nil, kvwrapper.ErrKeyNotFound
		}
		log.Warn("Could not retrieve key from etcd.", "key", key, "err", err)
		return nil, err
	}

	kv := &kvwrapper.KeyValue{
		Key:         key,
		Value:       string(r.Kvs[0].Value),
		HasChildren: false,
	}
	return kv, nil
}

// GetList returns a[]KeyValue found whose keys all begin with key as prefix
func (e EtcdV3Wrapper) GetList(key string, sort bool) ([]*kvwrapper.KeyValue, error) {
	options := []etcdv3.OpOption{
		etcdv3.WithSort(etcdv3.SortByKey, etcdv3.SortAscend),
		etcdv3.WithPrefix(),
	}
	r, err := e.kapi.Get(context.Background(), key, options...)
	if err != nil {
		if err == rpctypes.ErrKeyNotFound {
			return nil, kvwrapper.ErrKeyNotFound
		}
		log.Warn("Could not retrieve key from etcd.", "key", key, "err", err)
		return nil, err
	}
	kvs := make([]*kvwrapper.KeyValue, 0)
	num_kv := len(r.Kvs)
	for i := 0; i < num_kv; i++ {
		kv := &kvwrapper.KeyValue{
			Key:         string(r.Kvs[i].Key),
			Value:       string(r.Kvs[i].Value),
			HasChildren: false,
		}
		kvs = append(kvs, kv)
	}
	return kvs, nil
}

// delete individual key
// is candidate to be added to interface
func Delete(e EtcdV3Wrapper, key string) error {
	// by default no sorting nor range expansion is performed
	r, err := e.kapi.Delete(context.Background(), key)
	if err != nil {
		if err == rpctypes.ErrKeyNotFound {
			return kvwrapper.ErrKeyNotFound
		}
		log.Warn("Could not delete key from etcd.", "key", key, "err", err)
		return err
	} else if r.Deleted == 0 {
		return kvwrapper.ErrKeyNotFound
	}

	return nil
}

// delete all keys beginning with this prefix
// returns the number of key/value pairs that were deleted
// is candidate to be added to interface
func DeleteList(e EtcdV3Wrapper, key string) (int64, error) {
	options := []etcdv3.OpOption{
		etcdv3.WithPrefix(),
	}
	// by default no sorting nor range expansion is performed
	r, err := e.kapi.Delete(context.Background(), key, options...)
	if err != nil {
		if err == rpctypes.ErrKeyNotFound {
			return r.Deleted, kvwrapper.ErrKeyNotFound
		}
		log.Warn("Could not delete key from etcd.", "key", key, "err", err)
		return r.Deleted, err
	} else if r.Deleted == 0 {
		return r.Deleted, kvwrapper.ErrKeyNotFound
	}

	return r.Deleted, nil
}
