package kvwrapper_etcd

import (
	"context"
	"time"

	"github.com/behance/go-common/kvwrapper"
	log "github.com/behance/go-logging/log"
	etcd "github.com/coreos/etcd/client"
)

// EtcdWrapper wraps the go-etcd client so it can implement the KVWrapper interface
type EtcdWrapper struct {
	kapi etcd.KeysAPI
}

// NewKVWrapper returns a new kvwrapper_etcd as a KVWrapper
func (e EtcdWrapper) NewKVWrapper(servers []string, username, password string) kvwrapper.KVWrapper {
	config := etcd.Config{
		Endpoints: servers,
		Transport: etcd.DefaultTransport,
		Username:  username,
		Password:  password,
	}
	client, err := etcd.New(config)
	if err != nil {
		// even though this is a critical error, wedon't want to issue log.Fatal, since that would os.Exit(1) from within the lib
		log.Warn("Could not instantiate etcd V2 client.", "err", err)
		return nil
	}
	return EtcdWrapper{kapi: etcd.NewKeysAPI(client)}
}

// Set sets the key = val with a ttl of ttl. If key is a path, it will be created.
func (e EtcdWrapper) Set(key string, val string, ttl uint64) error {
	options := &etcd.SetOptions{
		TTL: time.Duration(ttl) * time.Second,
	}
	_, err := e.kapi.Set(context.Background(), key, val, options)
	if err != nil {
		log.Warn("Could not set key in etcd.", "key", key, "err", err)
		return err
	}
	return nil
}

// GetVal returns a single KeyValue found at key
func (e EtcdWrapper) GetVal(key string) (*kvwrapper.KeyValue, error) {
	options := &etcd.GetOptions{
		Sort:      false,
		Recursive: false,
	}
	r, err := e.kapi.Get(context.Background(), key, options)
	if err != nil {
		if etcd.IsKeyNotFound(err) {
			return nil, kvwrapper.ErrKeyNotFound
		}
		log.Warn("Could not retrieve key from etcd.", "key", key, "err", err)
		return nil, err
	}
	kv := &kvwrapper.KeyValue{
		Key:         key,
		Value:       r.Node.Value,
		HasChildren: r.Node.Dir,
	}
	return kv, nil
}

// GetList returns a []KeyValue found at key
func (e EtcdWrapper) GetList(key string, sort bool) ([]*kvwrapper.KeyValue, error) {
	options := &etcd.GetOptions{
		Sort:      sort,
		Recursive: true,
	}
	r, err := e.kapi.Get(context.Background(), key, options)
	if err != nil {
		if etcd.IsKeyNotFound(err) {
			return nil, kvwrapper.ErrKeyNotFound
		}
		log.Warn("Could not retrieve key from etcd.", "key", key, "err", err)
		return nil, err
	}
	kvs := make([]*kvwrapper.KeyValue, 0)
	for i := 0; i < r.Node.Nodes.Len(); i++ {
		kv := &kvwrapper.KeyValue{
			Key:         r.Node.Nodes[i].Key,
			Value:       r.Node.Nodes[i].Value,
			HasChildren: r.Node.Nodes[i].Dir,
		}
		kvs = append(kvs, kv)
	}
	return kvs, nil
}
