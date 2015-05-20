package kvwrapper_etcd

import (
	"github.com/behance/go-common/kvwrapper"
	"github.com/coreos/go-etcd/etcd"
	"strings"

	log "github.com/behance/go-common/log"
)

// EtcdWrapper wraps the go-etcd client so it can implement the KVWrapper interface
type EtcdWrapper struct {
	client *etcd.Client
}

// NewKVWrapper returns a new kvwrapper_etcd as a KVWrapper
func (e EtcdWrapper) NewKVWrapper(servers []string) kvwrapper.KVWrapper {
	e.client = etcd.NewClient(servers)
	return e
}

//Set stes the key = val with a ttl of ttl. If key is a path, it will be created.
func (e EtcdWrapper) Set(key string, val string, ttl uint64) error {
	_, err := e.client.Set(key, val, ttl)
	if err != nil {
		return err
	}
	return nil
}

// GetVal returns a single KeyValue found at key
func (e EtcdWrapper) GetVal(key string) (*kvwrapper.KeyValue, error) {
	r, err := e.client.Get(key, false, true)
	if err != nil {
		log.Warn("Could not retrieve key from etcd.", "key", key, "error", err)
		if strings.HasPrefix(err.Error(), "501:") {
			return nil, kvwrapper.ErrCouldNotConnect
		} else if strings.HasPrefix(err.Error(), "100:") {
			return nil, kvwrapper.ErrKeyNotFound
		} else {
			return nil, err
		}
	}
	kv := &kvwrapper.KeyValue{
		Key:         key,
		Value:       r.Node.Value,
		HasChildren: r.Node.Dir,
	}
	return kv, nil
}

// GetVal returns a []KeyValue found at key
func (e EtcdWrapper) GetList(key string, sort bool) ([]*kvwrapper.KeyValue, error) {
	r, err := e.client.Get(key, sort, true)
	if err != nil {
		log.Warn("Could not retrieve key from etcd.", "key", key, "error", err)
		if strings.HasPrefix(err.Error(), "501:") {
			return nil, kvwrapper.ErrCouldNotConnect
		} else if strings.HasPrefix(err.Error(), "100:") {
			return nil, kvwrapper.ErrKeyNotFound
		} else {
			return nil, err
		}
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
