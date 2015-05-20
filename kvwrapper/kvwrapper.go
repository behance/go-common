package kvwrapper

import (
	"errors"
	"strconv"
	"strings"
)

var (
	ErrKeyNotFound     = errors.New("Key not found")
	ErrCouldNotConnect = errors.New("Could not connect to KV store")
)

// KVWrapper is an interface that any Key Value Store (etcd, consul) needs to implement
// when used by flight director.
type KVWrapper interface {
	NewKVWrapper(servers []string) KVWrapper
	Set(key string, val string, ttl uint64) error
	GetVal(key string) (*KeyValue, error)
	GetList(key string, sort bool) ([]*KeyValue, error)
}

// KeyValue entity represents the unit returned by queries to a Key Value store.
type KeyValue struct {
	Key         string
	Value       string
	HasChildren bool
}

func (kv *KeyValue) String() string {
	return kv.Key + " : " + kv.Value + " : " + strconv.FormatBool(kv.HasChildren)
}

// NewKVWrapper takes a list of server urls and an empty specifc wrapper (like kvwrapper_etcd)
// and returns an initialized instance of KVWrapper
func NewKVWrapper(servers []string, wrapper KVWrapper) KVWrapper {
	kvw := wrapper.NewKVWrapper(servers)
	return kvw
}

type KVFaker struct {
	c map[string][]*KeyValue
}

func (f KVFaker) NewKVWrapper(servers []string) KVWrapper {
	f.c = make(map[string][]*KeyValue)
	return f
}
func (f KVFaker) Set(key string, val string, ttl uint64) error {
	keys := strings.Split(key, "/")
	lastKey := ""
	var newKey string
	for i := 0; i < len(keys); i++ {
		newKey = newKey + keys[i]
		if _, ok := f.c[newKey]; !ok {
			f.c[newKey] = make([]*KeyValue, 0)
		}
		kv := KeyValue{
			Key:         newKey,
			Value:       val,
			HasChildren: false,
		}
		if i < len(keys)-1 {
			//kv.Value = ""
			kv.HasChildren = true
			if lastKey != "" {
				f.c[lastKey] = append(f.c[lastKey], &kv)
			}
		} else {
			kvlast := KeyValue{
				Key:         newKey,
				Value:       val,
				HasChildren: true,
			}
			f.c[lastKey] = append(f.c[lastKey], &kvlast)
			f.c[newKey] = append(f.c[newKey], &kv)
		}
		newKey += "/"
		lastKey = newKey

	}
	//log.Warn(f.c)
	return nil
}
func (f KVFaker) GetVal(key string) (*KeyValue, error) {
	if kv, ok := f.c[key]; ok {
		return kv[len(kv)-1], nil
	} else {
		return nil, ErrKeyNotFound
	}
}
func (f KVFaker) GetList(key string, sort bool) ([]*KeyValue, error) {
	if _, ok := f.c[key]; !ok {
		return nil, ErrKeyNotFound
	}

	return f.c[key], nil
}
