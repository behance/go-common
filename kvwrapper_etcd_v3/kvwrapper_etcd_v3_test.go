package kvwrapper_etcd_v3

// package kvwrapper_etcd_v3
import (
	"fmt"
	"os"
	"testing"

	"github.com/behance/go-common/kvwrapper"
	"github.com/behance/go-common/log"
)

func TestGetSingle(t *testing.T) {
	if os.Getenv("KV_ETCD_LOCALHOST") == "" {
		t.Skip("skipping test; $KV_ETCD_LOCALHOST not set")
	}
	hosts := []string{"http://localhost:2379"}
	kvw := kvwrapper.NewKVWrapperWithAuth(
		hosts,
		EtcdV3Wrapper{},
		"", // cfg.KVUsername,
		"", // cfg.KVPassword,
	)

	set_err := kvw.Set("Foo", "Bar", 30)
	if set_err != nil {
		log.Warn("Set failed for ", "key", "Foo", "Value", "Bar", " With error: ", set_err)
		t.Error("Failed to create Foo:Bar as Key:Value pair")
		return
	}
	kv_pair, get_err := kvw.GetVal("Foo")
	if get_err != nil {
		log.Warn("Expected to get", "Bar", " But got error: ", get_err)
		t.Error("failed to retrieve newly created Foo:Bar key:value pair")
		return
	}
	if kv_pair.Value != "Bar" {
		log.Warn("We expected to get ", "Bar", " but we got: ", kv_pair.Value)
		t.Error("Expected Bar, got ", kv_pair.Value)
		return
	}
	set_err = kvw.Set("FooShortTTL", "Bar", 0)
	if set_err != nil {
		log.Warn("Set failed for ", "key", "FooShortTTL", "Value", "Bar", " With error: ", set_err)
		t.Error("Failed to create FooShortTTL:Bar as Key:Value pair")
		return
	}
	kv_pair, get_err = kvw.GetVal("FooShortTTL")
	if get_err != nil {
		log.Warn("Expected to get", "Bar", " But got error: ", get_err)
		t.Error("failed to retrieve newly created FooShortTTL:Bar key:value pair")
		return
	}
	kv_pair, get_err = kvw.GetVal("FooFoo")
	if get_err != nil {
		log.Info("Expected to get an error because key FooFoo doesn't exist ", "And got error: ", get_err)
	}
	if kv_pair != nil && kv_pair.Value != "" {
		log.Warn("We expected to get nil", " but we got: ", kv_pair.Value)
		t.Error("Expected nil, got ", kv_pair.Value)
		return
	}
}

func TestGetMultiple(t *testing.T) {
	if os.Getenv("KV_ETCD_LOCALHOST") == "" {
		t.Skip("skipping test; $KV_ETCD_LOCALHOST not set")
	}
	hosts := []string{"http://localhost:2379"}
	kvw := EtcdV3Wrapper.NewKVWrapper(EtcdV3Wrapper{}, hosts, "", "")

	set_err := kvw.Set("Foo/1", "Bar/1", 30)
	if set_err != nil {
		log.Warn("Set failed for ", "key", "Foo/2", "Value", "Bar/2", " With error: ", set_err)
		t.Error("Failed to add Foo/1:Bar/1 as key:value pair")
		return
	}
	set_err = kvw.Set("Foo/2", "Bar/2", 30)
	if set_err != nil {
		log.Warn("Set failed for ", "key", "Foo/2", "Value", "Bar/2", " With error: ", set_err)
		t.Error("Failed to add Foo/2:Bar/2 as key:value pair")
		return
	}
	set_err = kvw.Set("Foo/3", "Bar/3", 30)
	if set_err != nil {
		log.Warn("Set failed for ", "key", "Foo/3", "Value", "Bar/3", " With error: ", set_err)
		t.Error("Failed to add Foo/3:Bar/3 as key:value pair")
		return
	}

	kv_pairs, get_err := kvw.GetList("Foo/", false)
	if get_err != nil || len(kv_pairs) != 3 {
		log.Warn("Expected to get 3 key/value pairs but got ", len(kv_pairs), " error: ", get_err)
		t.Error("Expected 3 entries, got none")
		return
	}

	for _, ev := range kv_pairs {
		log.Debug(fmt.Sprintf("%s : %s\n", ev.Key, ev.Value))
	}

}

func TestDelSingle(t *testing.T) {
	if os.Getenv("KV_ETCD_LOCALHOST") == "" {
		t.Skip("skipping test; $KV_ETCD_LOCALHOST not set")
	}
	hosts := []string{"http://localhost:2379"}
	kvw := EtcdV3Wrapper.NewKVWrapper(EtcdV3Wrapper{}, hosts, "", "")

	kv_pairs, get_err := kvw.GetList("", false)
	num_entries := 0
	if get_err != nil && get_err != kvwrapper.ErrKeyNotFound {
		log.Warn("GetList failed for to retrieve anything with error: ", get_err)
	} else {
		num_entries = len(kv_pairs)
	}
	if num_entries == 0 {
		del_err := Delete(kvw.(EtcdV3Wrapper), "Foo")
		if del_err == nil || del_err != kvwrapper.ErrKeyNotFound {
			log.Warn("Expected to get error that key isn't found but got ", del_err)
			t.Error("delete of Foo succeeded even though it wasn't supposed to be in key/value store")
			return
		}
	}

	set_err := kvw.Set("Foo", "Bar", 30)
	if set_err != nil {
		log.Warn("Set failed for ", "key", "Foo", "Value", "Bar", " With error: ", set_err)
		return
	}

	del_err := Delete(kvw.(EtcdV3Wrapper), "Foo")
	if del_err != nil {
		log.Warn("Expected delete of key Foo to succeed but got error: ", del_err)
		t.Error("Delete of Foo failed")
		return
	} else {
		log.Debug("Deleted key Foo")
	}

}

func TestDelMultiple(t *testing.T) {
	if os.Getenv("KV_ETCD_LOCALHOST") == "" {
		t.Skip("skipping test; $KV_ETCD_LOCALHOST not set")
	}
	hosts := []string{"http://localhost:2379"}
	kvw := EtcdV3Wrapper.NewKVWrapper(EtcdV3Wrapper{}, hosts, "", "")

	kv_pairs, get_err := kvw.GetList("", false)
	var num_entries int64
	num_entries = 0
	if get_err != nil && get_err != kvwrapper.ErrKeyNotFound {
		log.Warn("GetList failed for to retrieve anything with error: ", get_err)
	} else {
		num_entries = int64(len(kv_pairs))
	}
	set_err := kvw.Set("/DelMult/Foo/1", "Bar/1", 30)
	if set_err != nil {
		log.Warn("Set failed for ", "key", "/DelMult/Foo/1", "Value", "Bar/2", " With error: ", set_err)
		return
	}
	set_err = kvw.Set("/DelMult/Foo/2", "Bar/2", 30)
	if set_err != nil {
		log.Warn("Set failed for ", "key", "/DelMult/Foo/2", "Value", "Bar/2", " With error: ", set_err)
		return
	}
	set_err = kvw.Set("/DelMult/Foo/3", "Bar/3", 30)
	if set_err != nil {
		log.Warn("Set failed for ", "key", "/DelMultFoo/3", "Value", "Bar/3", " With error: ", set_err)
		return
	}

	kv_pairs, get_err = kvw.GetList("/DelMult", false)
	if get_err != nil || len(kv_pairs) != 3 {
		num_ent := 0
		if kv_pairs != nil {
			num_ent = len(kv_pairs)
		}
		log.Warn("Expected to get 3 key/value pairs but got ", num_ent, " error: ", get_err)
		t.Error("Expected 3 entries, got ", num_ent)
		return
	} else {
		num_entries = int64(len(kv_pairs))
	}

	for _, ev := range kv_pairs {
		log.Debug(fmt.Sprintf("%s : %s\n", ev.Key, ev.Value))
	}
	num_dels, del_err := DeleteList(kvw.(EtcdV3Wrapper), "/DelMult")
	log.Debug("delete list returns: ", num_dels, " as number of elements deleted")
	if del_err != nil || num_dels != num_entries {
		log.Warn("Expected to get ", num_entries, " but got ", num_dels, " entries and error ", del_err)
		t.Error("didn't delete expected number of entries")
	} else {
		log.Debug("Deleted ", num_entries)
	}

}
