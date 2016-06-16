package kvwrapper_test

import (
	. "github.com/behance/go-common/kvwrapper"
	log "github.com/behance/go-common/log"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Kvwrapper", func() {
	var (
		kv KVWrapper
	)

	BeforeEach(func() {
		log.SetLevel(log.PanicLevel)
		kv = NewKVWrapper([]string{"http://localhost:2379"}, KVFaker{})
		kv.Set("parent/child1", "child1val", 0)
		kv.Set("parent/child2", "child2val", 0)
	})
	Describe("Get Wrapper", func() {
		It("Is initialized properly", func() {
			Expect(kv).ToNot(BeNil())
		})
		It("Handles invalid values", func() {
			s, err := kv.GetVal("xxxxxxxx")
			Expect(err).To(MatchError(ErrKeyNotFound))
			Expect(s).To(BeNil())

			l, err := kv.GetList("xxxxxxxx/phpinfo", false)
			Expect(err).To(MatchError(ErrKeyNotFound))
			Expect(len(l)).To(Equal(0))
		})
		It("Handles valid values", func() {
			l, err := kv.GetList("parent/", false)
			Expect(err).To(BeNil())
			Expect(len(l)).To(Equal(2))
		})
	})

	Describe("Set Wrapper", func() {
		It("Sets and gets values", func() {
			err := kv.Set("a/test/path", "value", 0)
			Expect(err).To(BeNil())

			s, err := kv.GetVal("a/test/")
			Expect(s.HasChildren).To(Equal(true))

			s, err = kv.GetVal("a/test/path")
			Expect(s.Value).To(Equal("value"))
		})
	})

})
