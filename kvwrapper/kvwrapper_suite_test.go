package kvwrapper_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestKvwrapper(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kvwrapper Suite")
}
