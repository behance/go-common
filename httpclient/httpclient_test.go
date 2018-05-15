package httpclient

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("httpclient.go functions", func() {

	Describe("It works", func() {
		It("It tries 4 times if the HTTP return code is 5xx and then succeeds", func() {
			reqNum := 1
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if reqNum < 4 {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte("Some temporary error occurred"))
				} else {
					w.WriteHeader(http.StatusNoContent)
				}
				reqNum++
			}))

			defer ts.Close()

			httpCfg := Config{
				HTTPProtocol:       "http",
				AllowUnverifiedTLS: true,
				NumTry:             4,
			}

			var client = httpCfg.GetHTTPClient()
			resp, err := client.Get(ts.URL)

			if err != nil {
				fmt.Println(err.Error())
			}

			Expect(err).ToNot(HaveOccurred())
			Expect(reqNum).To(Equal(5))
			Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
		})

	})

	Describe("Should not retry", func() {

		It("If the HTTP server times out without returning anything", func() {
			reqNum := 1
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reqNum++
			}))

			defer ts.Close()

			httpCfg := Config{
				HTTPProtocol:       "http",
				AllowUnverifiedTLS: true,
				NumTry:             3,
			}

			var client = httpCfg.GetHTTPClient()
			client.Get(ts.URL)

			Expect(reqNum).To(Equal(2))

		})

		It("If the TCP server times out without returning anything", func() {

			const (
				connHost = "127.0.0.1"
				connPost = "3333"
				connType = "tcp"
			)

			ln, err := net.Listen(connType, connHost+":"+connPost)
			if err != nil {
				fmt.Println("Error listening:", err.Error())
			}

			var conn net.Conn
			reqNum := 1

			go func() {

				defer ln.Close()
				conn, err = ln.Accept()
				if conn != nil {
					reqNum++
				}
			}()

			httpCfg := Config{
				HTTPProtocol:       "http",
				AllowUnverifiedTLS: true,
				NumTry:             3,
			}

			var httpClient = httpCfg.GetHTTPClient()
			_, err = httpClient.Get("http://" + connHost + ":" + connPost)

			if err != nil {
				fmt.Println(err)
			}

			Expect(reqNum).To(Equal(2))
			Expect(err).To(HaveOccurred())

		})
	})

	Describe("Test getHTTPTransport method", func() {
		It("Pass AllowUnverifiedTLS=true and ensure that InsecureSkipVerify is true ", func() {
			httpCfg := Config{
				HTTPProtocol:       "https",
				AllowUnverifiedTLS: true,
				NumTry:             4,
			}

			roundTripper := httpCfg.getHTTPTransport()
			Expect(roundTripper.TLSClientConfig.InsecureSkipVerify).To(BeTrue())
		})
	})

	Describe("GetHTTPSSEClient", func() {
		It("Check if timeout is zero", func() {
			httpCfg := Config{
				HTTPProtocol:       "https",
				AllowUnverifiedTLS: true,
				NumTry:             4,
			}

			expectedTimeout := 0 * time.Second

			httpSSEClient := httpCfg.GetHTTPSSEClient()
			Expect(httpSSEClient.Timeout).To(Equal(expectedTimeout))
		})
	})
})
