package httpclient

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/PuerkitoBio/rehttp"
)

// Config contains various config params for http client
type Config struct {
	HTTPProtocol       string
	AllowUnverifiedTLS bool
	NumTry             int
	// If left blank, it gets it from environment
	ProxyURL string
}

func (httpCfg Config) getHTTPTransport() *http.Transport {
	var roundTripper = &http.Transport{}

	if httpCfg.AllowUnverifiedTLS {
		roundTripper.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	if httpCfg.ProxyURL == "" {
		roundTripper.Proxy = http.ProxyFromEnvironment
	}

	return roundTripper
}

// GetHTTPClient provides an HTTP client with retries and exponential backoff, if enabled
func (httpCfg Config) GetHTTPClient() *http.Client {

	var timeout = 10 * time.Second

	var rehttpTransport = rehttp.NewTransport(
		httpCfg.getHTTPTransport(),
		// retries if httpCfg.NumTry is more than 1 and  error code is between 500 to 599
		rehttp.RetryAll(rehttp.RetryMaxRetries(httpCfg.NumTry), rehttp.RetryStatusInterval(500, 600)),
		// delay between 0 and base * 2^attempt capped at max (an exponential backoff delay with jitter).
		// here base is 1 and max is capped at 5 minutes
		// jitter algorithm used: http://www.awsarchitectureblog.com/2015/03/backoff.html
		rehttp.ExpJitterDelay(1, 300*time.Second),
	)

	return &http.Client{
		Timeout:   timeout,
		Transport: rehttpTransport,
	}
}

// GetHTTPSSEClient provides an HTTP client with retries and exponential backoff, if enabled
func (httpCfg Config) GetHTTPSSEClient() *http.Client {
	httpClient := httpCfg.GetHTTPClient()
	httpClient.Timeout = 0
	return httpClient
}
