package integration_test

import (
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
)

// A TCP proxy allows real broker connections to be interrupted without
// requiring Docker socket access inside the test container.
type brokerProxy struct {
	listener    net.Listener
	target      string
	uri         url.URL
	mu          sync.Mutex
	paused      bool
	connections map[net.Conn]struct{}
	wg          sync.WaitGroup
}

// newBrokerProxy starts a TCP proxy to the broker and registers cleanup so tests can
// interrupt real connections without controlling Docker.
func newBrokerProxy(t *testing.T, address string) *brokerProxy {
	t.Helper()
	u, err := url.Parse(address)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &brokerProxy{listener: listener, target: u.Host, uri: *u, connections: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.paused {
				p.mu.Unlock()
				_ = client.Close()
				continue
			}
			p.connections[client] = struct{}{}
			p.mu.Unlock()
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				defer func() { _ = client.Close(); p.mu.Lock(); delete(p.connections, client); p.mu.Unlock() }()
				upstream, err := net.Dial("tcp", p.target)
				if err != nil {
					return
				}
				defer upstream.Close()
				copied := make(chan struct{})
				go func() { _, _ = io.Copy(upstream, client); _ = upstream.Close(); close(copied) }()
				_, _ = io.Copy(client, upstream)
				_ = client.Close()
				<-copied
			}()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); p.pause(); p.wg.Wait() })
	return p
}

// URL returns the original broker URL with the proxy's address substituted, preserving
// credentials and other URL settings.
func (p *brokerProxy) URL() string {
	u := p.uri
	u.Host = p.listener.Addr().String()
	return u.String()
}

// pause rejects new proxy connections and closes active connections to simulate a broker
// outage.
func (p *brokerProxy) pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	for conn := range p.connections {
		_ = conn.Close()
	}
}

// resume allows new connections through the proxy so broker clients can reconnect.
func (p *brokerProxy) resume() { p.mu.Lock(); p.paused = false; p.mu.Unlock() }
