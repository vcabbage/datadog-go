// Copyright 2013 Ooyala, Inc.

package dogstatsd

import (
	"bytes"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var dogstatsdTests = []struct {
	GlobalNamespace string
	GlobalTags      []string
	Method          func(c *Client, name string, value interface{}, rate float64, tags ...string) error
	Metric          string
	Value           interface{}
	Tags            []string
	Rate            float64
	Expected        string
}{
	{"", nil, gauge, "test.gauge", 1.0, nil, 1.0, "test.gauge:1|g"},
	{"", nil, gauge, "test.gauge", 1.0, nil, 0.999999, "test.gauge:1|g|@0.999999"},
	{"", nil, gauge, "test.gauge", 1.0, []string{"tagA"}, 1.0, "test.gauge:1|g|#tagA"},
	{"", nil, gauge, "test.gauge", 1.0, []string{"tagA", "tagB"}, 1.0, "test.gauge:1|g|#tagA,tagB"},
	{"", nil, gauge, "test.gauge", 1.0, []string{"tagA"}, 0.999999, "test.gauge:1|g|@0.999999|#tagA"},
	{"", nil, count, "test.count", int64(1), []string{"tagA"}, 1.0, "test.count:1|c|#tagA"},
	{"", nil, count, "test.count", int64(-1), []string{"tagA"}, 1.0, "test.count:-1|c|#tagA"},
	{"", nil, histogram, "test.histogram", 2.3, []string{"tagA"}, 1.0, "test.histogram:2.3|h|#tagA"},
	{"", nil, distribution, "test.distribution", 2.3, []string{"tagA"}, 1.0, "test.distribution:2.3|d|#tagA"},
	{"", nil, set, "test.set", "uuid", []string{"tagA"}, 1.0, "test.set:uuid|s|#tagA"},
	{"flubber.", nil, set, "test.set", "uuid", []string{"tagA"}, 1.0, "flubber.test.set:uuid|s|#tagA"},
	{"", []string{"tagC"}, set, "test.set", "uuid", []string{"tagA"}, 1.0, "test.set:uuid|s|#tagC,tagA"},
	{"", nil, count, "test.count", int64(1), []string{"hello\nworld"}, 1.0, "test.count:1|c|#helloworld"},
}

func gauge(c *Client, name string, value interface{}, rate float64, tags ...string) error {
	return c.Gauge(name, value.(float64), rate, tags...)
}
func count(c *Client, name string, value interface{}, rate float64, tags ...string) error {
	return c.Count(name, value.(int64), rate, tags...)
}
func histogram(c *Client, name string, value interface{}, rate float64, tags ...string) error {
	return c.Histogram(name, value.(float64), rate, tags...)
}
func distribution(c *Client, name string, value interface{}, rate float64, tags ...string) error {
	return c.Distribution(name, value.(float64), rate, tags...)
}
func set(c *Client, name string, value interface{}, rate float64, tags ...string) error {
	return c.Set(name, value.(string), rate, tags...)
}

func assertNotPanics(t *testing.T, f func()) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal(r)
		}
	}()
	f()
}

func TestClientUDP(t *testing.T) {
	addr := "localhost:1201"
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}

	server, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client, err := New(ConnAddr(addr))
	if err != nil {
		t.Fatal(err)
	}

	clientTest(t, server, client)
}

type testWriter struct {
	io.WriteCloser
}

func (testWriter) SetWriteTimeout(time.Duration) error { return nil }
func (testWriter) MTU() int                            { return 65535 }

func TestClientWithConn(t *testing.T) {
	server, conn, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	client, err := New(ConnWriter(testWriter{conn}))
	if err != nil {
		t.Fatal(err)
	}

	clientTest(t, server, client)
}

func clientTest(t *testing.T, server io.Reader, client *Client) {
	for _, tt := range dogstatsdTests {
		client.Namespace = tt.GlobalNamespace
		client.Tags = tt.GlobalTags
		err := tt.Method(client, tt.Metric, tt.Value, tt.Rate, tt.Tags...)
		if err != nil {
			t.Fatal(err)
		}

		bytes := make([]byte, 1024)
		n, err := server.Read(bytes)
		if err != nil {
			t.Fatal(err)
		}
		message := bytes[:n]
		if string(message) != tt.Expected {
			t.Errorf("Expected: %q. Actual: %q", tt.Expected, string(message))
		}
	}
}

func TestClientUDS(t *testing.T) {
	dir, err := ioutil.TempDir("", "socket")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	addr := filepath.Join(dir, "dsd.socket")

	udsAddr, err := net.ResolveUnixAddr("unixgram", addr)
	if err != nil {
		t.Fatal(err)
	}

	server, err := net.ListenUnixgram("unixgram", udsAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client, err := New(ConnAddr("unix://" + addr))
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range dogstatsdTests {
		client.Namespace = tt.GlobalNamespace
		client.Tags = tt.GlobalTags
		err := tt.Method(client, tt.Metric, tt.Value, tt.Rate, tt.Tags...)
		if err != nil {
			t.Fatal(err)
		}

		bytes := make([]byte, 1024)
		n, err := server.Read(bytes)
		if err != nil {
			t.Fatal(err)
		}
		message := bytes[:n]
		if string(message) != tt.Expected {
			t.Errorf("Expected: %s. Actual: %s", tt.Expected, string(message))
		}
	}
}

func TestClientUDSClose(t *testing.T) {
	dir, err := ioutil.TempDir("", "socket")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	addr := filepath.Join(dir, "dsd.socket")

	client, err := New(ConnAddr("unix://" + addr))
	if err != nil {
		t.Fatal(err)
	}

	assertNotPanics(t, func() { client.Close() })
}

func TestBufferedClient(t *testing.T) {
	addr := "localhost:1201"
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}

	server, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client, err := New(ConnAddr(addr), ConnBuffer(typicalMTU))
	if err != nil {
		t.Fatal(err)
	}

	client.Namespace = "foo."
	client.Tags = []string{"dd:2"}

	client.Incr("ic", 1)
	client.Decr("dc", 1)
	client.Count("cc", 1, 1)
	client.Gauge("gg", 10, 1)
	client.Histogram("hh", 1, 1)
	client.Distribution("dd", 1, 1)
	client.Timing("tt", 123*time.Microsecond, 1)
	client.Set("ss", "ss", 1)

	var size int
	size += len("foo.ic:1|c|#dd:2")
	size += len("\nfoo.dc:-1|c|#dd:2")
	size += len("\nfoo.cc:1|c|#dd:2")
	size += len("\nfoo.gg:10|g|#dd:2")
	size += len("\nfoo.hh:1|h|#dd:2")
	size += len("\nfoo.dd:1|d|#dd:2")
	size += len("\nfoo.tt:0.123|ms|#dd:2")
	size += len("\nfoo.ss:ss|s|#dd:2")

	if len(client.buffer) != size {
		t.Errorf("Expected client to have buffered %d bytes, but found %d\n", size, len(client.buffer))
	}

	client.Set("ss", "xx", 1)
	client.mu.Lock()
	err = client.flushLocked()
	client.mu.Unlock()
	if err != nil {
		t.Errorf("Error sending: %s", err)
	}

	if len(client.buffer) != 0 {
		t.Errorf("Expecting send to flush buffer, but found %d\n", len(client.buffer))
	}

	buffer := make([]byte, 4096)
	n, err := io.ReadAtLeast(server, buffer, 1)
	result := string(buffer[:n])

	if err != nil {
		t.Error(err)
	}

	expected := []string{
		`foo.ic:1|c|#dd:2`,
		`foo.dc:-1|c|#dd:2`,
		`foo.cc:1|c|#dd:2`,
		`foo.gg:10|g|#dd:2`,
		`foo.hh:1|h|#dd:2`,
		`foo.dd:1|d|#dd:2`,
		`foo.tt:0.123|ms|#dd:2`,
		`foo.ss:ss|s|#dd:2`,
		`foo.ss:xx|s|#dd:2`,
	}

	for i, res := range strings.Split(result, "\n") {
		if res != expected[i] {
			t.Errorf("Got %q, expected %q", res, expected[i])
		}
	}

	evt := Event{Title: "title1", Text: "text1", Priority: Normal, AlertType: Success, Tags: []string{"tagg"}}
	client.Event(evt)
	client.SimpleEvent("event1", "text1")

	{
		evt1, _ := evt.Encode(client.Tags...)
		evt2, _ := NewEvent("event1", "text1").Encode(client.Tags...)
		size = len(evt1)
		size += 1 + len(evt2)
	}

	if len(client.buffer) != size {
		t.Errorf("Expected to find %d bytes, but found %d\n", size, len(client.buffer))
	}

	client.mu.Lock()
	err = client.flushLocked()
	client.mu.Unlock()

	if err != nil {
		t.Errorf("Error sending: %s", err)
	}

	if len(client.buffer) != 0 {
		t.Errorf("Expecting send to flush buffer, but found %d\n", len(client.buffer))
	}

	buffer = make([]byte, 1024)
	n, err = io.ReadAtLeast(server, buffer, 1)
	result = string(buffer[:n])

	if err != nil {
		t.Error(err)
	}

	if n == 0 {
		t.Errorf("Read 0 bytes but expected more.")
	}

	expected = []string{
		`_e{6,5}:title1|text1|p:normal|t:success|#dd:2,tagg`,
		`_e{6,5}:event1|text1|#dd:2`,
	}

	for i, res := range strings.Split(result, "\n") {
		if res != expected[i] {
			t.Errorf("Got %q, expected %q", res, expected[i])
		}
	}

}

func TestBufferedClientBackground(t *testing.T) {
	addr := "localhost:1201"
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}

	server, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	bufferLength := 5
	client, err := New(ConnAddr(addr), ConnBuffer(bufferLength))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.Namespace = "foo."
	client.Tags = []string{"dd:2"}

	client.Count("cc", 1, 1)
	client.Gauge("gg", 10, 1)
	client.Histogram("hh", 1, 1)
	client.Distribution("dd", 1, 1)
	client.Set("ss", "ss", 1)
	client.Set("ss", "xx", 1)

	time.Sleep(client.flushTime * 2)
	client.mu.Lock()
	if len(client.buffer) != 0 {
		t.Errorf("Watch goroutine should have flushed buffer, but found %d\n", len(client.buffer))
	}
	client.mu.Unlock()
}

func TestBufferedClientFlush(t *testing.T) {
	addr := "localhost:1201"
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}

	server, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	bufferLength := 5
	client, err := New(ConnAddr(addr), ConnBuffer(bufferLength))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.Namespace = "foo."
	client.Tags = []string{"dd:2"}

	client.Count("cc", 1, 1)
	client.Gauge("gg", 10, 1)
	client.Histogram("hh", 1, 1)
	client.Distribution("dd", 1, 1)
	client.Set("ss", "ss", 1)
	client.Set("ss", "xx", 1)

	client.Flush()

	client.mu.Lock()
	if len(client.buffer) != 0 {
		t.Errorf("Flush should have flushed buffer, but found %d\n", len(client.buffer))
	}
	client.mu.Unlock()
}

func TestSendMsgUDP(t *testing.T) {
	addr := "localhost:1201"
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}

	server, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client, err := New(ConnAddr(addr))
	if err != nil {
		t.Fatal(err)
	}

	err = client.append(bytes.Repeat([]byte("x"), client.mtu+1))
	if err == nil {
		t.Error("Expected error to be returned if message size is bigger than MTU")
	}

	message := "test message"

	err = client.append([]byte(message))
	if err != nil {
		t.Errorf("Expected no error to be returned if message size is smaller or equal to MTU, got: %s", err.Error())
	}

	buffer := make([]byte, client.mtu+1)
	n, err := io.ReadAtLeast(server, buffer, 1)

	if err != nil {
		t.Fatalf("Expected no error to be returned reading the buffer, got: %s", err.Error())
	}

	if n != len(message) {
		t.Fatalf("Failed to read full message from buffer. Got size `%d` expected `%d`", n, client.mtu)
	}

	if string(buffer[:n]) != message {
		t.Fatalf("The received message did not match what we expect.")
	}

	client, err = New(ConnAddr(addr), ConnBuffer(1))
	if err != nil {
		t.Fatal(err)
	}

	err = client.append(bytes.Repeat([]byte("x"), client.mtu+1))
	if err == nil {
		t.Error("Expected error to be returned if message size is bigger than MTU")
	}

	err = client.append([]byte(message))
	if err != nil {
		t.Errorf("Expected no error to be returned if message size is smaller or equal to MTU, got: %s", err.Error())
	}

	client.mu.Lock()
	err = client.flushLocked()
	client.mu.Unlock()

	if err != nil {
		t.Fatalf("Expected no error to be returned flushing the client, got: %s", err.Error())
	}

	buffer = make([]byte, client.mtu+1)
	n, err = io.ReadAtLeast(server, buffer, 1)

	if err != nil {
		t.Fatalf("Expected no error to be returned reading the buffer, got: %s", err.Error())
	}

	if n != len(message) {
		t.Fatalf("Failed to read full message from buffer. Got size `%d` expected `%d`", n, client.mtu)
	}

	if string(buffer[:n]) != message {
		t.Fatalf("The received message did not match what we expect.")
	}
}

func TestSendUDSErrors(t *testing.T) {
	dir, err := ioutil.TempDir("", "socket")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	message := "test message"

	addr := filepath.Join(dir, "dsd.socket")
	udsAddr, err := net.ResolveUnixAddr("unixgram", addr)
	if err != nil {
		t.Fatal(err)
	}

	client, err := New(ConnAddr("unix://" + addr))
	if err != nil {
		t.Fatal(err)
	}

	// Server not listening yet
	err = client.append([]byte(message))
	if err == nil || !strings.HasSuffix(err.Error(), "no such file or directory") {
		t.Errorf("Expected error \"no such file or directory\", got: %s", err.Error())
	}

	// Start server and send packet
	server, err := net.ListenUnixgram("unixgram", udsAddr)
	if err != nil {
		t.Fatal(err)
	}
	err = client.append([]byte(message))
	if err != nil {
		t.Errorf("Expected no error to be returned when server is listening, got: %s", err.Error())
	}
	bytes := make([]byte, 1024)
	n, err := server.Read(bytes)
	if err != nil {
		t.Fatal(err)
	}
	if string(bytes[:n]) != message {
		t.Errorf("Expected: %s. Actual: %s", string(message), string(bytes))
	}

	// close server and send packet
	server.Close()
	os.Remove(addr)
	err = client.append([]byte(message))
	if err == nil {
		t.Error("Expected an error, got nil")
	}

	// Restart server and send packet
	server, err = net.ListenUnixgram("unixgram", udsAddr)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	defer server.Close()
	err = client.append([]byte(message))
	if err != nil {
		t.Errorf("Expected no error to be returned when server is listening, got: %s", err.Error())
	}

	bytes = make([]byte, 1024)
	n, err = server.Read(bytes)
	if err != nil {
		t.Fatal(err)
	}
	if string(bytes[:n]) != message {
		t.Errorf("Expected: %s. Actual: %s", string(message), string(bytes))
	}
}

func TestSendUDSIgnoreErrors(t *testing.T) {
	client, err := New(ConnAddr("unix:///invalid"))
	if err != nil {
		t.Fatal(err)
	}

	// Default mode throws error
	err = client.append([]byte("message"))
	if err == nil || !strings.HasSuffix(err.Error(), "no such file or directory") {
		t.Errorf("Expected error \"connect: no such file or directory\", got: %s", err.Error())
	}

	// Skip errors
	client.SkipErrors = true
	err = client.append([]byte("message"))
	if err != nil {
		t.Errorf("Expected no error to be returned when in skip errors mode, got: %s", err.Error())
	}
}

func TestNilSafe(t *testing.T) {
	var c *Client
	assertNotPanics(t, func() { c.SetWriteTimeout(0) })
	assertNotPanics(t, func() { c.Flush() })
	assertNotPanics(t, func() { c.Close() })
	assertNotPanics(t, func() { c.Count("", 0, 1) })
	assertNotPanics(t, func() { c.Histogram("", 0, 1) })
	assertNotPanics(t, func() { c.Distribution("", 0, 1) })
	assertNotPanics(t, func() { c.Gauge("", 0, 1) })
	assertNotPanics(t, func() { c.Set("", "", 1) })
	assertNotPanics(t, func() {
		c.send("", "", "", 1)
	})
	assertNotPanics(t, func() { c.Event(NewEvent("", "")) })
	assertNotPanics(t, func() { c.SimpleEvent("", "") })
	assertNotPanics(t, func() { c.ServiceCheck(NewServiceCheck("", Ok)) })
	assertNotPanics(t, func() { c.SimpleServiceCheck("", Ok) })
}

func TestEvents(t *testing.T) {
	matrix := []struct {
		event   Event
		encoded string
	}{
		{
			NewEvent("Hello", "Something happened to my event"),
			`_e{5,30}:Hello|Something happened to my event`,
		}, {
			Event{Title: "hi", Text: "okay", AggregationKey: "foo"},
			`_e{2,4}:hi|okay|k:foo`,
		}, {
			Event{Title: "hi", Text: "okay", AggregationKey: "foo", AlertType: Info},
			`_e{2,4}:hi|okay|k:foo|t:info`,
		}, {
			Event{Title: "hi", Text: "w/e", AlertType: Error, Priority: Normal},
			`_e{2,3}:hi|w/e|p:normal|t:error`,
		}, {
			Event{Title: "hi", Text: "uh", Tags: []string{"host:foo", "app:bar"}},
			`_e{2,2}:hi|uh|#host:foo,app:bar`,
		}, {
			Event{Title: "hi", Text: "line1\nline2", Tags: []string{"hello\nworld"}},
			`_e{2,12}:hi|line1\nline2|#helloworld`,
		},
	}

	for _, m := range matrix {
		r, err := m.event.Encode()
		if err != nil {
			t.Errorf("Error encoding: %s\n", err)
			continue
		}
		if string(r) != m.encoded {
			t.Errorf("Expected %q, got %q\n", m.encoded, r)
		}
	}

	e := NewEvent("", "hi")
	if _, err := e.Encode(); err == nil {
		t.Errorf("Expected error on empty Title.")
	}

	e = NewEvent("hi", "")
	if _, err := e.Encode(); err == nil {
		t.Errorf("Expected error on empty Text.")
	}

	e = NewEvent("hello", "world")
	s, err := e.Encode("tag1", "tag2")
	if err != nil {
		t.Error(err)
	}
	expected := "_e{5,5}:hello|world|#tag1,tag2"
	if string(s) != expected {
		t.Errorf("Expected %s, got %s", expected, s)
	}
	if len(e.Tags) != 0 {
		t.Errorf("Modified event in place illegally.")
	}
}

func TestServiceChecks(t *testing.T) {
	matrix := []struct {
		serviceCheck ServiceCheck
		encoded      string
	}{
		{
			NewServiceCheck("DataCatService", Ok),
			`_sc|DataCatService|0`,
		}, {
			NewServiceCheck("DataCatService", Warn),
			`_sc|DataCatService|1`,
		}, {
			NewServiceCheck("DataCatService", Critical),
			`_sc|DataCatService|2`,
		}, {
			NewServiceCheck("DataCatService", Unknown),
			`_sc|DataCatService|3`,
		}, {
			ServiceCheck{Name: "DataCatService", Status: Ok, Hostname: "DataStation.Cat"},
			`_sc|DataCatService|0|h:DataStation.Cat`,
		}, {
			ServiceCheck{Name: "DataCatService", Status: Ok, Hostname: "DataStation.Cat", Message: "Here goes valuable message"},
			`_sc|DataCatService|0|h:DataStation.Cat|m:Here goes valuable message`,
		}, {
			ServiceCheck{Name: "DataCatService", Status: Ok, Hostname: "DataStation.Cat", Message: "Here are some cyrillic chars: к л м н о п р с т у ф х ц ч ш"},
			`_sc|DataCatService|0|h:DataStation.Cat|m:Here are some cyrillic chars: к л м н о п р с т у ф х ц ч ш`,
		}, {
			ServiceCheck{Name: "DataCatService", Status: Ok, Hostname: "DataStation.Cat", Message: "Here goes valuable message", Tags: []string{"host:foo", "app:bar"}},
			`_sc|DataCatService|0|h:DataStation.Cat|#host:foo,app:bar|m:Here goes valuable message`,
		}, {
			ServiceCheck{Name: "DataCatService", Status: Ok, Hostname: "DataStation.Cat", Message: "Here goes \n that should be escaped", Tags: []string{"host:foo", "app:b\nar"}},
			`_sc|DataCatService|0|h:DataStation.Cat|#host:foo,app:bar|m:Here goes \n that should be escaped`,
		}, {
			ServiceCheck{Name: "DataCatService", Status: Ok, Hostname: "DataStation.Cat", Message: "Here goes m: that should be escaped", Tags: []string{"host:foo", "app:bar"}},
			`_sc|DataCatService|0|h:DataStation.Cat|#host:foo,app:bar|m:Here goes m\: that should be escaped`,
		},
	}

	for _, m := range matrix {
		r, err := m.serviceCheck.Encode()
		if err != nil {
			t.Errorf("Error encoding: %s\n", err)
			continue
		}
		if string(r) != m.encoded {
			t.Errorf("Expected %q, got %q\n", m.encoded, r)
		}
	}

	sc := NewServiceCheck("", Ok)
	if _, err := sc.Encode(); err == nil {
		t.Errorf("Expected error on empty Name.")
	}

	sc = NewServiceCheck("sc", ServiceCheckStatus(5))
	if _, err := sc.Encode(); err == nil {
		t.Errorf("Expected error on invalid status value.")
	}

	sc = NewServiceCheck("hello", Warn)
	s, err := sc.Encode("tag1", "tag2")
	if err != nil {
		t.Error(err)
	}
	expected := "_sc|hello|1|#tag1,tag2"
	if string(s) != expected {
		t.Errorf("Expected %s, got %s", expected, s)
	}
	if len(sc.Tags) != 0 {
		t.Errorf("Modified serviceCheck in place illegally.")
	}
}

func TestFlushOnClose(t *testing.T) {
	client, err := New(ConnAddr("localhost:1201"), ConnBuffer(64))
	if err != nil {
		t.Fatal(err)
	}
	// stop the flushing mechanism so we can test the buffer without interferences
	client.doneOnce.Do(func() { close(client.done) })

	message := "test message"

	err = client.append([]byte(message))
	if err != nil {
		t.Fatal(err)
	}

	if len(client.buffer) != len(message) {
		t.Errorf("Client buffer should contain %d bytes, got %d", len(message), len(client.buffer))
	}

	err = client.Close()
	if err != nil {
		t.Fatal(err)
	}

	if len(client.buffer) != 0 {
		t.Errorf("Client buffer should be empty, got %d", len(client.buffer))
	}
}
