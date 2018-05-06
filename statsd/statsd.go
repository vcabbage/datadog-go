// Copyright 2013 Ooyala, Inc.

/*
Package statsd provides a Go dogstatsd client. Dogstatsd extends the popular statsd,
adding tags and histograms and pushing upstream to Datadog.

Refer to http://docs.datadoghq.com/guides/dogstatsd/ for information about DogStatsD.

Example Usage:

    // Create the client
    c, err := statsd.New("127.0.0.1:8125")
    if err != nil {
        log.Fatal(err)
    }
    // Prefix every metric with the app name
    c.Namespace = "flubber."
    // Send the EC2 availability zone as a tag with every metric
    c.Tags = append(c.Tags, "us-east-1a")
    err = c.Gauge("request.duration", 1.2, nil, 1)

statsd is based on go-statsd-client.
*/
package statsd

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

// OptimalPayloadSize defines the optimal payload size for a UDP datagram, 1432 bytes
// is optimal for regular networks with an MTU of 1500 so datagrams don't get
// fragmented. It's generally recommended not to fragment UDP datagrams as losing
// a single fragment will cause the entire datagram to be lost.
//
// This can be increased if your network has a greater MTU or you don't mind UDP
// datagrams getting fragmented. The practical limit is MaxUDPPayloadSize
const OptimalPayloadSize = 1432

// MaxUDPPayloadSize defines the maximum payload size for a UDP datagram.
// Its value comes from the calculation: 65535 bytes Max UDP datagram size -
// 8byte UDP header - 60byte max IP headers
// any number greater than that will see frames being cut out.
const MaxUDPPayloadSize = 65467

// UnixAddressPrefix holds the prefix to use to enable Unix Domain Socket
// traffic instead of UDP.
const UnixAddressPrefix = "unix://"

// Stat suffixes
const (
	gaugeSuffix        = "|g"
	countSuffix        = "|c"
	histogramSuffix    = "|h"
	distributionSuffix = "|d"
	decrSuffix         = "-1|c"
	incrSuffix         = "1|c"
	setSuffix          = "|s"
	timingSuffix       = "|ms"
)

// A Writer offers a standard interface regardless of the underlying
// protocol. For now UDS and UPD writers are available.
type Writer interface {
	Write(data []byte) (n int, err error)
	SetWriteTimeout(time.Duration) error
	Close() error
}

// A Client is a handle for sending messages to dogstatsd.  It is safe to
// use one Client from multiple goroutines simultaneously.
type Client struct {
	addr string

	// Writer handles the underlying networking protocol
	writer Writer
	// Namespace to prepend to all statsd calls
	Namespace string
	// Tags are global tags to be added to every statsd call
	Tags []string
	// skipErrors turns off error passing and allows UDS to emulate UDP behaviour
	SkipErrors bool

	// BufferLength is the length of the buffer in commands.
	bufferLength int

	flushTime time.Duration

	doneOnce sync.Once
	done     chan struct{}

	mu       sync.Mutex
	buffer   []byte
	commands int
}

func (c *Client) Clone() (*Client, error) {
	wr, err := newWriter(c.addr)
	if err != nil {
		return nil, err
	}
	s := &Client{
		writer:       wr,
		Namespace:    c.Namespace,
		Tags:         c.Tags,
		SkipErrors:   c.SkipErrors,
		bufferLength: c.bufferLength,
		flushTime:    c.flushTime,
		done:         make(chan struct{}),
	}
	if s.flushTime > 0 {
		go s.watch()
	}

	return s, nil
}

// New returns a pointer to a new Client given an addr in the format "hostname:port" or
// "unix:///path/to/socket".
func New(addr string) (*Client, error) {
	w, err := newWriter(addr)
	return &Client{addr: addr, writer: w}, err
}

// NewWithWriter creates a new Client with given writer. Writer is a
// io.WriteCloser + SetWriteTimeout(time.Duration) error
func NewWithWriter(w Writer) *Client {
	return &Client{writer: w}
}

// NewBuffered returns a Client that buffers its output and sends it in chunks.
// Buflen is the length of the buffer in number of commands.
func NewBuffered(addr string, bufLen int) (*Client, error) {
	if bufLen == 1 {
		return New(addr)
	}

	w, err := newWriter(addr)
	if err != nil {
		return nil, err
	}

	c := &Client{
		addr:         addr,
		writer:       w,
		bufferLength: bufLen,
		flushTime:    100 * time.Millisecond,
		done:         make(chan struct{}),
	}
	go c.watch()

	return c, nil
}

func newWriter(addr string) (Writer, error) {
	if strings.HasPrefix(addr, UnixAddressPrefix) {
		return newUdsWriter(addr[len(UnixAddressPrefix)-1:])
	}
	return newUDPWriter(addr)
}

// format a message from its name, value, tags and rate. Also adds global namespace and tags.
func (c *Client) appendStat(name string, value interface{}, suffix string, rate float64, tags ...string) error {
	// preallocated buffer, stack allocated as long as it doesn't escape
	buf := make([]byte, 0, 200)

	if c.Namespace != "" {
		buf = append(buf, c.Namespace...)
	}
	buf = append(buf, name...)
	buf = append(buf, ':')

	switch val := value.(type) {
	case float64:
		buf = strconv.AppendFloat(buf, val, 'f', 6, 64)

	case int64:
		buf = strconv.AppendInt(buf, val, 10)

	case string:
		buf = append(buf, val...)

	default:
		// do nothing
	}
	buf = append(buf, suffix...)

	if rate < 1 {
		buf = append(buf, "|@"...)
		buf = strconv.AppendFloat(buf, rate, 'f', -1, 64)
	}

	buf = appendTagString(buf, c.Tags, tags)

	return c.append(buf)
}

func (c *Client) append(buf []byte) error {
	// return an error if message is bigger than MaxUDPPayloadSize
	if len(buf) > MaxUDPPayloadSize {
		return errors.New("message size exceeds MaxUDPPayloadSize")
	}

	c.mu.Lock()
	// check if MaxUDPPayloadSize reached
	if len(c.buffer)+len(buf)+1 > MaxUDPPayloadSize {
		err := c.flushLocked(true)
		if err != nil {
			return err
		}
	}

	// add to buffer
	if c.commands > 0 {
		c.buffer = append(c.buffer, '\n')
	}
	c.buffer = append(c.buffer, buf...)
	c.commands++
	err := c.flushLocked(false)
	c.mu.Unlock()

	return err
}

// SetWriteTimeout allows the user to set a custom UDS write timeout. Not supported for UDP.
func (c *Client) SetWriteTimeout(d time.Duration) error {
	if c == nil {
		return nil
	}
	return c.writer.SetWriteTimeout(d)
}

func (c *Client) watch() {
	ticker := time.NewTicker(c.flushTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			// FIXME: eating error here
			c.flushLocked(true)
			c.mu.Unlock()
		case <-c.done:
			return
		}
	}
}

// Flush forces a flush of the pending commands in the buffer
func (c *Client) Flush() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushLocked(true)
}

// flush the commands in the buffer.
//
// c.mu must be held by caller.
func (c *Client) flushLocked(force bool) error {
	if !force && (c.commands == 0 || c.commands < c.bufferLength) {
		return nil
	}

	_, err := c.writer.Write(c.buffer)
	c.buffer = c.buffer[:0]
	c.commands = 0

	if c.SkipErrors {
		return nil
	}
	return err
}

// send handles sampling and sends the message over UDP. It also adds global namespace prefixes and tags.
func (c *Client) send(name string, value interface{}, suffix string, rate float64, tags ...string) error {
	if c == nil {
		return nil
	}
	if rate < 1 && rand.Float64() > rate {
		return nil
	}
	return c.appendStat(name, value, suffix, rate, tags...)
}

// Gauge measures the value of a metric at a particular time.
func (c *Client) Gauge(name string, value float64, rate float64, tags ...string) error {
	return c.send(name, value, gaugeSuffix, rate, tags...)
}

// Count tracks how many times something happened per second.
func (c *Client) Count(name string, value int64, rate float64, tags ...string) error {
	return c.send(name, value, countSuffix, rate, tags...)
}

// Histogram tracks the statistical distribution of a set of values on each host.
func (c *Client) Histogram(name string, value float64, rate float64, tags ...string) error {
	return c.send(name, value, histogramSuffix, rate, tags...)
}

// Distribution tracks the statistical distribution of a set of values across your infrastructure.
func (c *Client) Distribution(name string, value float64, rate float64, tags ...string) error {
	return c.send(name, value, distributionSuffix, rate, tags...)
}

// Decr is just Count of -1
func (c *Client) Decr(name string, rate float64, tags ...string) error {
	return c.send(name, nil, decrSuffix, rate, tags...)
}

// Incr is just Count of 1
func (c *Client) Incr(name string, rate float64, tags ...string) error {
	return c.send(name, nil, incrSuffix, rate, tags...)
}

// Set counts the number of unique elements in a group.
func (c *Client) Set(name string, value string, rate float64, tags ...string) error {
	return c.send(name, value, setSuffix, rate, tags...)
}

// Timing sends timing information, it is an alias for TimeInMilliseconds
func (c *Client) Timing(name string, value time.Duration, rate float64, tags ...string) error {
	return c.TimeInMilliseconds(name, value.Seconds()*1000, rate, tags...)
}

// TimeInMilliseconds sends timing information in milliseconds.
// It is flushed by statsd with percentiles, mean and other info (https://github.com/etsy/statsd/blob/master/docs/metric_types.md#timing)
func (c *Client) TimeInMilliseconds(name string, value float64, rate float64, tags ...string) error {
	return c.send(name, value, timingSuffix, rate, tags...)
}

// Event sends the provided Event.
func (c *Client) Event(e Event) error {
	if c == nil {
		return nil
	}
	stat, err := e.Encode(c.Tags...)
	if err != nil {
		return err
	}

	return c.append(stat)
}

// SimpleEvent sends an event with the provided title and text.
func (c *Client) SimpleEvent(title, text string) error {
	return c.Event(NewEvent(title, text))
}

// ServiceCheck sends the provided ServiceCheck.
func (c *Client) ServiceCheck(sc ServiceCheck) error {
	if c == nil {
		return nil
	}
	stat, err := sc.Encode(c.Tags...)
	if err != nil {
		return err
	}

	return c.append(stat)
}

// SimpleServiceCheck sends an serviceCheck with the provided name and status.
func (c *Client) SimpleServiceCheck(name string, status ServiceCheckStatus) error {
	return c.ServiceCheck(NewServiceCheck(name, status))
}

// Close the client connection.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	if c.done != nil {
		c.doneOnce.Do(func() { close(c.done) })
	}

	// if this client is buffered, flush before closing the writer
	if c.bufferLength > 0 {
		if err := c.Flush(); err != nil {
			return err
		}
	}

	return c.writer.Close()
}

// Events support
// EventAlertType and EventAlertPriority became exported types after this issue was submitted: https://github.com/DataDog/datadog-go/issues/41
// The reason why they got exported is so that client code can directly use the types.

// EventAlertType is the alert type for events
type EventAlertType string

// Event alert types
const (
	Info    EventAlertType = "info"
	Error   EventAlertType = "error"
	Warning EventAlertType = "warning"
	Success EventAlertType = "success"
)

// EventPriority is the event priority for events
type EventPriority string

// Event Priorities
const (
	Normal EventPriority = "normal"
	Low    EventPriority = "low"
)

// An Event is an object that can be posted to your DataDog event stream.
type Event struct {
	// Title of the event. Required.
	Title string

	// Text is the description of the event. Required.
	Text string

	// Timestamp is a timestamp for the event. If not provided, the dogstatsd
	// server will set this to the current time.
	Timestamp time.Time

	// Hostname for the event.
	Hostname string

	// AggregationKey groups this event with others of the same key.
	AggregationKey string

	// Priority of the event. Can be statsd.Low or statsd.Normal.
	Priority EventPriority

	// SourceTypeName is a source type for the event.
	SourceTypeName string

	// AlertType can be statsd.Info, statsd.Error, statsd.Warning, or statsd.Success.
	// If absent, the default value applied by the dogstatsd server is Info.
	AlertType EventAlertType

	// Tags for the event.
	Tags []string
}

// NewEvent creates a new event with the given title and text.  Error checking
// against these values is done at send-time, or upon running e.Check.
func NewEvent(title, text string) Event {
	return Event{
		Title: title,
		Text:  text,
	}
}

// Check verifies that an event is valid.
func (e Event) Check() error {
	if len(e.Title) == 0 {
		return fmt.Errorf("statsd.Event title is required")
	}
	if len(e.Text) == 0 {
		return fmt.Errorf("statsd.Event text is required")
	}
	return nil
}

// Encode returns the dogstatsd wire protocol representation for an event.
// Tags may be passed which will be added to the encoded output but not to
// the Event's list of tags, eg. for default tags.
func (e Event) Encode(tags ...string) ([]byte, error) {
	err := e.Check()
	if err != nil {
		return nil, err
	}
	text := strings.Replace(e.Text, "\n", "\\n", -1)

	buf := make([]byte, 0, 200)
	buf = append(buf, "_e{"...)
	buf = strconv.AppendInt(buf, int64(len(e.Title)), 10)
	buf = append(buf, ',')
	buf = strconv.AppendInt(buf, int64(len(text)), 10)
	buf = append(buf, "}:"...)
	buf = append(buf, e.Title...)
	buf = append(buf, '|')
	buf = append(buf, text...)

	if !e.Timestamp.IsZero() {
		buf = append(buf, "|d:"...)
		buf = strconv.AppendInt(buf, e.Timestamp.Unix(), 10)
	}

	if len(e.Hostname) != 0 {
		buf = append(buf, "|h:"...)
		buf = append(buf, e.Hostname...)
	}

	if len(e.AggregationKey) != 0 {
		buf = append(buf, "|k:"...)
		buf = append(buf, e.AggregationKey...)
	}

	if len(e.Priority) != 0 {
		buf = append(buf, "|p:"...)
		buf = append(buf, string(e.Priority)...)
	}

	if len(e.SourceTypeName) != 0 {
		buf = append(buf, "|s:"...)
		buf = append(buf, e.SourceTypeName...)
	}

	if len(e.AlertType) != 0 {
		buf = append(buf, "|t:"...)
		buf = append(buf, string(e.AlertType)...)
	}

	buf = appendTagString(buf, tags, e.Tags)

	return buf, nil
}

// ServiceCheckStatus support
type ServiceCheckStatus byte

// Service check statuses.
const (
	Ok       ServiceCheckStatus = 0
	Warn     ServiceCheckStatus = 1
	Critical ServiceCheckStatus = 2
	Unknown  ServiceCheckStatus = 3
)

// An ServiceCheck is an object that contains status of DataDog service check.
type ServiceCheck struct {
	// Name of the service check. Required.
	Name string

	// Status of service check. Required.
	Status ServiceCheckStatus

	// Timestamp is a timestamp for the serviceCheck. If not provided, the dogstatsd
	// server will set this to the current time.
	Timestamp time.Time

	// Hostname for the serviceCheck.
	Hostname string

	// A message describing the current state of the serviceCheck.
	Message string

	// Tags for the serviceCheck.
	Tags []string
}

// NewServiceCheck creates a new serviceCheck with the given name and status. Error checking
// against these values is done at send-time, or upon running sc.Check.
func NewServiceCheck(name string, status ServiceCheckStatus) ServiceCheck {
	return ServiceCheck{
		Name:   name,
		Status: status,
	}
}

// Check verifies that an event is valid.
func (sc ServiceCheck) Check() error {
	if len(sc.Name) == 0 {
		return fmt.Errorf("statsd.ServiceCheck name is required")
	}
	if sc.Status > 3 {
		return fmt.Errorf("statsd.ServiceCheck status has invalid value")
	}
	return nil
}

// Encode returns the dogstatsd wire protocol representation for an serviceCheck.
// Tags may be passed which will be added to the encoded output but not to
// the Event's list of tags, eg. for default tags.
func (sc ServiceCheck) Encode(tags ...string) ([]byte, error) {
	err := sc.Check()
	if err != nil {
		return nil, err
	}

	msg := strings.Replace(sc.Message, "\n", "\\n", -1)
	msg = strings.Replace(msg, "m:", `m\:`, -1)

	buf := make([]byte, 0, 200)
	buf = append(buf, "_sc|"...)
	buf = append(buf, sc.Name...)
	buf = append(buf, '|')
	buf = strconv.AppendInt(buf, int64(sc.Status), 10)

	if !sc.Timestamp.IsZero() {
		buf = append(buf, "|d:"...)
		buf = strconv.AppendInt(buf, sc.Timestamp.Unix(), 10)
	}

	if len(sc.Hostname) != 0 {
		buf = append(buf, "|h:"...)
		buf = append(buf, sc.Hostname...)
	}

	buf = appendTagString(buf, tags, sc.Tags)

	if len(msg) != 0 {
		buf = append(buf, "|m:"...)
		buf = append(buf, msg...)
	}

	return buf, nil
}

func appendTagString(buf []byte, tagList1, tagList2 []string) []byte {
	if len(tagList1) == 0 {
		if len(tagList2) == 0 {
			return buf
		}
		tagList1 = tagList2
		tagList2 = nil
	}

	buf = append(buf, "|#"...)
	buf = appendWithoutNewlines(buf, tagList1[0])
	for _, tag := range tagList1[1:] {
		buf = append(buf, ',')
		buf = appendWithoutNewlines(buf, tag)
	}
	for _, tag := range tagList2 {
		buf = append(buf, ',')
		buf = appendWithoutNewlines(buf, tag)
	}
	return buf
}

func appendWithoutNewlines(buf []byte, s string) []byte {
	// fastpath for strings without newlines
	if strings.IndexByte(s, '\n') == -1 {
		return append(buf, s...)
	}

	for _, b := range []byte(s) {
		if b != '\n' {
			buf = append(buf, b)
		}
	}
	return buf
}
