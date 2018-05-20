// Copyright 2013 Ooyala, Inc.

/*
Package dogstatsd provides a dogstatsd client. Dogstatsd extends the popular statsd,
adding tags and histograms and pushing upstream to Datadog.

Refer to http://docs.datadoghq.com/guides/dogstatsd/ for information about DogStatsD.
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

const (
	// DefaultAddr is used if no writer or address is provided.
	DefaultAddr = "127.0.0.1:8125"

	// DefaultFlushTimeout is default duration between flushing the buffer to
	// the server when buffering is enabled.
	DefaultFlushTimeout = 100 * time.Millisecond
)

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
	MTU() int
	Close() error
}

// A Client is a handle for sending messages to dogstatsd.  It is safe to
// use one Client from multiple goroutines simultaneously.
type Client struct {
	opts []ConnOpt

	// Writer handles the underlying networking protocol
	writer Writer
	// Namespace to prepend to all statsd calls
	Namespace string
	// Tags are global tags to be added to every statsd call
	Tags []string
	// skipErrors turns off error passing and allows UDS to emulate UDP behaviour
	SkipErrors bool

	maxBuffer int
	mtu       int

	flushTime time.Duration

	doneOnce sync.Once
	done     chan struct{}

	mu     sync.Mutex
	buffer []byte
	timer  *time.Timer
}

// New returns a new Client.
//
// If no options are provided, metrics will be sent unbuffered to 127.0.0.1:8125.
func New(opts ...ConnOpt) (*Client, error) {
	c := &Client{
		opts:      opts,
		flushTime: DefaultFlushTimeout,
		done:      make(chan struct{}),
	}

	for _, opt := range opts {
		err := opt(c)
		if err != nil {
			return nil, err
		}
	}

	if c.writer == nil {
		err := ConnAddr(DefaultAddr)(c)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

// ConnOpt is a function for configuring a dogstatsd Client.
type ConnOpt func(*Client) error

// ConnAddr configures the address to send metrics to.
func ConnAddr(addr string) ConnOpt {
	return func(c *Client) error {
		w, err := newWriter(addr)
		if err != nil {
			return err
		}
		return ConnWriter(w)(c)
	}
}

// ConnBuffer sets the maximum buffer size before sending to
// the server.
//
// The dogstatsd_buffer_size option in the DataDog agent's
// config.yaml must be equal to or larger than the buffer size
// set here to prevent dropped/corrupted metrics.
//
// The buffer size is additionally limited by the interface's MTU.
// Setting the buffer size larger than the MTU will result in a
// maximum buffer size equal to the MTU.
func ConnBuffer(size int) ConnOpt {
	return func(c *Client) error {
		c.maxBuffer = size
		return nil
	}
}

// ConnWriter sets the writer to write statistics to.
func ConnWriter(w Writer) ConnOpt {
	return func(c *Client) error {
		c.writer = w
		c.mtu = w.MTU()
		return nil
	}
}

func newWriter(addr string) (Writer, error) {
	const unixPrefix = "unix://"
	if strings.HasPrefix(addr, unixPrefix) {
		return newUdsWriter(addr[len(unixPrefix)-1:])
	}
	return newUDPWriter(addr)
}

// Clone creates a new client with the same settings.
//
// This is useful to avoid lock contention between busy,
// long-running goroutines.
func (c *Client) Clone() (*Client, error) {
	cc, err := New(c.opts...)
	if err != nil {
		return nil, err
	}

	cc.Namespace = c.Namespace
	cc.Tags = c.Tags
	cc.SkipErrors = c.SkipErrors

	return c, nil
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
		buf = strconv.AppendFloat(buf, val, 'f', -1, 64)

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
	// return an error if message is larger than mtu
	if len(buf) > c.mtu {
		return errors.New("message size exceeds MTU")
	}

	c.mu.Lock()

	// check if mtu reached
	newSize := len(c.buffer) + 1 + len(buf) // existing + newline + new
	if newSize > c.mtu || (c.maxBuffer > 0 && newSize > c.maxBuffer) {
		err := c.flushLocked()
		if err != nil {
			c.mu.Unlock()
			return err
		}
	}

	// add to buffer
	if len(c.buffer) > 0 {
		c.buffer = append(c.buffer, '\n')
	}
	c.buffer = append(c.buffer, buf...)

	// flush now if not buffered or reached maxBuffer
	var err error
	if c.maxBuffer <= 0 || newSize == c.maxBuffer {
		err = c.flushLocked()
	}

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

// Flush forces a flush of the pending commands in the buffer
func (c *Client) Flush() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushLocked()
}

// flush the commands in the buffer.
//
// c.mu must be held by caller.
func (c *Client) flushLocked() error {
	if len(c.buffer) == 0 {
		return nil
	}

	if c.timer != nil {
		c.timer.Stop()
	}

	_, err := c.writer.Write(c.buffer)
	c.buffer = c.buffer[:0]

	if c.maxBuffer > 0 {
		c.timer = time.AfterFunc(c.flushTime, func() { c.Flush() })
	}

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
	return c.TimeInMilliseconds(name, float64(value)/float64(time.Millisecond), rate, tags...)
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
	if c.maxBuffer > 0 {
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
