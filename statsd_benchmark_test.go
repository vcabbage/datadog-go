// +build go1.7

package statsd

import (
	"math"
	"strconv"
	"testing"
	"time"
)

func BenchmarkClientFormat(b *testing.B) {
	var tests = []struct {
		globalNamespace string
		globalTags      []string
		name            string
		value           interface{}
		suffix          string
		tags            []string
	}{
		{"", nil, "test.gauge", 1.0, gaugeSuffix, nil},
		{"", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA"}},
		{"", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
		{"", nil, "test.count", int64(1), countSuffix, []string{"tagA"}},
		{"", nil, "test.histogram", 2.3, histogramSuffix, []string{"tagA"}},
		{"", nil, "test.distribution", 2.3, distributionSuffix, []string{"tagA"}},
		{"", nil, "test.set", "uuid", setSuffix, []string{"tagA"}},
		{"flubber.", nil, "test.set", "uuid", setSuffix, []string{"tagA"}},
		{"", []string{"tagC"}, "test.set", "uuid", setSuffix, []string{"tagA"}},
		{"", nil, "test.count", int64(1), countSuffix, []string{"hello\nworld"}},
	}

	b.ReportAllocs()

	for i, tt := range tests {
		b.Run(strconv.Itoa(i), func(b *testing.B) {
			c, _ := New(ConnWriter(nullWriter{}))
			c.Namespace = tt.globalNamespace
			c.Tags = tt.globalTags

			b.ReportAllocs()
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				c.appendStat(tt.name, tt.value, tt.suffix, 1.0, tt.tags...)
			}
		})
	}
}

type nullWriter struct{}

func (nullWriter) Write(b []byte) (int, error)         { return len(b), nil }
func (nullWriter) SetWriteTimeout(time.Duration) error { return nil }
func (nullWriter) MTU() int                            { return math.MaxInt32 }
func (nullWriter) Close() error                        { return nil }

func BenchmarkFlush(b *testing.B) {
	var tests = []struct {
		globalNamespace string
		globalTags      []string
		name            string
		value           interface{}
		suffix          string
		tags            []string
	}{
		{"", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
	}

	b.ReportAllocs()

	for i, tt := range tests {
		b.Run(strconv.Itoa(i), func(b *testing.B) {
			c, err := New(ConnAddr("127.0.0.1:56789"))
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c.send(tt.name, tt.value, tt.suffix, 1.0, tt.tags...)
				}
			})
		})
	}
}

func BenchmarkFlushBatch(b *testing.B) {
	const kb = 1024

	var tests = []struct {
		batchSize       int
		globalNamespace string
		globalTags      []string
		name            string
		value           interface{}
		suffix          string
		tags            []string
	}{
		{1 * kb, "", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
		{4 * kb, "", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
		{8 * kb, "", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
		{16 * kb, "", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
		{32 * kb, "", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
		{64 * kb, "", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
	}

	b.ReportAllocs()

	for _, tt := range tests {
		b.Run(strconv.Itoa(tt.batchSize), func(b *testing.B) {
			c, err := New(ConnAddr("127.0.0.1:56789"), ConnBuffer(tt.batchSize))
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c.send(tt.name, tt.value, tt.suffix, 1.0, tt.tags...)
				}
			})
		})
	}
}

func BenchmarkFlushBatchShard(b *testing.B) {
	const kb = 1024

	var tests = []struct {
		batchSize       int
		globalNamespace string
		globalTags      []string
		name            string
		value           interface{}
		suffix          string
		tags            []string
	}{
		{1 * kb, "", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
		{4 * kb, "", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
		{8 * kb, "", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
		{16 * kb, "", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
		{32 * kb, "", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
		{64 * kb, "", nil, "test.gauge", 1.0, gaugeSuffix, []string{"tagA", "tagB"}},
	}

	b.ReportAllocs()

	for _, tt := range tests {
		b.Run(strconv.Itoa(tt.batchSize), func(b *testing.B) {
			c, err := New(ConnAddr("127.0.0.1:56789"), ConnBuffer(tt.batchSize))
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				c, err := c.Clone()
				if err != nil {
					b.Fatal(err)
				}
				for pb.Next() {
					c.send(tt.name, tt.value, tt.suffix, 1.0, tt.tags...)
				}
			})

			b.StopTimer()
			c.Close()
		})
	}
}
