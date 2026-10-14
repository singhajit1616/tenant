package domain

import (
	"fmt"
	"strings"
	"time"
)

// TimeFormat is ISO 8601 UTC with millisecond precision and a Z suffix.
const TimeFormat = "2006-01-02T15:04:05.000Z"

// Time is a UTC timestamp serialized as ISO 8601 milliseconds.
type Time time.Time

// NewTime converts a timestamp to UTC and truncates it to millisecond precision.
func NewTime(t time.Time) Time {
	return Time(t.UTC().Truncate(time.Millisecond))
}

// Now returns the current UTC time truncated to milliseconds.
func Now() Time {
	return NewTime(time.Now())
}

// Time exposes the underlying standard-library timestamp.
func (t Time) Time() time.Time {
	return time.Time(t)
}

// MarshalJSON encodes a timestamp as a quoted ISO 8601 UTC string with milliseconds and a
// Z suffix.
func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.Time().UTC().Format(TimeFormat) + `"`), nil
}

// UnmarshalJSON parses the API timestamp format or RFC3339Nano and normalizes the result
// to UTC milliseconds.
func (t *Time) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	parsed, err := time.Parse(TimeFormat, s)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return fmt.Errorf("invalid timestamp %q: %w", s, err)
		}
	}
	*t = NewTime(parsed)
	return nil
}

// Equal reports whether two timestamps represent the same instant.
func (t Time) Equal(other Time) bool {
	return t.Time().Equal(other.Time())
}
