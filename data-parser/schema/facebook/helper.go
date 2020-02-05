package facebook

import (
	"fmt"
	"time"
)

func weekdayOfTime(t time.Time) int {
	weekday := t.Weekday() // time.Time Sunday is 0, this project Monday is 0

	if weekday == time.Sunday {
		return int(time.Saturday)
	} else {
		return int(weekday) - 1
	}
}

// 1999-01-01
func dateOfTime(t time.Time) string {
	return fmt.Sprintf("%d-%d-%d", t.Year(), t.Month(), t.Day())
}
