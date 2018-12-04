package mdb

import (
	"fmt"
	"qpush/pkg/metrics"
	"time"
)

var (
	maxRetry  = 10
	retryWait = 10 * time.Millisecond
)

// RetryUntilSuccess do what it says
func RetryUntilSuccess(maxRetries int, sleepDurationOnFailure time.Duration, method string, f func() error) (err error) {

	var retry int
	defer func() {
		retryMetric := metrics.GetHist(metrics.RetryCount)

		errStr := fmt.Sprintf("%v", err)
		lvs := []string{"method", method, "error", errStr}

		retryMetric.With(lvs...).Observe(float64(maxRetries - retry + 1))
	}()

	for retry = maxRetries; retry != 0; retry-- {
		if err = f(); err == nil {
			return nil
		}
		if sleepDurationOnFailure > 0 {
			time.Sleep(sleepDurationOnFailure)
		}
	}
	return err
}
