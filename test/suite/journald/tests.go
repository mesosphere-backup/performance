package journald

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/coreos/go-systemd/journal"
	"github.com/coreos/go-systemd/sdjournal"
	"github.com/mesosphere/performance/test/http"
)

func newJournalSince(since time.Duration) (*sdjournal.Journal, error) {
	journal, err := sdjournal.NewJournal()
	if err != nil {
		return journal, err
	}
	journal.Wait(since)

	return journal, nil
}

// droppedLogs function is meant to be ran in asynchronous goroutine. It sends
// events to the supervisor independently from the parent goroutine when logs
// are detected as dropped in journald.
func droppedLogsDetector(ctx context.Context, supervisorURL string) error {
	jlog.Info("Starting dropped logs detection service")
	eventName := fmt.Sprintf(TEST_SUITE + DELIMITER + "dropped-logs")
	dropEvent, err := newEvent(eventName)
	if err != nil {
		return err
	}

	lastJournalCheck := time.Since(time.Now())
	jlog.Info("getting new journal")

	sdReader, err := newJournalSince(lastJournalCheck)
	if err != nil {
		return err
	}

	jlog.Infof("Checking journal since %s", lastJournalCheck)
	for {

		select {

		case <-ctx.Done():
			jlog.Info("Canceling dropped log detector routine")
			return nil

		default:

			jlog.Info("adding suppressed match")
			if err := sdReader.AddMatch("Suppressed"); err != nil {
				return err
			}

			jlog.Info("getting journal entry")
			entry, err := sdReader.GetEntry()
			if err != nil {
				return err
			}

			jlog.Info("detecting fields from entry")
			if len(entry.Fields) != 0 {
				jlog.Info("DROP LOG EVENT")
				jlog.Warnf("%+v", entry)
				if err := http.PostToSupervisor(supervisorURL, dropEvent); err != nil {
					return err
				}
			}

		}

	}

	return nil
}

// ConstantRate sends log lines to STDOUT or STDERR at a constant rate denoted by
// j.LoggingRate. This method is canceled by the parent context when it reaches the
// configured context.WithTimeout() which is set by JournaldTestSuite.TestDuration
// parameter.
func ConstantRate(supervisorURL string, j JournaldTestSuite) error {
	jlog.Info("Starting constant rate test for journald")

	if !journal.Enabled() {
		return errors.New("Systemd-journald not enabled, canceling request to start test")
	}

	rate := time.Duration(time.Second / time.Duration(j.LoggingRate))
	ticker := time.NewTicker(rate)

	line := []string{}
	for length := 0; length <= 2048; length++ {
		line = append(line, "0")
	}

	startEventName := fmt.Sprintf(TEST_SUITE+DELIMITER+"constant-rate"+DELIMITER+START+DELIMITER+"%d"+DELIMITER+LINES_PER_SECOND, j.LoggingRate)
	stopEventName := fmt.Sprintf(TEST_SUITE+DELIMITER+"constant-rate"+DELIMITER+STOP+DELIMITER+"%d"+DELIMITER+LINES_PER_SECOND, j.LoggingRate)

	startEvent, err := newEvent(startEventName)
	if err != nil {
		return err
	}

	stopEvent, err := newEvent(stopEventName)
	if err != nil {
		return err
	}

	if err := http.PostToSupervisor(supervisorURL, startEvent); err != nil {
		return err
	}

	jlog.Infof("Starting constant rate loop:\n   Duration %d seconds\n   Rate: %d lines per second", j.TestDuration, j.LoggingRate)

	go droppedLogsDetector(j.Context, supervisorURL)

	for {
		select {
		case <-ticker.C:
			//journal.Print(journal.PriInfo, fmt.Sprintf("%v\n", line))
			fmt.Fprintf(os.Stdout, fmt.Sprintf("%v", line))
			if j.StdErr {
				journal.Print(journal.PriErr, fmt.Sprintf("%v\n", line))
			}
		case <-j.Context.Done():
			return http.PostToSupervisor(supervisorURL, stopEvent)
		}
	}
}
