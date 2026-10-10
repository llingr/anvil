// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// handlerState is how call left a handler's record
type handlerState int

const (
	stateNeverCalled handlerState = iota
	stateRunning                  // called, and not returned before the deadline
	stateFinished                 // returned before the deadline, with its error or none
)

func (state handlerState) String() string {
	return [...]string{"never called", "running", "finished"}[state]
}

// handlerRecord builds a handler's record as call would leave it in state. It is the only code in
// the tests that sets the record's fields.
func handlerRecord(name string, state handlerState, err error) *shutdownHandler {
	called := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	record := &shutdownHandler{name: name, fn: shutdown.HandlerFunc(quick)}
	switch state {
	case stateRunning:
		record.called = called
	case stateFinished:
		record.called = called
		record.returned = called.Add(time.Second)
		record.err = err
	}
	return record
}

// stateOf reads the state call left record in, and the error it recorded
func stateOf(record *shutdownHandler) (handlerState, error) {
	switch {
	case record.isNeverCalled():
		return stateNeverCalled, nil
	case record.isRunning():
		return stateRunning, nil
	default:
		return stateFinished, record.err
	}
}

// recordedGroup is the group at position order, named name ("" for none), with the given records
// in the order added
func recordedGroup(order int, name string, handlers ...*shutdownHandler) *shutdownGroup {
	return &shutdownGroup{order: order, name: name, handlers: handlers}
}

// A group is labelled by its position, with its name in brackets when it has one
func TestGroupLabel(t *testing.T) {
	if got := recordedGroup(1, "").label(); got != "group 1" {
		t.Errorf("unnamed label %q, want %q", got, "group 1")
	}
	if got := recordedGroup(12, "PAYMENTS").label(); got != "group 12 (PAYMENTS)" {
		t.Errorf("named label %q, want %q", got, "group 12 (PAYMENTS)")
	}
}

// With a result already waiting and the ctx already ended, await takes the result, so the record
// depends on when the handler returned and not on which the select saw first. Go picks at random
// between ready cases, so 100 waits take the ctx's side about half the time.
func TestAwaitTakesAResultWaitingAsTheCtxEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	failed := errors.New("flush failed")
	for attempt := range 100 {
		results := make(chan result, 1)
		sent := result{err: failed, at: time.Now()}
		results <- sent
		got, ok := await(ctx, results)
		if !ok || got != sent {
			t.Fatalf("wait %d returned %v %v, want the waiting result %v", attempt, got, ok, sent)
		}
	}
}

// Work is unfinished exactly when a handler is running or was never called, failures included as
// finished. The deadline error, built only then, names every handler running at the deadline, in
// the order the groups stop and then the order added within a group, or else the first never
// called in that order, skipping empty groups. Each case lists its groups in the order added, so
// the last listed stops first.
func TestDeadlineError(t *testing.T) {
	failed := errors.New("flush failed")
	cases := []struct {
		name     string
		deadline time.Duration
		groups   []*shutdownGroup // in the order added; the last stops first
		want     string           // "" when nothing is unfinished, so there is no error to build
	}{
		{
			name:     "every handler finished, one failing",
			deadline: 28 * time.Second,
			groups: []*shutdownGroup{
				recordedGroup(1, "EGRESS", handlerRecord("pool", stateFinished, nil)),
				recordedGroup(2, "CORE", handlerRecord("ledger", stateFinished, failed)),
				recordedGroup(3, "INGRESS", handlerRecord("consumer", stateFinished, nil)),
			},
		},
		{
			name:     "no handlers in any group",
			deadline: 28 * time.Second,
			groups:   []*shutdownGroup{recordedGroup(1, ""), recordedGroup(2, "CORE"), recordedGroup(3, "INGRESS")},
		},
		{
			name:     "one running, those stopping after it never called",
			deadline: 28 * time.Second,
			groups: []*shutdownGroup{
				recordedGroup(1, "EGRESS", handlerRecord("pool", stateNeverCalled, nil)),
				recordedGroup(2, "CORE", handlerRecord("ledger", stateRunning, nil), handlerRecord("outbox", stateNeverCalled, nil)),
				recordedGroup(3, "INGRESS", handlerRecord("consumer", stateFinished, nil)),
			},
			want: "shutdown deadline 28s passed: group 2 (CORE) ledger",
		},
		{
			name:     "several running, in the order added, not by name",
			deadline: 28 * time.Second,
			groups: []*shutdownGroup{
				recordedGroup(1, "EGRESS", handlerRecord("kafka", stateNeverCalled, nil)),
				recordedGroup(2, "NOTIFICATIONS",
					handlerRecord("sms", stateRunning, nil),
					handlerRecord("push", stateFinished, nil),
					handlerRecord("email", stateRunning, nil)),
			},
			want: "shutdown deadline 28s passed: group 2 (NOTIFICATIONS) sms, group 2 (NOTIFICATIONS) email",
		},
		{
			name:     "running in two groups, in the order they stop",
			deadline: 28 * time.Second,
			groups: []*shutdownGroup{
				recordedGroup(1, "EGRESS", handlerRecord("kafka", stateRunning, nil)),
				recordedGroup(2, "CORE", handlerRecord("ledger", stateRunning, nil)),
			},
			want: "shutdown deadline 28s passed: group 2 (CORE) ledger, group 1 (EGRESS) kafka",
		},
		{
			name:     "running in an unnamed group",
			deadline: 28 * time.Second,
			groups: []*shutdownGroup{
				recordedGroup(1, "", handlerRecord("go", stateRunning, nil)),
				recordedGroup(2, "", handlerRecord("*http.Server", stateFinished, nil)),
			},
			want: "shutdown deadline 28s passed: group 1 go",
		},
		{
			name:     "a failed handler is not running",
			deadline: 28 * time.Second,
			groups: []*shutdownGroup{
				recordedGroup(1, "CORE", handlerRecord("ledger", stateFinished, failed), handlerRecord("outbox", stateRunning, nil)),
			},
			want: "shutdown deadline 28s passed: group 1 (CORE) outbox",
		},
		{
			name:     "running named over one never called before it, in a group begun at the instant",
			deadline: 28 * time.Second,
			groups: []*shutdownGroup{
				recordedGroup(1, "DB_POOLS", handlerRecord("postgres", stateNeverCalled, nil), handlerRecord("dynamo", stateRunning, nil)),
			},
			want: "shutdown deadline 28s passed: group 1 (DB_POOLS) dynamo",
		},
		{
			name:     "none running: the first never called, in the order they stop",
			deadline: 28 * time.Second,
			groups: []*shutdownGroup{
				recordedGroup(1, "LEDGER", handlerRecord("outbox", stateNeverCalled, nil)),
				recordedGroup(2, "PAYMENTS", handlerRecord("refunds", stateNeverCalled, nil), handlerRecord("payments", stateNeverCalled, nil)),
				recordedGroup(3, "INGRESS_HTTP", handlerRecord("http", stateFinished, nil)),
			},
			want: "shutdown deadline 28s passed before group 2 (PAYMENTS) refunds",
		},
		{
			name:     "none running: one never called after one finished in a group begun at the instant",
			deadline: 28 * time.Second,
			groups: []*shutdownGroup{
				recordedGroup(1, "DB_POOLS", handlerRecord("postgres", stateFinished, nil), handlerRecord("dynamo", stateNeverCalled, nil)),
			},
			want: "shutdown deadline 28s passed before group 1 (DB_POOLS) dynamo",
		},
		{
			name:     "none called, past groups with no handlers",
			deadline: 28 * time.Second,
			groups: []*shutdownGroup{
				recordedGroup(1, "LEDGER", handlerRecord("outbox", stateNeverCalled, nil), handlerRecord("ledger", stateNeverCalled, nil)),
				recordedGroup(2, "DB_POOLS"),
				recordedGroup(3, "PAYMENTS"),
				recordedGroup(4, "INGRESS_HTTP"),
			},
			want: "shutdown deadline 28s passed before group 1 (LEDGER) outbox",
		},
		{
			name:     "none called",
			deadline: 28 * time.Second,
			groups: []*shutdownGroup{
				recordedGroup(1, "CORE", handlerRecord("ledger", stateNeverCalled, nil)),
				recordedGroup(2, "INGRESS", handlerRecord("consumer", stateNeverCalled, nil)),
			},
			want: "shutdown deadline 28s passed before group 2 (INGRESS) consumer",
		},
		{
			name:     "a deadline of a second and a half",
			deadline: 1500 * time.Millisecond,
			groups:   []*shutdownGroup{recordedGroup(1, "EGRESS", handlerRecord("kafka", stateRunning, nil))},
			want:     "shutdown deadline 1.5s passed: group 1 (EGRESS) kafka",
		},
		{
			name:     "a deadline under a second",
			deadline: 300 * time.Millisecond,
			groups:   []*shutdownGroup{recordedGroup(1, "EGRESS", handlerRecord("kafka", stateNeverCalled, nil)), recordedGroup(2, "CORE")},
			want:     "shutdown deadline 300ms passed before group 1 (EGRESS) kafka",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			unfinished := false
			for _, group := range tc.groups {
				for _, handler := range group.handlers {
					if handler.returned.IsZero() {
						unfinished = true
					}
				}
			}
			if unfinished != (tc.want != "") {
				t.Fatalf("unfinished %v, want %v", unfinished, tc.want != "")
			}
			if !unfinished {
				return
			}
			if err := deadlineError(tc.deadline, tc.groups); err.Error() != tc.want {
				t.Errorf("deadline error %q, want %q", err, tc.want)
			}
		})
	}
}
