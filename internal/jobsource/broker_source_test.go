package jobsource

import (
	"context"
	"errors"
	"testing"
	"time"

	"gha-runner-controller/internal/github"
)

type fakeBroker struct {
	stats           *github.ScaleSetStatistics
	sessionCreated  bool
	sessionDeleted  bool
	acquireNone     bool
	acquireErr      error
	acquiredIDs     []int64
	deletedMessages []int
}

func (f *fakeBroker) CreateSession(ctx context.Context, scaleSetID int) (*github.ScaleSetStatistics, error) {
	f.sessionCreated = true
	return f.stats, nil
}
func (f *fakeBroker) AcquireJobs(ctx context.Context, scaleSetID int, requestIDs []int64) ([]int64, error) {
	if f.acquireErr != nil {
		return nil, f.acquireErr
	}
	if f.acquireNone {
		return nil, nil
	}
	f.acquiredIDs = append(f.acquiredIDs, requestIDs...)
	return requestIDs, nil
}
func (f *fakeBroker) GetMessage(ctx context.Context, scaleSetID, lastMessageID, maxCapacity int) (*github.JobMessages, error) {
	<-ctx.Done() // block until the test cancels: no busy spin
	return nil, ctx.Err()
}
func (f *fakeBroker) DeleteMessage(ctx context.Context, scaleSetID, messageID int) error {
	f.deletedMessages = append(f.deletedMessages, messageID)
	return nil
}
func (f *fakeBroker) DeleteSession(ctx context.Context, scaleSetID int) error {
	f.sessionDeleted = true
	return nil
}

func jobAvailable(requestID int64) github.JobAvailableMessage {
	m := github.JobAvailableMessage{}
	m.RunnerRequestID = requestID
	m.JobDisplayName = "smoke-test"
	m.OwnerName = "org"
	m.RepositoryName = "repo"
	m.WorkflowRunID = 456
	return m
}

func TestJobsInFlightFromBatchStatistics(t *testing.T) {
	src := NewBrokerSource(&fakeBroker{}, 1, 2)
	if got := src.JobsInFlight(); got != 0 {
		t.Fatalf("JobsInFlight before any snapshot = %d, want 0", got)
	}
	src.handle(context.Background(), &github.JobMessages{
		MessageID:  1,
		Statistics: &github.ScaleSetStatistics{TotalAvailableJobs: 2, TotalAssignedJobs: 3},
	})
	if got := src.JobsInFlight(); got != 5 {
		t.Fatalf("JobsInFlight = %d, want 5 (available 2 + assigned 3)", got)
	}
	// a batch without statistics must not clear the snapshot
	src.handle(context.Background(), &github.JobMessages{MessageID: 2})
	if got := src.JobsInFlight(); got != 5 {
		t.Fatalf("JobsInFlight after stat-less batch = %d, want 5 (unchanged)", got)
	}
}

func TestJobsInFlightFromSessionCreation(t *testing.T) {
	// ARC's restart-convergence pattern: the session response carries the
	// statistics, so no event replay is needed after a restart.
	fb := &fakeBroker{stats: &github.ScaleSetStatistics{TotalAssignedJobs: 2}}
	src := NewBrokerSource(fb, 1, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src.Start(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for !fb.sessionCreated && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !fb.sessionCreated {
		t.Fatal("Start did not create the message session")
	}
	if got := src.JobsInFlight(); got != 2 {
		t.Fatalf("JobsInFlight from session stats = %d, want 2", got)
	}
}

func TestAvailableAcquires(t *testing.T) {
	fb := &fakeBroker{}
	src := NewBrokerSource(fb, 1, 2)
	if !src.handle(context.Background(), &github.JobMessages{
		MessageID: 1,
		Available: []github.JobAvailableMessage{jobAvailable(1001)},
	}) {
		t.Fatal("handle should be ack-able")
	}
	if len(fb.acquiredIDs) != 1 || fb.acquiredIDs[0] != 1001 {
		t.Fatalf("acquired = %v, want [1001]", fb.acquiredIDs)
	}
}

func TestAcquireRefusalRetriesOnRedelivery(t *testing.T) {
	fb := &fakeBroker{acquireNone: true}
	src := NewBrokerSource(fb, 1, 2)
	msg := &github.JobMessages{MessageID: 1, Available: []github.JobAvailableMessage{jobAvailable(1001)}}

	if src.handle(context.Background(), msg) {
		t.Fatal("handle should report not-ackable on acquisition refusal")
	}
	fb.acquireNone = false
	if !src.handle(context.Background(), msg) {
		t.Fatal("handle should report ack-able once acquisition succeeds")
	}
}

func TestAcquireErrorNotAckable(t *testing.T) {
	fb := &fakeBroker{acquireErr: errors.New("boom")}
	src := NewBrokerSource(fb, 1, 2)
	if src.handle(context.Background(), &github.JobMessages{
		MessageID: 1,
		Available: []github.JobAvailableMessage{jobAvailable(1001)},
	}) {
		t.Fatal("handle should report not-ackable on acquire error")
	}
}

func TestBusyTracking(t *testing.T) {
	src := NewBrokerSource(&fakeBroker{}, 1, 2)
	started := github.JobStartedMessage{}
	started.RunnerName = "vm-1"
	src.handle(context.Background(), &github.JobMessages{MessageID: 1, Started: []github.JobStartedMessage{started}})
	if !src.BusyRunnerNames()["vm-1"] {
		t.Fatal("vm-1 should be busy after JobStarted")
	}
	completed := github.JobCompletedMessage{}
	completed.RunnerName = "vm-1"
	src.handle(context.Background(), &github.JobMessages{MessageID: 2, Completed: []github.JobCompletedMessage{completed}})
	if src.BusyRunnerNames()["vm-1"] {
		t.Fatal("vm-1 should not be busy after JobCompleted")
	}
}

func TestCloseDeletesSession(t *testing.T) {
	fb := &fakeBroker{}
	src := NewBrokerSource(fb, 1, 2)
	src.Close()
	if !fb.sessionDeleted {
		t.Error("Close did not delete the session")
	}
}
