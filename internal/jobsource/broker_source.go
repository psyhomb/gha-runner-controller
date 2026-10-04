// Package jobsource discovers demand for runners via the actions-service
// long-poll ("broker"): GitHub's internal API. Scaling is driven by
// the scale-set statistics snapshot carried on every message batch (and
// returned at session creation) - there is no client-side job bookkeeping to
// go stale. The server is the source of truth on every
// message, and every state change generates a message, so the cached snapshot
// is current by construction.
package jobsource

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"gha-runner-controller/internal/github"
)

// Source is the seam between demand discovery and the VM lifecycle.
type Source interface {
	// JobsInFlight returns jobs that occupy or need a runner (available +
	// assigned; assigned includes running), from the latest statistics
	// snapshot.
	JobsInFlight() int
	// BusyRunnerNames reports runner names with a job in flight (tracked from
	// JobStarted/JobCompleted messages).
	BusyRunnerNames() map[string]bool
}

// BrokerAPI is the subset of the broker client BrokerSource needs
// (consumer-side interface, for testing with fakes).
type BrokerAPI interface {
	CreateSession(ctx context.Context, scaleSetID int) (*github.ScaleSetStatistics, error)
	GetMessage(ctx context.Context, scaleSetID, lastMessageID, maxCapacity int) (*github.JobMessages, error)
	DeleteMessage(ctx context.Context, scaleSetID, messageID int) error
	DeleteSession(ctx context.Context, scaleSetID int) error
	AcquireJobs(ctx context.Context, scaleSetID int, requestIDs []int64) ([]int64, error)
}

// BrokerSource consumes the scale set message stream. It keeps only two
// pieces of state: the latest statistics snapshot (drives scaling) and the
// set of busy runners (protects reaping).
//
// Acquisition discipline: per the official actions/scaleset listener contract
// (the official actions/scaleset library), every JobAvailable the listener wants must be
// passed to AcquireJobs, or the job stays unassigned forever. Jobs queued
// while a runner is already registered are assigned directly by GitHub and
// need no acquisition - JobAvailable is the scale-from-zero path. Batches
// containing a job we could not acquire are NOT acked - the broker redelivers
// unacked messages, which is our acquisition retry (and replays into fresh
// sessions after restarts). After maxHandleAttempts failed attempts a message
// is dead-lettered (acked) to avoid poisoning the queue; the statistics keep
// the job visible to scaling regardless.
type BrokerSource struct {
	broker      BrokerAPI
	scaleSetID  int
	maxCapacity int

	mu            sync.Mutex
	stats         *github.ScaleSetStatistics // latest snapshot (session creation or batch)
	busy          map[string]bool            // runner names with a job in flight
	lastMessageID int
}

const maxHandleAttempts = 5

func NewBrokerSource(broker BrokerAPI, scaleSetID, maxCapacity int) *BrokerSource {
	return &BrokerSource{
		broker:      broker,
		scaleSetID:  scaleSetID,
		maxCapacity: maxCapacity,
		busy:        make(map[string]bool),
	}
}

// JobsInFlight implements Source.
func (s *BrokerSource) JobsInFlight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stats == nil {
		return 0
	}
	return s.stats.TotalAvailableJobs + s.stats.TotalAssignedJobs
}

// BusyRunnerNames implements Source.
func (s *BrokerSource) BusyRunnerNames() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]bool, len(s.busy))
	for name := range s.busy {
		out[name] = true
	}
	return out
}

// Start runs the long-poll loop until ctx is cancelled. The message session
// is created first (with retries - it can fail transiently, e.g. a 409 while
// a previous session is still expiring after a restart).
func (s *BrokerSource) Start(ctx context.Context) {
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			stats, err := s.broker.CreateSession(ctx, s.scaleSetID)
			if err != nil {
				slog.Warn("broker: create session failed, retrying in 5s", "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
				}
				continue
			}
			// The session response carries the current statistics: we converge
			// immediately on (re)start, without any event replay.
			s.setStats(stats)
			logStats := []any{"scaleSetID", s.scaleSetID}
			if stats != nil {
				logStats = append(logStats, "availableJobs", stats.TotalAvailableJobs, "assignedJobs", stats.TotalAssignedJobs, "runningJobs", stats.TotalRunningJobs, "registeredRunners", stats.TotalRegisteredRunners)
			}
			slog.Info("broker: message session created", logStats...)
			break
		}

		attempts := map[int]int{}
		for {
			if ctx.Err() != nil {
				return
			}
			msg, err := s.broker.GetMessage(ctx, s.scaleSetID, s.getLastMessageID(), s.maxCapacity)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Warn("broker: get message failed, retrying in 5s", "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
				}
				continue
			}
			if msg == nil {
				continue // long-poll window elapsed without messages
			}
			// One INFO line per delivered batch: makes "is the long-poll
			// receiving anything?" answerable at the default log level.
			attrs := []any{
				"messageID", msg.MessageID,
				"available", len(msg.Available),
				"assigned", len(msg.Assigned),
				"started", len(msg.Started),
				"completed", len(msg.Completed),
			}
			if msg.Statistics != nil {
				attrs = append(attrs,
					"availableJobs", msg.Statistics.TotalAvailableJobs,
					"acquiredJobs", msg.Statistics.TotalAcquiredJobs,
					"assignedJobs", msg.Statistics.TotalAssignedJobs,
					"runningJobs", msg.Statistics.TotalRunningJobs,
					"registeredRunners", msg.Statistics.TotalRegisteredRunners,
					"busyRunners", msg.Statistics.TotalBusyRunners,
					"idleRunners", msg.Statistics.TotalIdleRunners)
			}
			slog.Info("broker: message batch", attrs...)
			if s.handle(ctx, msg) {
				delete(attempts, msg.MessageID)
				// lastMessageID advances only after successful handling,
				// then the message is acked (mirrors the reference listener).
				s.setLastMessageID(msg.MessageID)
				if err := s.broker.DeleteMessage(ctx, s.scaleSetID, msg.MessageID); err != nil {
					slog.Warn("broker: ack message failed", "messageID", msg.MessageID, "error", err)
				}
				continue
			}
			attempts[msg.MessageID]++
			if attempts[msg.MessageID] >= maxHandleAttempts {
				slog.Warn("broker: dead-lettering message after repeated failures", "messageID", msg.MessageID, "attempts", attempts[msg.MessageID])
				delete(attempts, msg.MessageID)
				s.setLastMessageID(msg.MessageID)
				if err := s.broker.DeleteMessage(ctx, s.scaleSetID, msg.MessageID); err != nil {
					slog.Warn("broker: ack message failed", "messageID", msg.MessageID, "error", err)
				}
			}
			// unacked messages are redelivered on the next poll
		}
	}()
}

// Close deletes the message session (best-effort, fresh 10s context).
func (s *BrokerSource) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.broker.DeleteSession(ctx, s.scaleSetID); err != nil {
		slog.Warn("broker: delete session failed", "error", err)
	}
}

func (s *BrokerSource) setStats(stats *github.ScaleSetStatistics) {
	if stats == nil {
		return
	}
	s.mu.Lock()
	s.stats = stats
	s.mu.Unlock()
}

func (s *BrokerSource) getLastMessageID() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastMessageID
}

func (s *BrokerSource) setLastMessageID(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id > s.lastMessageID {
		s.lastMessageID = id
	}
}

// handle processes one queue message and reports whether it may be acked.
func (s *BrokerSource) handle(ctx context.Context, msg *github.JobMessages) (ackable bool) {
	ackable = true
	s.setStats(msg.Statistics)
	for _, m := range msg.Available {
		repo := m.OwnerName + "/" + m.RepositoryName
		slog.Info("broker: job available", "repo", repo, "job", m.JobDisplayName, "run", m.WorkflowRunID)
		if !s.acquire(ctx, m.RunnerRequestID, repo, m.JobDisplayName) {
			ackable = false // leave unacked so redelivery retries the acquisition
		}
	}
	// Assigned jobs need no action: GitHub assigns them directly to the scale
	// set (they flow to the next registered runner), and the statistics in
	// this batch already count them for scaling.
	for _, m := range msg.Started {
		if m.RunnerName != "" {
			s.mu.Lock()
			s.busy[m.RunnerName] = true
			s.mu.Unlock()
		}
	}
	for _, m := range msg.Completed {
		if m.RunnerName != "" {
			s.mu.Lock()
			delete(s.busy, m.RunnerName)
			s.mu.Unlock()
		}
	}
	return ackable
}

// acquire claims an available job for the scale set.
func (s *BrokerSource) acquire(ctx context.Context, requestID int64, repo, name string) bool {
	acquired, err := s.broker.AcquireJobs(ctx, s.scaleSetID, []int64{requestID})
	if err != nil {
		slog.Warn("broker: acquire job failed", "repo", repo, "job", name, "error", err)
		return false
	}
	if len(acquired) == 0 {
		slog.Warn("broker: acquisition refused (capacity), will retry on redelivery", "repo", repo, "job", name)
		return false
	}
	slog.Info("broker: job acquired", "repo", repo, "job", name)
	return true
}
