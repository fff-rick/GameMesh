package scheduler

import "github.com/gamemesh-labs/gamemesh/pkg/model"

// Candidates is a read-only view of scheduler candidates.  It deliberately
// exposes values rather than a mutable slice, so a registry-published snapshot
// cannot be changed by a scheduler implementation.
type Candidates interface {
	Len() int
	At(int) model.GameServerSnapshot
}

// StaticCandidates adapts a slice for tests and the M0 simulator. It copies
// the input so callers cannot alter it after handing it to a scheduler.
type StaticCandidates struct {
	servers []model.GameServerSnapshot
}

func NewStaticCandidates(servers []model.GameServerSnapshot) StaticCandidates {
	return StaticCandidates{servers: append([]model.GameServerSnapshot(nil), servers...)}
}

// BorrowCandidates adapts a caller-owned, short-lived slice without copying.
// It is only for controlled producers such as the M0 simulator; Registry
// consumers should pass its immutable Snapshot directly.
func BorrowCandidates(servers []model.GameServerSnapshot) StaticCandidates {
	return StaticCandidates{servers: servers}
}

func (c StaticCandidates) Len() int { return len(c.servers) }

func (c StaticCandidates) At(index int) model.GameServerSnapshot { return c.servers[index] }
