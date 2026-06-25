package bench

import (
	"fmt"
)

// Cohort labels the activity profile of a tenant during the run
// phase of a scenario. The 90/9/1 split mirrors the spike brief's
// realistic distribution: most tenants are dormant, a slice are
// occasional users, a tiny fraction are continuously active.
type Cohort uint8

const (
	CohortCold Cohort = iota // ~90 %: seeded then untouched
	CohortWarm               // ~9 %:  a handful of writes per run
	CohortHot                // ~1 %:  many writes per run
)

// String returns the cohort name used in metric labels.
func (c Cohort) String() string {
	switch c {
	case CohortCold:
		return "cold"
	case CohortWarm:
		return "warm"
	case CohortHot:
		return "hot"
	default:
		return "unknown"
	}
}

// Tenant is one row in the synthetic population. The ID is
// deterministic from (cohort, index) so reruns produce the same
// distribution and any anomaly is reproducible.
type Tenant struct {
	ID     string
	Cohort Cohort
	Index  int // position within the cohort
}

// Population materialises a 90/9/1 tenant slice of the requested
// total size. The split rounds slightly conservatively (cold gets
// any remainder) so total == N exactly. Deterministic: same N
// returns the same slice.
//
// At N=10_000 this is 9_000 cold / 900 warm / 100 hot.
func Population(total int) []Tenant {
	if total <= 0 {
		return nil
	}
	hot := total / 100   // 1 %
	warm := total / 10 / // 9 %
		1 * 9
	// integer division above produces 9% via two steps; explicit:
	warm = (total * 9) / 100
	cold := total - hot - warm

	out := make([]Tenant, 0, total)
	for i := 0; i < cold; i++ {
		out = append(out, Tenant{
			ID:     fmt.Sprintf("tenant-cold-%06d", i),
			Cohort: CohortCold,
			Index:  i,
		})
	}
	for i := 0; i < warm; i++ {
		out = append(out, Tenant{
			ID:     fmt.Sprintf("tenant-warm-%06d", i),
			Cohort: CohortWarm,
			Index:  i,
		})
	}
	for i := 0; i < hot; i++ {
		out = append(out, Tenant{
			ID:     fmt.Sprintf("tenant-hot-%06d", i),
			Cohort: CohortHot,
			Index:  i,
		})
	}
	return out
}
