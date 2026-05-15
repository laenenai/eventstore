package es_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laenenai/eventstore/es"
)

// fakeReader records every ListStates call and serves rows from a
// pre-baked list keyed by typeURL. Pagination is implemented by the
// fake the same way the real store does it: rows with stream_id >
// afterStreamID, capped at limit.
type fakeReader struct {
	rows  []es.StateCacheRow // ordered by (tenant, stream_id)
	calls int
}

func (f *fakeReader) GetState(_ context.Context, _, _ string) (es.StateCacheRow, error) {
	return es.StateCacheRow{}, errors.New("not used")
}

func (f *fakeReader) ListStates(
	_ context.Context, tenantID, typeURL, afterStreamID string, limit int,
) ([]es.StateCacheRow, error) {
	f.calls++
	out := make([]es.StateCacheRow, 0, limit)
	for _, r := range f.rows {
		if r.TenantID != tenantID || r.TypeURL != typeURL {
			continue
		}
		if afterStreamID != "" && r.StreamID <= afterStreamID {
			continue
		}
		out = append(out, r)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// erroringReader returns an error on the Nth call.
type erroringReader struct {
	errOnCall int
	calls     int
}

func (e *erroringReader) GetState(_ context.Context, _, _ string) (es.StateCacheRow, error) {
	return es.StateCacheRow{}, errors.New("not used")
}

func (e *erroringReader) ListStates(
	_ context.Context, _, _, _ string, _ int,
) ([]es.StateCacheRow, error) {
	e.calls++
	if e.calls == e.errOnCall {
		return nil, errors.New("store boom")
	}
	return nil, nil
}

func mkRow(tenant, typ, streamID string) es.StateCacheRow {
	return es.StateCacheRow{
		TenantID: tenant,
		TypeURL:  typ,
		StreamID: streamID,
	}
}

func TestScanAllStates_SinglePage(t *testing.T) {
	r := &fakeReader{rows: []es.StateCacheRow{
		mkRow("t-1", "T.v1.X", "a"),
		mkRow("t-1", "T.v1.X", "b"),
	}}

	var got []string
	for row, err := range es.ScanAllStates(context.Background(), r, "t-1", "T.v1.X", 1000) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got = append(got, row.StreamID)
	}
	if want := []string{"a", "b"}; !equal(got, want) {
		t.Errorf("rows: got %v want %v", got, want)
	}
	if r.calls != 1 {
		t.Errorf("calls: got %d want 1 (short page should not trigger another fetch)", r.calls)
	}
}

func TestScanAllStates_MultiPagePagination(t *testing.T) {
	rows := []es.StateCacheRow{
		mkRow("t-1", "T", "a"),
		mkRow("t-1", "T", "b"),
		mkRow("t-1", "T", "c"),
		mkRow("t-1", "T", "d"),
		mkRow("t-1", "T", "e"),
	}
	r := &fakeReader{rows: rows}

	var got []string
	for row, err := range es.ScanAllStates(context.Background(), r, "t-1", "T", 2) {
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		got = append(got, row.StreamID)
	}
	if want := []string{"a", "b", "c", "d", "e"}; !equal(got, want) {
		t.Errorf("rows: got %v want %v", got, want)
	}
	// pages: (a,b), (c,d), (e) — last page is short, stops the loop.
	// calls: 3.
	if r.calls != 3 {
		t.Errorf("calls: got %d want 3", r.calls)
	}
}

func TestScanAllStates_FiltersByTenantAndType(t *testing.T) {
	rows := []es.StateCacheRow{
		mkRow("t-1", "T.A", "a"),
		mkRow("t-1", "T.B", "x"), // wrong type
		mkRow("t-2", "T.A", "y"), // wrong tenant
		mkRow("t-1", "T.A", "b"),
	}
	r := &fakeReader{rows: rows}

	var got []string
	for row, err := range es.ScanAllStates(context.Background(), r, "t-1", "T.A", 1000) {
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		got = append(got, row.StreamID)
	}
	if want := []string{"a", "b"}; !equal(got, want) {
		t.Errorf("rows: got %v want %v", got, want)
	}
}

func TestScanAllStates_Empty(t *testing.T) {
	r := &fakeReader{} // no rows
	count := 0
	for _, err := range es.ScanAllStates(context.Background(), r, "t-1", "T", 100) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
	}
	if count != 0 {
		t.Errorf("empty store: got %d rows want 0", count)
	}
}

func TestScanAllStates_EarlyBreak(t *testing.T) {
	rows := []es.StateCacheRow{
		mkRow("t-1", "T", "a"),
		mkRow("t-1", "T", "b"),
		mkRow("t-1", "T", "c"),
		mkRow("t-1", "T", "d"),
	}
	r := &fakeReader{rows: rows}

	count := 0
	for _, err := range es.ScanAllStates(context.Background(), r, "t-1", "T", 2) {
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		count++
		if count == 2 {
			break
		}
	}
	if count != 2 {
		t.Errorf("expected exactly 2 rows after break, got %d", count)
	}
	// Caller broke during the first page; we should have made exactly
	// one ListStates call.
	if r.calls != 1 {
		t.Errorf("calls: got %d want 1", r.calls)
	}
}

func TestScanAllStates_PropagatesStoreError(t *testing.T) {
	r := &erroringReader{errOnCall: 1}
	var sawErr error
	for _, err := range es.ScanAllStates(context.Background(), r, "t-1", "T", 100) {
		if err != nil {
			sawErr = err
			break
		}
	}
	if sawErr == nil {
		t.Error("expected store error to be yielded")
	}
}

func TestScanAllStates_RespectsContextCancellation(t *testing.T) {
	rows := []es.StateCacheRow{
		mkRow("t-1", "T", "a"),
		mkRow("t-1", "T", "b"),
		mkRow("t-1", "T", "c"),
	}
	r := &fakeReader{rows: rows}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before iteration starts

	for _, err := range es.ScanAllStates(ctx, r, "t-1", "T", 1) {
		if err == nil {
			t.Fatal("expected ctx.Err()")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("got %v want context.Canceled", err)
		}
		break
	}
}

func TestScanAllStates_ZeroPageSizeUsesDefault(t *testing.T) {
	r := &fakeReader{rows: []es.StateCacheRow{
		mkRow("t-1", "T", "a"),
	}}
	for range es.ScanAllStates(context.Background(), r, "t-1", "T", 0) {
	}
	// No assertion needed — just verifying no panic and the iterator
	// terminates.
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
