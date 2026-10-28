package statefeed

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http/httptest"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
)

// This fixture isolates feed work from chain execution and disk reads. The
// subscription size matches the 1,075-account deployment; changed counts cover
// empty, typical sparse, and dense blocks without asserting a live-chain SLA.
func performanceFixture(b testing.TB, changed int) (*Server, *types.Header, *tracing.StateUpdate) {
	b.Helper()
	sub := Subscription{Schema: 1}
	u := &tracing.StateUpdate{BlockNumber: 1, StorageChanges: make(map[common.Address]map[common.Hash]*tracing.StorageChange)}
	for i := 1; i <= 1075; i++ {
		address := common.BigToAddress(big.NewInt(int64(i)))
		dependency := Dependency{Address: address}
		for j := 1; j <= 34; j++ {
			dependency.Slots = append(dependency.Slots, common.BigToHash(big.NewInt(int64(j))))
		}
		sub.Accounts = append(sub.Accounts, dependency)
		if i <= changed {
			u.StorageChanges[address] = map[common.Hash]*tracing.StorageChange{
				dependency.Slots[0]: {New: common.HexToHash("0x01")},
				dependency.Slots[1]: {New: common.HexToHash("0x02")},
			}
		}
	}
	s, err := NewWithSubscription(DefaultConfig, sub)
	if err != nil {
		b.Fatal(err)
	}
	head := &types.Header{Number: big.NewInt(0), Difficulty: big.NewInt(0), BaseFee: big.NewInt(1)}
	s.Attach(nil, head, big.NewInt(42161))
	h := types.CopyHeader(head)
	h.Number = big.NewInt(1)
	h.ParentHash = head.Hash()
	s.Begin(h, true) // Build the subscription lookup before timing steady-state work.
	return s, h, u
}

func BenchmarkCapture(b *testing.B) {
	for _, changed := range []int{0, 1, 4, 12, 128, 1075} {
		b.Run(fmt.Sprintf("changed_%d", changed), func(b *testing.B) {
			s, h, update := performanceFixture(b, changed)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Begin(h, true)
				s.capture(update)
			}
			b.StopTimer()
			if s.captureError || s.staged == nil || len(s.staged.Accounts) != changed {
				b.Fatal("incorrect captured account count")
			}
		})
	}
}

func BenchmarkEncodeDelta(b *testing.B) {
	for _, changed := range []int{0, 4, 12, 1075} {
		b.Run(fmt.Sprintf("changed_%d", changed), func(b *testing.B) {
			s, _, update := performanceFixture(b, changed)
			s.capture(update)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := json.Marshal(s.staged); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReadLatestDelta(b *testing.B) {
	s, h, update := performanceFixture(b, 4)
	s.capture(update)
	s.Publish(h)
	// Keep all default history entries so this includes the history scan that a
	// caught-up reader currently pays. Payloads are immutable and shared.
	encoded := s.history[0].data
	for sequence := uint64(2); sequence <= 256; sequence++ {
		s.history = append(s.history, entry{sequence, encoded})
	}
	s.sequence = 256
	handler := s.Handler()
	path := updatesURL(s, s.epoch, "255")
	request := httptest.NewRequest("GET", path, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != 200 {
			b.Fatal(response.Code)
		}
	}
}

func TestCaptureOrdersOnlySubscribedChanges(t *testing.T) {
	addresses := []common.Address{common.HexToAddress("0x04"), common.HexToAddress("0x02"), common.HexToAddress("0x03"), common.HexToAddress("0x01")}
	sub := Subscription{Schema: 1}
	for _, address := range addresses {
		sub.Accounts = append(sub.Accounts, Dependency{Address: address, Slots: []common.Hash{slot, hidden}})
	}
	s, err := NewWithSubscription(DefaultConfig, sub)
	if err != nil {
		t.Fatal(err)
	}
	head := &types.Header{Number: big.NewInt(0), Difficulty: big.NewInt(0), BaseFee: big.NewInt(1)}
	s.Attach(nil, head, big.NewInt(42161))
	h := types.CopyHeader(head)
	h.Number = big.NewInt(1)
	h.ParentHash = head.Hash()
	s.Begin(h, true)
	s.capture(&tracing.StateUpdate{
		BlockNumber: 1,
		AccountChanges: map[common.Address]*tracing.AccountChange{
			addresses[0]: {}, // Deletion clears every subscribed field and slot.
		},
		StorageChanges: map[common.Address]map[common.Hash]*tracing.StorageChange{
			addresses[1]:                {slot: {New: common.Hash{}}}, // Explicit zero remains present.
			common.HexToAddress("0xff"): {slot: {New: common.HexToHash("0x99")}},
		},
		CodeChanges: map[common.Address]*tracing.CodeChange{
			addresses[3]: {New: &tracing.ContractCode{Code: []byte{0x00}}},
		},
	})
	s.Publish(h)
	if s.sequence != 1 {
		t.Fatal("valid capture reset")
	}
	var envelope Envelope
	if err := json.Unmarshal(s.history[0].data, &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Accounts) != 3 {
		t.Fatalf("unexpected accounts: %+v", envelope.Accounts)
	}
	for i, expected := range []common.Address{addresses[3], addresses[1], addresses[0]} {
		if envelope.Accounts[i].Address != expected {
			t.Fatalf("account %d out of order", i)
		}
	}
	if envelope.Accounts[0].Code == nil || len(*envelope.Accounts[0].Code) != 1 {
		t.Fatal("code-only change omitted")
	}
	if value, present := envelope.Accounts[1].Storage[slot]; !present || value != (common.Hash{}) {
		t.Fatal("explicit zero omitted")
	}
	deleted := envelope.Accounts[2]
	if deleted.Exists == nil || *deleted.Exists || deleted.Code == nil || len(*deleted.Code) != 0 || deleted.Balance == nil || deleted.Balance.ToInt().Sign() != 0 || deleted.Nonce == nil || *deleted.Nonce != 0 || len(deleted.Storage) != 2 {
		t.Fatal("incomplete account deletion")
	}
}

func TestCaptureOrderingDensity(t *testing.T) {
	for _, changed := range []int{0, 1, 4, 12, 128, 1075} {
		t.Run(fmt.Sprintf("changed_%d", changed), func(t *testing.T) {
			s, _, update := performanceFixture(t, changed)
			s.capture(update)
			if s.captureError || s.staged == nil || len(s.staged.Accounts) != changed {
				t.Fatal("incorrect captured account count")
			}
			for i, account := range s.staged.Accounts {
				if expected := common.BigToAddress(big.NewInt(int64(i + 1))); account.Address != expected {
					t.Fatalf("account %d: got %s, want %s", i, account.Address, expected)
				}
			}
		})
	}
}
