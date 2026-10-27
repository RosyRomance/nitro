package statefeed

import (
	"encoding/json"
	"math/big"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

var addr = common.HexToAddress("0x1234")
var slot = common.HexToHash("0x01")
var hidden = common.HexToHash("0x02")

type backend struct{ db state.Database }

func (b backend) StateAt(h *types.Header) (*state.StateDB, error) { return state.New(h.Root, b.db) }
func fixture(t *testing.T) (*Server, *state.StateDB, backend) {
	t.Helper()
	b := backend{state.NewDatabaseForTesting()}
	db, err := state.New(types.EmptyRootHash, b.db)
	if err != nil {
		t.Fatal(err)
	}
	db.SetBalance(addr, uint256.NewInt(100), tracing.BalanceChangeUnspecified)
	db.SetCode(addr, []byte{0x00}, tracing.CodeChangeUnspecified)
	db.SetState(addr, slot, common.HexToHash("0x03"))
	root, err := db.Commit(0, true, true)
	if err != nil {
		t.Fatal(err)
	}
	h := &types.Header{Number: big.NewInt(0), Root: root, Difficulty: big.NewInt(0), BaseFee: big.NewInt(1), GasLimit: 30000000}
	cfg := DefaultConfig
	cfg.HistoryBlocks = 2
	s, err := NewWithSubscription(cfg, Subscription{Schema: 1, Accounts: []Dependency{{Address: addr, Slots: []common.Hash{slot}}}})
	if err != nil {
		t.Fatal(err)
	}
	s.Attach(b, h, big.NewInt(42161))
	db, err = b.StateAt(h)
	if err != nil {
		t.Fatal(err)
	}
	return s, db, b
}
func commit(t *testing.T, s *Server, db *state.StateDB, plain bool) *types.Header {
	t.Helper()
	h := &types.Header{Number: new(big.Int).Add(s.head.Number, big.NewInt(1)), ParentHash: s.head.Hash(), Difficulty: big.NewInt(0), BaseFee: big.NewInt(1), Time: s.head.Time + 1, GasLimit: 30000000}
	root, u, err := db.CommitWithUpdate(h.Number.Uint64(), true, plain)
	if err != nil {
		t.Fatal(err)
	}
	h.Root = root
	s.Begin(h, plain)
	tr, err := u.ToTracingUpdate()
	if err != nil {
		t.Fatal(err)
	}
	s.capture(tr)
	return h
}
func request(s *Server, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}
func updatesURL(s *Server, epoch string, after string) string {
	return "/v1/updates?epoch=" + url.QueryEscape(epoch) + "&after=" + after + "&subscription_version=" + s.version
}
func TestCommittedChangesAndSnapshot(t *testing.T) {
	for _, plain := range []bool{false, true} {
		t.Run(map[bool]string{false: "hashed", true: "plain"}[plain], func(t *testing.T) {
			s, db, _ := fixture(t)
			initial := request(s, "/v1/snapshot")
			if initial.Code != 200 {
				t.Fatal(initial.Body.String())
			}
			var seed Envelope
			if err := json.Unmarshal(initial.Body.Bytes(), &seed); err != nil {
				t.Fatal(err)
			}
			if len(seed.Accounts) != 1 || seed.Accounts[0].Storage[slot] != common.HexToHash("0x03") || seed.Accounts[0].Balance.ToInt().Uint64() != 100 {
				t.Fatal(seed)
			}
			revision := db.Snapshot()
			db.SetState(addr, slot, common.HexToHash("0x99"))
			db.RevertToSnapshot(revision)
			db.SetState(addr, hidden, common.HexToHash("0x55"))
			db.SetNonce(addr, 7, tracing.NonceChangeUnspecified)
			db.SetBalance(addr, uint256.NewInt(90), tracing.BalanceChangeUnspecified)
			db.SetCode(addr, []byte{0x60, 0x00}, tracing.CodeChangeUnspecified)
			h := commit(t, s, db, plain)
			if s.sequence != 0 || len(s.history) != 0 {
				t.Fatal("published before canonical append")
			}
			s.Publish(h)
			if s.sequence != 1 {
				t.Fatalf("capture reset unexpectedly: %+v", s)
			}
			var e Envelope
			if err := json.Unmarshal(s.history[0].data, &e); err != nil {
				t.Fatal(err)
			}
			if len(e.Accounts) != 1 || len(e.Accounts[0].Storage) != 0 || *e.Accounts[0].Nonce != 7 || e.Accounts[0].Balance.ToInt().Uint64() != 90 || len(*e.Accounts[0].Code) != 2 {
				t.Fatalf("wrong filtered final state: %+v", e)
			}
		})
	}
}
func TestExplicitZeroEmptyBlockAndWireFixture(t *testing.T) {
	s, db, b := fixture(t)
	seed := request(s, "/v1/snapshot")
	db.SetState(addr, slot, common.Hash{})
	h := commit(t, s, db, true)
	s.Publish(h)
	var delta Envelope
	if err := json.Unmarshal(s.history[0].data, &delta); err != nil {
		t.Fatal(err)
	}
	v, ok := delta.Accounts[0].Storage[slot]
	if !ok || v != (common.Hash{}) {
		t.Fatal("deleted slot must be explicitly zero")
	}
	w := request(s, updatesURL(s, s.epoch, "0"))
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if path := os.Getenv("STATEFEED_FIXTURE_OUT"); path != "" {
		data, _ := json.MarshalIndent(map[string]any{"subscription": s.sub, "snapshot": json.RawMessage(seed.Body.Bytes()), "delta": delta}, "", "  ")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	db, err := b.StateAt(h)
	if err != nil {
		t.Fatal(err)
	}
	h = commit(t, s, db, true)
	s.Publish(h)
	var empty Envelope
	if err := json.Unmarshal(s.history[1].data, &empty); err != nil {
		t.Fatal(err)
	}
	if !empty.Complete || len(empty.Accounts) != 0 || empty.Sequence != 2 {
		t.Fatal("empty block must advance completeness", empty)
	}
}
func TestHistoryExpiryAndReorgInvalidateCursor(t *testing.T) {
	s, db, b := fixture(t)
	oldEpoch := s.epoch
	for i := 0; i < 3; i++ {
		db.SetState(addr, slot, common.BigToHash(big.NewInt(int64(i+10))))
		h := commit(t, s, db, true)
		s.Publish(h)
		var err error
		db, err = b.StateAt(h)
		if err != nil {
			t.Fatal(err)
		}
	}
	if w := request(s, updatesURL(s, oldEpoch, "0")); w.Code != 409 {
		t.Fatal("expired cursor accepted", w.Code)
	}
	if w := request(s, updatesURL(s, oldEpoch, "1")); w.Code != 200 {
		t.Fatal("available cursor rejected", w.Code)
	}
	s.Reset(s.head)
	if w := request(s, updatesURL(s, oldEpoch, "3")); w.Code != 409 {
		t.Fatal("reorg cursor accepted", w.Code)
	}
	if s.epoch == oldEpoch || s.sequence != 0 || len(s.history) != 0 {
		t.Fatal("reset failed")
	}
}
func TestMissingCaptureNeverPublishesCompleteDelta(t *testing.T) {
	s, _, _ := fixture(t)
	old := s.epoch
	h := types.CopyHeader(s.head)
	h.Number = big.NewInt(1)
	h.ParentHash = s.head.Hash()
	s.Begin(h, true)
	s.Publish(h)
	if s.epoch == old || len(s.history) > 0 {
		t.Fatal("missing capture accepted")
	}
}
func TestSubscriptionDedupAndLimits(t *testing.T) {
	s := Subscription{Schema: 1, Accounts: []Dependency{{Address: addr, Slots: []common.Hash{slot, slot}}, {Address: addr, Slots: []common.Hash{slot}}}}
	if err := s.Normalize(); err != nil {
		t.Fatal(err)
	}
	if len(s.Accounts) != 1 || len(s.Accounts[0].Slots) != 1 {
		t.Fatal(s)
	}
	version := s.Version()
	if err := s.Normalize(); err != nil || s.Version() != version {
		t.Fatal("unstable subscription version")
	}
	cfg := DefaultConfig
	cfg.Enable = true
	cfg.Subscription = "x"
	cfg.Addr = "0.0.0.0"
	if cfg.Validate() == nil {
		t.Fatal("public bind accepted")
	}
}

func TestDeletedAccountClearsAllSubscribedState(t *testing.T) {
	s, db, _ := fixture(t)
	db.SelfDestruct(addr)
	h := commit(t, s, db, false)
	s.Publish(h)
	var e Envelope
	if err := json.Unmarshal(s.history[0].data, &e); err != nil {
		t.Fatal(err)
	}
	a := e.Accounts[0]
	if a.Exists == nil || *a.Exists || a.Code == nil || len(*a.Code) != 0 || a.Balance == nil || a.Balance.ToInt().Sign() != 0 || a.Nonce == nil || *a.Nonce != 0 {
		t.Fatalf("incomplete deletion: %+v", a)
	}
	if v, ok := a.Storage[slot]; !ok || v != (common.Hash{}) {
		t.Fatal("deleted account retained known slot")
	}
}
func TestOversizedDeltaForcesResnapshot(t *testing.T) {
	s, db, _ := fixture(t)
	old := s.epoch
	s.cfg.HistoryBytes = 64
	db.SetState(addr, slot, common.HexToHash("0x88"))
	h := commit(t, s, db, true)
	s.Publish(h)
	if old == s.epoch || len(s.history) != 0 {
		t.Fatal("oversized delta must invalidate cursor")
	}
	if w := request(s, updatesURL(s, old, "0")); w.Code != 409 {
		t.Fatal("oversized update silently lost")
	}
}
