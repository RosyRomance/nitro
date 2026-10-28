// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md
package statefeed

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
)

type Backend interface {
	StateAt(*types.Header) (*state.StateDB, error)
}
type entry struct {
	sequence uint64
	data     json.RawMessage
}
type Server struct {
	mu           sync.Mutex
	cfg          Config
	sub          Subscription
	version      string
	chainID      string
	epoch        string
	sequence     uint64
	head         *types.Header
	backend      Backend
	pending      *types.Header
	plainKeys    bool
	staged       *Envelope
	captureError bool
	watch        map[common.Address]map[common.Hash]common.Hash // raw or hashed key -> raw key, rebuilt at Begin
	history      []entry
	historyBytes int
	changed      chan struct{}
	http         *http.Server
	hooks        *tracing.Hooks
	resets       uint64
	blocks       uint64
}

func New(c Config) (*Server, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	sub, err := LoadSubscription(c.Subscription)
	if err != nil {
		return nil, err
	}
	return NewWithSubscription(c, sub)
}
func NewWithSubscription(c Config, sub Subscription) (*Server, error) {
	if err := sub.Normalize(); err != nil {
		return nil, err
	}
	s := &Server{cfg: c, sub: sub, version: sub.Version(), changed: make(chan struct{})}
	s.hooks = &tracing.Hooks{OnStateUpdate: s.capture}
	return s, nil
}
func (s *Server) Hooks() *tracing.Hooks              { return s.hooks }
func (s *Server) IsCommitOnly(h *tracing.Hooks) bool { return h == s.hooks }
func (s *Server) Attach(backend Backend, head *types.Header, chainID *big.Int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.backend = backend
	s.chainID = chainID.String()
	s.resetLocked(head)
}
func (s *Server) resetLocked(head *types.Header) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	s.epoch = hex.EncodeToString(b[:])
	s.sequence = 0
	s.head = types.CopyHeader(head)
	s.history = nil
	s.historyBytes = 0
	s.staged = nil
	s.pending = nil
	s.resets++
	close(s.changed)
	s.changed = make(chan struct{})
}

// Reset invalidates old cursors immediately, including reorgs with no replacement block.
func (s *Server) Reset(head *types.Header) { s.mu.Lock(); defer s.mu.Unlock(); s.resetLocked(head) }

// Begin is called only for a real append, never speculative execution/prefetch.
func (s *Server) Begin(h *types.Header, plainKeys bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = types.CopyHeader(h)
	s.staged = nil
	s.captureError = false
	// Mapping is stable across all blocks in one storage-key era.
	if s.watch == nil || s.plainKeys != plainKeys {
		s.plainKeys = plainKeys
		s.watch = make(map[common.Address]map[common.Hash]common.Hash, len(s.sub.Accounts))
		for _, a := range s.sub.Accounts {
			keys := make(map[common.Hash]common.Hash, len(a.Slots))
			for _, k := range a.Slots {
				index := k
				if !plainKeys {
					index = crypto.Keccak256Hash(k[:])
				}
				keys[index] = k
			}
			s.watch[a.Address] = keys
		}
	}
}
func (s *Server) capture(u *tracing.StateUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil {
		return
	} // initialization/import is not a published append
	if s.staged != nil || u.BlockNumber != s.pending.Number.Uint64() || u.Root != s.pending.Root {
		s.captureError = true
		return
	}
	e := &Envelope{Schema: 1, ChainID: s.chainID, Epoch: s.epoch, SubscriptionVersion: s.version, Kind: "delta", Complete: true, Block: blockInfo(s.pending), Accounts: make([]Account, 0)}
	touched := make(map[common.Address]*Account)
	get := func(a common.Address) *Account {
		if touched[a] == nil {
			touched[a] = &Account{Address: a, Storage: make(map[common.Hash]common.Hash)}
		}
		return touched[a]
	}
	// Iterate actual changes, never poll all watched values.
	for a, c := range u.AccountChanges {
		if _, ok := s.watch[a]; !ok {
			continue
		}
		out := get(a)
		exists := c.New != nil
		out.Exists = &exists
		if !exists {
			code := hexutil.Bytes{}
			out.Code = &code
			out.Balance = (*hexutil.Big)(new(big.Int))
			nonce := uint64(0)
			out.Nonce = &nonce
			for _, k := range s.watch[a] {
				out.Storage[k] = common.Hash{}
			}
		} else {
			out.Balance = (*hexutil.Big)(c.New.Balance.ToBig())
			nonce := c.New.Nonce
			out.Nonce = &nonce
			if c.Prev == nil && common.BytesToHash(c.New.CodeHash) == types.EmptyCodeHash {
				code := hexutil.Bytes{}
				out.Code = &code
			}
		}
	}
	for a, changes := range u.StorageChanges {
		keys, ok := s.watch[a]
		if !ok {
			continue
		}
		for index, c := range changes {
			if raw, ok := keys[index]; ok {
				get(a).Storage[raw] = c.New
			}
		}
	}
	for a, c := range u.CodeChanges {
		if _, ok := s.watch[a]; !ok {
			continue
		}
		code := hexutil.Bytes{}
		if c.New != nil {
			code = append(code, c.New.Code...)
		}
		get(a).Code = &code
	}
	// Stable ordering makes captures reproducible. Most blocks touch only a
	// small fraction of the subscription, so avoid scanning every watched
	// account solely to order the few changes.
	e.Accounts = make([]Account, 0, len(touched))
	if len(touched)*16 < len(s.sub.Accounts) {
		for _, out := range touched {
			e.Accounts = append(e.Accounts, *out)
		}
		slices.SortFunc(e.Accounts, func(a, b Account) int { return a.Address.Cmp(b.Address) })
	} else {
		// Dense blocks are cheaper to order with the already-sorted
		// subscription than with O(changes * log(changes)) comparisons.
		for _, a := range s.sub.Accounts {
			if out := touched[a.Address]; out != nil {
				e.Accounts = append(e.Accounts, *out)
			}
		}
	}
	if s.head == nil || u.OriginRoot != s.head.Root {
		s.captureError = true
	}
	s.staged = e
}

// Publish runs after WriteBlockAndSetHead succeeded. Commit callbacks alone are
// insufficient: they can precede canonical head selection.
func (s *Server) Publish(h *types.Header) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.captureError || s.staged == nil || s.head == nil || h.ParentHash != s.head.Hash() || h.Number.Uint64() != s.head.Number.Uint64()+1 || s.staged.Block.Hash != h.Hash() {
		s.resetLocked(h)
		return // fail closed; next client read requires a fresh snapshot
	}
	s.sequence++
	s.staged.Sequence = s.sequence
	data, err := json.Marshal(s.staged)
	if err != nil || len(data) > s.cfg.HistoryBytes {
		s.resetLocked(h)
		return
	}
	s.history = append(s.history, entry{s.sequence, data})
	s.historyBytes += len(data)
	for len(s.history) > s.cfg.HistoryBlocks || s.historyBytes > s.cfg.HistoryBytes {
		s.historyBytes -= len(s.history[0].data)
		s.history[0] = entry{}
		s.history = s.history[1:]
	}
	s.head = types.CopyHeader(h)
	s.pending = nil
	s.staged = nil
	s.blocks++
	close(s.changed)
	s.changed = make(chan struct{})
}
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", net.JoinHostPort(s.cfg.Addr, s.cfg.Port))
	if err != nil {
		return err
	}
	s.http = &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	go func() {
		if err := s.http.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("State feed HTTP server stopped", "err", err)
		}
	}()
	return nil
}
func (s *Server) Close() {
	if s.http != nil {
		_ = s.http.Close()
	}
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/snapshot", s.snapshot)
	mux.HandleFunc("/v1/updates", s.updates)
	mux.HandleFunc("/v1/health", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		writeJSON(w, map[string]any{"schema": 1, "epoch": s.epoch, "subscription_version": s.version, "sequence": s.sequence, "blocks": s.blocks, "resets": s.resets, "history_bytes": s.historyBytes})
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet {
			http.Error(w, "GET required", http.StatusMethodNotAllowed)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.head == nil || s.backend == nil {
		s.mu.Unlock()
		http.Error(w, "not initialized", 503)
		return
	}
	h := types.CopyHeader(s.head)
	seq, epoch := s.sequence, s.epoch
	backend := s.backend
	s.mu.Unlock()
	db, err := backend.StateAt(h)
	if err != nil {
		http.Error(w, "snapshot state unavailable", 409)
		return
	}
	e := Envelope{Schema: 1, ChainID: s.chainID, Epoch: epoch, SubscriptionVersion: s.version, Sequence: seq, Kind: "snapshot", Complete: true, Block: blockInfo(h), Accounts: make([]Account, 0, len(s.sub.Accounts))}
	for _, a := range s.sub.Accounts {
		if r.Context().Err() != nil {
			return
		}
		exists := db.Exist(a.Address)
		code := hexutil.Bytes(append([]byte{}, db.GetCode(a.Address)...))
		nonce := db.GetNonce(a.Address)
		out := Account{Address: a.Address, Exists: &exists, Code: &code, Balance: (*hexutil.Big)(db.GetBalance(a.Address).ToBig()), Nonce: &nonce, Storage: make(map[common.Hash]common.Hash, len(a.Slots))}
		for _, k := range a.Slots {
			out.Storage[k] = db.GetState(a.Address, k)
		}
		e.Accounts = append(e.Accounts, out)
	}
	if db.Error() != nil {
		http.Error(w, "snapshot read failed", 503)
		return
	}
	s.mu.Lock()
	valid := epoch == s.epoch && s.availableLocked(seq)
	s.mu.Unlock()
	if !valid {
		http.Error(w, "snapshot raced reset or history expiry; retry", 409)
		return
	}
	writeJSON(w, e)
}
func (s *Server) availableLocked(after uint64) bool {
	return after <= s.sequence && (after == s.sequence || (len(s.history) > 0 && after >= s.history[0].sequence-1))
}
func (s *Server) updates(w http.ResponseWriter, r *http.Request) {
	after, err := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
	if err != nil {
		http.Error(w, "invalid cursor", 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	for {
		s.mu.Lock()
		if r.URL.Query().Get("epoch") != s.epoch || r.URL.Query().Get("subscription_version") != s.version || !s.availableLocked(after) {
			s.mu.Unlock()
			http.Error(w, "reset required", 409)
			return
		}
		out := make([]json.RawMessage, 0, 64)
		for _, e := range s.history {
			if e.sequence > after {
				out = append(out, e.data)
				if len(out) == 64 {
					break
				}
			}
		}
		changed := s.changed
		s.mu.Unlock()
		if len(out) > 0 {
			writeJSON(w, map[string]any{"updates": out})
			return
		}
		select {
		case <-changed:
			continue
		case <-ctx.Done():
			if r.Context().Err() == nil {
				writeJSON(w, map[string]any{"updates": out})
			}
			return
		}
	}
}
func (s *Server) String() string { return fmt.Sprintf("statefeed/%s", s.version) }
