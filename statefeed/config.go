// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

// Package statefeed exports a bounded, fixed subscription to committed state.
// It deliberately has no knowledge of pools, quotes, signers, or transactions.
package statefeed

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/spf13/pflag"
)

type Config struct {
	Enable        bool   `koanf:"enable"`
	Addr          string `koanf:"addr"`
	Port          string `koanf:"port"`
	Subscription  string `koanf:"subscription"`
	HistoryBlocks int    `koanf:"history-blocks"`
	HistoryBytes  int    `koanf:"history-bytes"`
}

var DefaultConfig = Config{Addr: "127.0.0.1", Port: "9647", HistoryBlocks: 256, HistoryBytes: 64 << 20}

func AddOptions(prefix string, f *pflag.FlagSet) {
	f.Bool(prefix+".enable", DefaultConfig.Enable, "export committed subscribed state")
	f.String(prefix+".addr", DefaultConfig.Addr, "loopback state feed listen address")
	f.String(prefix+".port", DefaultConfig.Port, "state feed HTTP port")
	f.String(prefix+".subscription", "", "fixed subscription JSON file; changing it requires restart")
	f.Int(prefix+".history-blocks", DefaultConfig.HistoryBlocks, "maximum retained state updates")
	f.Int(prefix+".history-bytes", DefaultConfig.HistoryBytes, "maximum retained encoded update bytes")
}
func (c Config) Validate() error {
	if !c.Enable {
		return nil
	}
	ip := net.ParseIP(c.Addr)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("state feed requires a loopback listen address")
	}
	if c.Port == "" || c.Subscription == "" || c.HistoryBlocks < 1 || c.HistoryBlocks > 16384 || c.HistoryBytes < 1024 || c.HistoryBytes > 1<<30 {
		return fmt.Errorf("invalid state feed limits or subscription")
	}
	return nil
}

type Dependency struct {
	Address common.Address `json:"address"`
	Slots   []common.Hash  `json:"slots"`
}
type Subscription struct {
	Schema   uint64       `json:"schema"`
	Accounts []Dependency `json:"accounts"`
}

func (s *Subscription) Normalize() error {
	if s.Schema != 1 || len(s.Accounts) == 0 || len(s.Accounts) > 4096 {
		return fmt.Errorf("invalid subscription schema/account count")
	}
	merged := make(map[common.Address]map[common.Hash]bool)
	slots := 0
	for _, a := range s.Accounts {
		if merged[a.Address] == nil {
			merged[a.Address] = make(map[common.Hash]bool)
		}
		for _, k := range a.Slots {
			if !merged[a.Address][k] {
				slots++
				merged[a.Address][k] = true
			}
			if slots > 65536 {
				return fmt.Errorf("subscription exceeds 65536 slots")
			}
		}
	}
	s.Accounts = make([]Dependency, 0, len(merged))
	for address, keys := range merged {
		a := Dependency{Address: address, Slots: make([]common.Hash, 0, len(keys))}
		for k := range keys {
			a.Slots = append(a.Slots, k)
		}
		sort.Slice(a.Slots, func(i, j int) bool { return a.Slots[i].Cmp(a.Slots[j]) < 0 })
		s.Accounts = append(s.Accounts, a)
	}
	sort.Slice(s.Accounts, func(i, j int) bool { return s.Accounts[i].Address.Cmp(s.Accounts[j].Address) < 0 })
	return nil
}
func (s Subscription) Version() string {
	data, _ := json.Marshal(s)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}
func LoadSubscription(path string) (Subscription, error) {
	var s Subscription
	f, err := os.Open(path)
	if err != nil {
		return s, err
	}
	defer f.Close()
	dec := json.NewDecoder(io.LimitReader(f, 8<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return s, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return s, fmt.Errorf("subscription must contain one JSON object")
	}
	return s, s.Normalize()
}
