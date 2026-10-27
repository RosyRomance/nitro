// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md
package statefeed

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

type Block struct {
	Number        uint64         `json:"number"`
	Hash          common.Hash    `json:"hash"`
	ParentHash    common.Hash    `json:"parent_hash"`
	StateRoot     common.Hash    `json:"state_root"`
	Timestamp     uint64         `json:"timestamp"`
	L1BlockNumber uint64         `json:"l1_block_number"`
	GasLimit      uint64         `json:"gas_limit"`
	Coinbase      common.Address `json:"coinbase"`
	Difficulty    *hexutil.Big   `json:"difficulty"`
	BaseFee       *hexutil.Big   `json:"base_fee"`
	MixDigest     common.Hash    `json:"mix_digest"`
}

func blockInfo(h *types.Header) Block {
	return Block{Number: h.Number.Uint64(), Hash: h.Hash(), ParentHash: h.ParentHash, StateRoot: h.Root, Timestamp: h.Time,
		L1BlockNumber: types.DeserializeHeaderExtraInformation(h).L1BlockNumber, GasLimit: h.GasLimit,
		Coinbase: h.Coinbase, Difficulty: (*hexutil.Big)(h.Difficulty), BaseFee: (*hexutil.Big)(h.BaseFee), MixDigest: h.MixDigest}
}

// Absent fields in a delta mean unchanged. Snapshot fields are always explicit.
// Account deletion is explicit and clears every subscribed field and slot.
type Account struct {
	Address common.Address              `json:"address"`
	Exists  *bool                       `json:"exists,omitempty"`
	Code    *hexutil.Bytes              `json:"code,omitempty"`
	Balance *hexutil.Big                `json:"balance,omitempty"`
	Nonce   *uint64                     `json:"nonce,omitempty"`
	Storage map[common.Hash]common.Hash `json:"storage"`
}
type Envelope struct {
	Schema              uint64    `json:"schema"`
	ChainID             string    `json:"chain_id"`
	Epoch               string    `json:"epoch"`
	SubscriptionVersion string    `json:"subscription_version"`
	Sequence            uint64    `json:"sequence"`
	Kind                string    `json:"kind"`
	Complete            bool      `json:"complete"`
	Block               Block     `json:"block"`
	Accounts            []Account `json:"accounts"`
}
