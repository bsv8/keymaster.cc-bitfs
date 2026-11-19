package seller

import (
	"context"
)

// SessionStore persists seller state. Missing entries must match fs.ErrNotExist.
// SaveExact atomically accepts the first value and accepts later identical writes.
type SessionStore interface {
	SaveExact(name string, data []byte) error
	Save(name string, data []byte) error
	Load(name string) ([]byte, error)
	Exists(name string) (bool, error)
	List(prefix string) ([]string, error)
}

type CatalogEntry struct {
	Path         string
	RelativePath string
	Name         string
	Size         uint64
	SeedHashHex  string
	BlockCount   uint64
	Seed         []byte
}

// Catalog supplies indexed content. Path and RelativePath are stable
// identifiers supplied by the host and persisted with the session.
type Catalog interface {
	Preferred(context.Context, string) (*CatalogEntry, error)
	ReadBlock(context.Context, *CatalogEntry, []byte) ([]byte, error)
}

type Pricing interface {
	Effective(relativePath string) (seedPrice, blockPrice string, override bool)
}

type ArbitrationFee struct {
	BaseSat    uint64
	PerItemSat uint64
}

type ArbitrationFees func(context.Context, []byte) (ArbitrationFee, error)

// CloseSubmission contains the exact signed transaction and its input evidence.
// The host reports an uncertain broadcast with CloseBroadcastPendingError.
type CloseSubmission struct {
	SessionID        string
	TxID             string
	Raw              []byte
	FundingRaw       []byte
	LatestPaymentRaw []byte
	MinerFeeRate     uint64
}
