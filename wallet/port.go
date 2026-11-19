package wallet

import (
	"context"

	"github.com/bsv-blockchain/go-sdk/script"
)

// FundingPort lets a host reuse its wallet allocator and transaction policy.
type FundingPort interface {
	BuildWithFeeRate(context.Context, []Output, int64) (*SpendResult, error)
	Release(*SpendResult) error
	LockingScript() *script.Script
	PrepareProtocol(context.Context, string, ProtocolRequest) (*SpendResult, error)
	BroadcastPrepared(context.Context, string, *SpendResult) (string, error)
}
