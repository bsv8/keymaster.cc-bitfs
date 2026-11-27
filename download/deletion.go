package download

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/bsv8/keymaster.cc-bitfs/host"
	"github.com/bsv8/keymaster.cc-bitfs/ledger"
	"github.com/bsv8/keymaster.cc-bitfs/store"
)

func validateSeed(seed string) error {
	bytes, err := hex.DecodeString(seed)
	if err != nil || len(bytes) != 32 || seed != strings.ToLower(seed) {
		return errors.New("download: Seed hash must be 32 lowercase hex bytes")
	}
	return nil
}

const deletionMarker = "scheduler/deleting"
const downloadIntent = "scheduler/download.intent"

var ErrDeletionPending = errors.New("download: deletion requires recovery or cleanup")

type DeleteOptions struct {
	SeedHash string
	Store    store.Repository
	Ledger   func() (*ledger.Ledger, error)
	Lock     func(context.Context, string) (release func(), err error)
}

func checkWorkspace(repo store.Repository) error {
	deleting, err := repo.Exists(deletionMarker)
	if err != nil {
		return err
	}
	if deleting {
		return ErrDeletionPending
	}
	return nil
}

// Delete removes the selected workspace's staging data and execution pointer.
// The account and protocol evidence remain available to the recovery worker.
func Delete(ctx context.Context, options DeleteOptions) error {
	if options.Store == nil || options.Lock == nil || options.Ledger == nil {
		return errors.New("download: store, ledger and lock are required")
	}
	if err := validateSeed(options.SeedHash); err != nil {
		return err
	}
	release, err := options.Lock(ctx, options.SeedHash)
	if err != nil {
		return err
	}
	defer release()
	if err := options.Store.Save(deletionMarker, []byte(options.SeedHash)); err != nil {
		return err
	}
	names, err := options.Store.List("staging/")
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := options.Store.Remove(name); err != nil {
			return err
		}
	}
	account, err := options.Ledger()
	if err != nil {
		return err
	}
	for _, pool := range account.Pools() {
		if pool.State != ledger.PoolClosed && pool.State != ledger.PoolFundingFailed {
			return fmt.Errorf("%w: pool %s is %s", ErrDeletionPending, pool.ID, pool.State)
		}
	}
	if len(account.Unresolved()) != 0 {
		return ErrDeletionPending
	}
	execs, err := host.ListExecs(options.Store, options.SeedHash)
	if err != nil {
		return err
	}
	for _, id := range execs {
		state, _, err := host.LoadExecSnapshot(options.Store, options.SeedHash, id)
		if err != nil {
			return err
		}
		for _, request := range state.Requests {
			if request.State == "pending" || request.State == "unresolved" {
				return fmt.Errorf("%w: purchase authorization %s", ErrDeletionPending, request.ID)
			}
		}
	}
	for _, id := range execs {
		if err := options.Store.Remove(playbackQuotePath(options.SeedHash, id)); err != nil {
			return err
		}
	}
	if err := host.RetireExecutions(options.Store, options.SeedHash); err != nil {
		return err
	}
	for _, name := range []string{"output/final.json", "output/preferred-name", "output/pending.json", "output/rename.json"} {
		if err := options.Store.Remove(name); err != nil {
			return err
		}
	}
	if err := options.Store.Remove(downloadIntent); err != nil {
		return err
	}
	return options.Store.Remove(deletionMarker)
}
