package seller

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/bsv8/BitFSProtocol/protocol"
	"github.com/bsv8/keymaster.cc-bitfs/discovery"
)

// AuthenticatedSession is a seller stream with its transport-authenticated buyer key.
type AuthenticatedSession interface {
	Session
	RemotePublicKey() []byte
}

type RuntimeOptions struct {
	ResolveSeller func(context.Context, string) (*Seller, error)
	OpenSession   func(context.Context, discovery.Demand) (AuthenticatedSession, error)
	MaxSessions   int
	Now           func() time.Time
	OnError       func(discovery.Demand, error)
}

// Runtime admits verified demands, creates quotes and owns seller sessions.
type Runtime struct {
	options RuntimeOptions
	ctx     context.Context
	cancel  context.CancelFunc

	mu       sync.Mutex
	stopping bool
	active   int
	workers  sync.WaitGroup
}

func NewRuntime(parent context.Context, options RuntimeOptions) (*Runtime, error) {
	if options.ResolveSeller == nil || options.OpenSession == nil {
		return nil, errors.New("bitfs: seller and transport ports are required")
	}
	if options.MaxSessions <= 0 {
		options.MaxSessions = 32
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	ctx, cancel := context.WithCancel(parent)
	return &Runtime{options: options, ctx: ctx, cancel: cancel}, nil
}

// Accept admits a demand from the host's verified discovery subscription.
func (r *Runtime) Accept(demand discovery.Demand) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping || r.ctx.Err() != nil || r.active >= r.options.MaxSessions || !demand.ExpiresAt.After(r.options.Now()) {
		return false
	}
	r.active++
	r.workers.Add(1)
	go func() {
		defer r.workers.Done()
		defer func() {
			r.mu.Lock()
			r.active--
			r.mu.Unlock()
		}()
		if err := r.serve(demand); err != nil && r.ctx.Err() == nil && r.options.OnError != nil {
			r.options.OnError(demand, err)
		}
	}()
	return true
}

func (r *Runtime) serve(demand discovery.Demand) error {
	buyerRaw, err := hex.DecodeString(demand.FromPublicKey)
	if err != nil {
		return err
	}
	buyer, err := protocol.PublicKeyFromBytes(buyerRaw)
	if err != nil {
		return err
	}
	service, err := r.options.ResolveSeller(r.ctx, demand.SeedHashHex)
	if err != nil || service == nil {
		return err
	}
	if err := service.validate(); err != nil {
		return err
	}
	session, err := r.options.OpenSession(r.ctx, demand)
	if err != nil {
		return err
	}
	defer session.Close()
	stop := context.AfterFunc(r.ctx, func() { _ = session.Close() })
	defer stop()
	if !bytes.Equal(session.RemotePublicKey(), buyerRaw) {
		return errors.New("bitfs: seller demand key differs from connected buyer")
	}
	id := "d-" + demand.MessageID
	if _, err := service.CreateQuote(r.ctx, QuoteRequest{SessionID: id,
		SellerID: hex.EncodeToString(service.Signer.PublicKey().Bytes()), BuyerPublicKey: buyer,
		SeedHashHex: demand.SeedHashHex}); err != nil {
		return err
	}
	return service.Serve(r.ctx, id, session)
}

// Stop prevents new quotes, then lets active sessions complete until the grace
// period expires. At the deadline the remaining streams are closed.
func (r *Runtime) Stop(grace context.Context) {
	r.mu.Lock()
	r.stopping = true
	r.mu.Unlock()
	done := make(chan struct{})
	go func() {
		r.workers.Wait()
		close(done)
	}()
	select {
	case <-done:
		r.cancel()
	case <-grace.Done():
		r.cancel()
		<-done
	}
}
