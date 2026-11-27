// Command bitfs runs the Go BitFS buyer or seller with filesystem and libp2p adapters.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	tx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BitFSProtocol/protocol"
	channels "github.com/bsv8/ChannelProtocol"
	"github.com/bsv8/ChannelProtocol/hashrequest"
	"github.com/bsv8/bitcoin-libp2p/golibp2p"
	"github.com/bsv8/bitcoin-libp2p/signer"
	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/transport/websocket"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/bsv8/keymaster.cc-bitfs/arbitration"
	"github.com/bsv8/keymaster.cc-bitfs/bitfs"
	"github.com/bsv8/keymaster.cc-bitfs/discovery"
	"github.com/bsv8/keymaster.cc-bitfs/download"
	"github.com/bsv8/keymaster.cc-bitfs/internal/cli/catalog"
	"github.com/bsv8/keymaster.cc-bitfs/internal/cli/pricing"
	"github.com/bsv8/keymaster.cc-bitfs/ledger"
	"github.com/bsv8/keymaster.cc-bitfs/lock"
	"github.com/bsv8/keymaster.cc-bitfs/output"
	"github.com/bsv8/keymaster.cc-bitfs/seller"
	"github.com/bsv8/keymaster.cc-bitfs/ssp"
	"github.com/bsv8/keymaster.cc-bitfs/store"
	"github.com/bsv8/keymaster.cc-bitfs/transport"
	"github.com/bsv8/keymaster.cc-bitfs/wallet"
	"github.com/bsv8/keymaster.cc-bitfs/woc"
	"github.com/bsv8/keymaster.cc-download-scheduler/scheduler"
)

type config struct {
	Network        string
	IdentityFile   string
	ChainURL       string
	Supplier       string
	Listen         string
	Arbiters       []arbitration.Endpoint
	FeeRate        int64
	MaxBlockPrice  string
	SeatsPerSeller int64
	TradeDir       string
	OutputDir      string
	SellerDir      string
	SeedPrice      string
	BlockPrice     string
}

type runtime struct {
	config
	key      []byte
	chain    *woc.Client
	wallet   *wallet.Wallet
	signer   protocol.Signer
	gateway  *ssp.Client
	host     host.Host
	arbiters *arbitration.Client
}

func main() {
	log.SetFlags(0)
	if err := run(os.Args[1:]); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("bitfs", flag.ContinueOnError)
	configPath := flags.String("config", "", "configuration JSON path")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "usage: bitfs --config FILE get|resume|delete SEED or bitfs --config FILE serve")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	args = flags.Args()
	if len(args) == 0 || *configPath == "" {
		flags.Usage()
		return errors.New("bitfs: config and command are required")
	}
	raw, err := os.ReadFile(*configPath)
	if err != nil {
		return err
	}
	cfg := config{Network: "test", Listen: "/ip4/127.0.0.1/tcp/0", FeeRate: 100, SeatsPerSeller: 1, SeedPrice: "1", BlockPrice: "1"}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	if cfg.TradeDir == "" {
		return errors.New("bitfs: TradeDir is required")
	}
	stop, cancelStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelStop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if args[0] == "serve" {
		if len(args) != 1 {
			return errors.New("bitfs: serve takes no seed")
		}
		rt, err := openRuntime(ctx, cfg)
		if err != nil {
			return err
		}
		defer rt.close()
		return rt.serve(ctx, stop.Done())
	}
	if len(args) != 2 || (args[0] != "get" && args[0] != "resume" && args[0] != "delete") {
		return errors.New("bitfs: expected get, resume, or delete with a seed")
	}
	seed := args[1]
	decoded, err := hex.DecodeString(seed)
	if err != nil || len(decoded) != 32 || seed != strings.ToLower(seed) {
		return errors.New("bitfs: invalid Seed Hash")
	}
	work, err := store.Open(filepath.Join(cfg.TradeDir, "downloads", seed))
	if err != nil {
		return err
	}
	lockWorkspace := func(_ context.Context, _ string) (func(), error) {
		guard, err := lock.Acquire(filepath.Join(work.Root(), "scheduler", ".active.lock"))
		if err != nil {
			return nil, err
		}
		return func() {
			if err := guard.Release(); err != nil {
				log.Print(err)
			}
		}, nil
	}
	loadLedger := func() (*ledger.Ledger, error) {
		return ledger.OpenRepository(work, "ledger.json", seed, cfg.Network, seed)
	}
	if args[0] == "delete" {
		err := download.Delete(ctx, download.DeleteOptions{SeedHash: seed, Store: work, Ledger: loadLedger, Lock: lockWorkspace})
		if errors.Is(err, download.ErrDeletionPending) {
			return json.NewEncoder(os.Stdout).Encode(map[string]string{"state": "recovery-pending", "seed": seed, "detail": err.Error()})
		}
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"state": "deleted", "seed": seed})
	}
	rt, err := openRuntime(ctx, cfg)
	if err != nil {
		return err
	}
	defer rt.close()
	return rt.get(ctx, stop.Done(), seed, args[0] == "resume", work, lockWorkspace, loadLedger)
}

func openRuntime(ctx context.Context, cfg config) (*runtime, error) {
	if cfg.IdentityFile == "" || cfg.ChainURL == "" || cfg.Supplier == "" {
		return nil, errors.New("bitfs: IdentityFile, ChainURL and Supplier are required")
	}
	encoded, err := os.ReadFile(cfg.IdentityFile)
	if err != nil {
		return nil, err
	}
	key, err := hex.DecodeString(strings.TrimSpace(string(encoded)))
	if err != nil || len(key) != 32 {
		return nil, errors.New("bitfs: identity file requires a 32-byte hex key")
	}
	private, _ := ec.PrivateKeyFromBytes(key)
	tradeSigner, err := protocol.NewPrivateKeySigner(private)
	if err != nil {
		return nil, err
	}
	transportSigner, err := signer.NewLocalSigner(key)
	if err != nil {
		return nil, err
	}
	chain := woc.New(cfg.ChainURL, os.Getenv("BITFS_CHAIN_API_KEY"))
	w, err := wallet.New(chain, wallet.Options{Network: cfg.Network, PrivateKey: key, FeeRateSatoshisPerKb: cfg.FeeRate})
	if err != nil {
		return nil, err
	}
	listen, err := ma.NewMultiaddr(cfg.Listen)
	if err != nil {
		return nil, err
	}
	h, err := golibp2p.NewHost(golibp2p.HostConfig{Signer: transportSigner, ListenAddrs: []ma.Multiaddr{listen}, TransportOptions: []libp2p.Option{libp2p.Transport(websocket.New)}})
	if err != nil {
		return nil, err
	}
	gateway, err := ssp.Dial(ctx, ssp.Options{Gateway: cfg.Supplier, Signer: transportSigner})
	if err != nil {
		_ = h.Close()
		return nil, err
	}
	return &runtime{config: cfg, key: key, chain: chain, wallet: w, signer: tradeSigner,
		gateway: gateway, host: h, arbiters: &arbitration.Client{Host: h, Endpoints: cfg.Arbiters}}, nil
}

func (r *runtime) close() {
	_ = r.gateway.Close()
	_ = r.host.Close()
	r.wallet.Close()
	clear(r.key)
}

func (r *runtime) facts(ctx context.Context) (func() protocol.Facts, error) {
	height, err := r.chain.ChainHeight(ctx)
	if err != nil {
		return nil, err
	}
	var tip atomic.Uint32
	tip.Store(height)
	go func() {
		timer := time.NewTicker(15 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				current, err := r.chain.ChainHeight(ctx)
				if err != nil {
					log.Printf("bitfs: refresh chain height: %v", err)
					continue
				}
				tip.Store(current)
			}
		}
	}()
	return func() protocol.Facts {
		return protocol.Facts{Now: time.Now(), BlockHeight: protocol.BlockHeight(tip.Load())}
	}, nil
}

func (r *runtime) get(ctx context.Context, stop <-chan struct{}, seed string, resume bool, work store.Repository,
	lockWorkspace func(context.Context, string) (func(), error), loadLedger func() (*ledger.Ledger, error),
) error {
	if r.OutputDir == "" {
		return errors.New("bitfs: OutputDir is required for get and resume")
	}
	facts, err := r.facts(ctx)
	if err != nil {
		return err
	}
	assembler, err := output.New(work, r.OutputDir)
	if err != nil {
		return err
	}
	addresses := r.host.Addrs()
	if len(addresses) == 0 {
		return errors.New("bitfs: no listen address")
	}
	locator, err := hashrequest.NewMultiaddrLocator(addresses[0].Encapsulate(ma.StringCast("/p2p/" + r.host.ID().String())).String())
	if err != nil {
		return err
	}
	channelKey, err := channels.ParsePrivateKey(r.key)
	if err != nil {
		return err
	}
	channelPublic, err := channels.PublicKeyFromPrivate(channelKey)
	if err != nil {
		return err
	}
	publisher := discovery.Publisher{
		PrivateKey: channelKey, PublicKey: channelPublic, Locators: []hashrequest.Locator{locator},
		Publish: func(ctx context.Context, channel string, body []byte) error {
			result, err := r.gateway.Publish(ctx, channel, body)
			return gatewayResult(result, err)
		},
	}
	incoming := make(chan *transport.Session, 8)
	if err := transport.Listen(r.host, func(session *transport.Session) {
		select {
		case incoming <- session:
		default:
			_ = session.Close()
		}
	}); err != nil {
		return err
	}
	result, err := download.Run(ctx, download.Options{
		SeedHash: seed, Store: work, Output: assembler, Lock: lockWorkspace, Stop: stop, RequireExisting: resume,
		Arbitration: r.arbiters,
		OpenTask: func() (*bitfs.BuyerTask, error) {
			account, err := loadLedger()
			if err != nil {
				return nil, err
			}
			return &bitfs.BuyerTask{
				Wallet: r.wallet, Ledger: account, Store: work, Signer: r.signer, Facts: facts, Chain: r.chain,
				MinerFeeRate: protocol.SatoshisPerKilobyte(r.FeeRate), WalletFeeRateSatoshisPerKb: r.FeeRate,
			}, nil
		},
		Accept: func(ctx context.Context) (download.Session, error) {
			select {
			case session := <-incoming:
				return session, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		RequestQuotes: func(ctx context.Context) error {
			_, err := publisher.PublishDemand(ctx, seed, discovery.MaxDemandLifetime)
			return err
		},
		Config: scheduler.Config{MaxBlockPrice: r.MaxBlockPrice, SeatsPerSeller: r.SeatsPerSeller},
		OnView: func(view scheduler.View) {
			if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
				"state": view.Execution, "funds": view.Funds, "completed": view.Completed, "total": view.Total,
			}); err != nil {
				log.Print(err)
			}
		},
		Logf: log.Printf,
	})
	if errors.Is(err, download.ErrPaused) {
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"state": "paused", "seed": seed})
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"state": "completed", "seed": seed, "path": result.Path, "bytes": result.Bytes, "committed": result.Committed,
	})
}

func gatewayResult(result ssp.Result, err error) error {
	if err != nil {
		return err
	}
	if !result.Success {
		return fmt.Errorf("bitfs: gateway rejected request (%s)", result.ErrorCode)
	}
	return nil
}

type catalogAdapter struct{ catalog *catalog.Catalog }

func (c catalogAdapter) Preferred(ctx context.Context, seed string) (*seller.CatalogEntry, error) {
	entry, err := c.catalog.Preferred(ctx, seed)
	if err != nil || entry == nil {
		return nil, err
	}
	return &seller.CatalogEntry{Path: entry.Path, RelativePath: entry.RelativePath, Name: entry.Name,
		Size: entry.Size, SeedHashHex: entry.SeedHashHex, BlockCount: entry.BlockCount, Seed: entry.Seed}, nil
}

func (c catalogAdapter) ReadBlock(ctx context.Context, entry *seller.CatalogEntry, hash []byte) ([]byte, error) {
	return c.catalog.ReadBlock(ctx, &catalog.Entry{Path: entry.Path, RelativePath: entry.RelativePath,
		Name: entry.Name, Size: entry.Size, SeedHashHex: entry.SeedHashHex, BlockCount: entry.BlockCount, Seed: entry.Seed}, hash)
}

func (r *runtime) serve(ctx context.Context, stop <-chan struct{}) error {
	if r.SellerDir == "" {
		return errors.New("bitfs: SellerDir is required")
	}
	sessions, err := store.Open(filepath.Join(r.TradeDir, "seller"))
	if err != nil {
		return err
	}
	guard, err := lock.Acquire(filepath.Join(sessions.Root(), ".active.lock"))
	if err != nil {
		return err
	}
	defer guard.Release()
	db, err := catalog.OpenDatabase(ctx, filepath.Join(r.TradeDir, "catalog.sqlite"))
	if err != nil {
		return err
	}
	defer db.Close()
	index, err := db.OpenRoot(ctx, "standalone", r.SellerDir)
	if err != nil {
		return err
	}
	defer index.Close()
	if _, err := index.Refresh(ctx, false); err != nil {
		return err
	}
	prices, err := pricing.Open(filepath.Join(r.TradeDir, "pricing.json"), r.SeedPrice, r.BlockPrice)
	if err != nil {
		return err
	}
	facts, err := r.facts(ctx)
	if err != nil {
		return err
	}
	var arbiters []protocol.PublicKey
	for _, endpoint := range r.Arbiters {
		raw, err := hex.DecodeString(endpoint.PublicKeyHex)
		if err != nil {
			return err
		}
		key, err := protocol.PublicKeyFromBytes(raw)
		if err != nil {
			return err
		}
		arbiters = append(arbiters, key)
	}
	service := &seller.Seller{
		Sessions: sessions, Catalog: catalogAdapter{index}, Pricing: prices, RootID: "standalone",
		Signer: r.signer, Facts: facts, Arbiters: arbiters,
		Arbitration: func(ctx context.Context, key []byte) (seller.ArbitrationFee, error) {
			_, info, err := r.arbiters.Select(ctx, [][]byte{key})
			return seller.ArbitrationFee{BaseSat: info.BaseSat, PerItemSat: info.PerItemSat}, err
		},
		SubmitClose: r.submitClose,
	}
	trade, err := seller.NewRuntime(ctx, seller.RuntimeOptions{
		ResolveSeller: func(ctx context.Context, seed string) (*seller.Seller, error) {
			entry, err := service.Catalog.Preferred(ctx, seed)
			if err != nil || entry == nil {
				return nil, err
			}
			return service, nil
		},
		OpenSession: r.openSellerSession,
		OnError: func(demand discovery.Demand, err error) {
			log.Printf("bitfs: serve demand %s: %v", demand.MessageID, err)
		},
	})
	if err != nil {
		return err
	}
	defer func() {
		grace, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		trade.Stop(grace)
	}()
	listener := discovery.Listener{
		Subscribe: func(ctx context.Context, channel string) error {
			result, err := r.gateway.Subscribe(ctx, channel)
			return gatewayResult(result, err)
		},
		Deliveries: r.gateway.AddPublishHandler,
		OnDemand:   func(demand discovery.Demand) { trade.Accept(demand) },
	}
	if err := listener.Start(ctx); err != nil {
		return err
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"state": "serving", "peer": r.host.ID().String()}); err != nil {
		return err
	}
	<-stop
	return nil
}

func (r *runtime) openSellerSession(ctx context.Context, demand discovery.Demand) (seller.AuthenticatedSession, error) {
	var failure error
	for _, locator := range demand.Locators {
		if locator.Kind != hashrequest.LocatorMultiaddr {
			continue
		}
		session, err := transport.Dial(ctx, r.host, locator.Address)
		if err == nil {
			return session, nil
		}
		failure = errors.Join(failure, err)
	}
	return nil, errors.Join(errors.New("bitfs: no reachable buyer locator"), failure)
}

func (r *runtime) submitClose(ctx context.Context, input seller.CloseSubmission) error {
	rawHex := hex.EncodeToString(input.Raw)
	sources := [][]byte{input.FundingRaw}
	if len(input.LatestPaymentRaw) != 0 {
		sources = append(sources, input.LatestPaymentRaw)
	}
	proofs, err := wallet.BindInputs(rawHex, sources)
	if err != nil {
		return err
	}
	closed, err := tx.NewTransactionFromBytes(input.Raw)
	if err != nil {
		return err
	}
	walletScript := r.wallet.LockingScript().String()
	var declared []wallet.Output
	for _, out := range closed.Outputs {
		if out.LockingScript == nil {
			return errors.New("bitfs: close transaction contains an empty script")
		}
		if !strings.EqualFold(out.LockingScript.String(), walletScript) {
			declared = append(declared, wallet.Output{Satoshis: out.Satoshis, LockingScript: out.LockingScript})
		}
	}
	prepared, err := r.wallet.PrepareProtocol(ctx, "pool-close", wallet.ProtocolRequest{
		Business: "0", FeeRate: strconv.FormatUint(input.MinerFeeRate, 10), Declared: declared,
		SignedHex: rawHex, Proofs: proofs, Binding: wallet.ProtocolBinding{Kind: "close", PoolID: input.SessionID},
	})
	if err != nil {
		return err
	}
	_, err = r.wallet.BroadcastPrepared(ctx, "pool-close", prepared)
	var receipt *woc.BroadcastError
	if errors.As(err, &receipt) && receipt.Receipt.Status == "unknown" {
		return &seller.CloseBroadcastPendingError{TxID: input.TxID, Cause: err}
	}
	return err
}
