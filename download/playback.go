package download

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"sync"

	"github.com/bsv8/BitFSProtocol/content"
	"github.com/bsv8/BitFSProtocol/wire"
	masterseed "github.com/bsv8/MasterSeed"
	"github.com/bsv8/keymaster.cc-bitfs/bitfs"
	"github.com/bsv8/keymaster.cc-bitfs/host"
	"github.com/bsv8/keymaster.cc-bitfs/store"
	"github.com/bsv8/keymaster.cc-download-scheduler/scheduler"
)

const playbackBlockBytes int64 = 256 * 1024
const maxPlaybackInteger int64 = 1<<53 - 1

// PlaybackRange describes the file and contains bytes when the requested
// range has been accepted. A nil FileSizeBytes means the manifest is unavailable.
type PlaybackRange struct {
	FileSizeBytes *int64
	MediaType     string
	Bytes         []byte
}

// PlaybackReader reads a range of up to one block from this download.
// Missing blocks in a live execution promote the requested range and the
// next three blocks for scheduling.
type PlaybackReader func(ctx context.Context, offset, length int64) (PlaybackRange, error)

func playbackQuotePath(seed, execID string) string {
	return "scheduler/tasks/" + seed + "." + execID + ".playback-quote"
}

// ReadPlaybackRange reads the durable contents of a named execution. Hosts
// use the reader supplied by Options.PlaybackReady to also set live priority.
func ReadPlaybackRange(ctx context.Context, repo store.Repository, seed, execID string, offset, length int64) (PlaybackRange, error) {
	if err := validateSeed(seed); err != nil {
		return PlaybackRange{}, err
	}
	if !host.ValidateExecID(execID) {
		return PlaybackRange{}, errors.New("download: invalid playback execution")
	}
	if err := validatePlaybackRange(offset, length); err != nil {
		return PlaybackRange{}, err
	}
	if err := ctx.Err(); err != nil {
		return PlaybackRange{}, err
	}
	state, _, err := host.LoadExecSnapshot(repo, seed, execID)
	if errors.Is(err, fs.ErrNotExist) {
		return PlaybackRange{}, nil
	}
	if err != nil {
		return PlaybackRange{}, err
	}
	terms, err := loadPlaybackTerms(repo, seed, execID, state)
	if errors.Is(err, fs.ErrNotExist) {
		return PlaybackRange{}, nil
	}
	if err != nil {
		return PlaybackRange{}, err
	}
	result, _, err := readPlayback(ctx, repo, seed, state, terms, offset, length)
	return result, err
}

func loadPlaybackTerms(repo store.Repository, seed, execID string, state scheduler.State) (*content.FileQuoteTerms, error) {
	raw, err := repo.Load(playbackQuotePath(seed, execID))
	if errors.Is(err, fs.ErrNotExist) {
		// Older executions retain the original signed quote in the Seed pool.
		for _, request := range state.Requests {
			if request.State != "settled" || !slices.Contains(request.ContentIDs, seed) {
				continue
			}
			meta, loadErr := bitfs.LoadPoolMeta(repo, host.ExecPoolID(execID, request.PoolID))
			if loadErr != nil {
				return nil, loadErr
			}
			raw, err = hex.DecodeString(meta.QuoteRawHex)
			break
		}
	}
	if err != nil {
		return nil, err
	}
	artifact, err := wire.ParseAs(wire.FileQuote, raw)
	if err != nil {
		return nil, err
	}
	document, err := wire.DecodeFileQuote(artifact)
	if err != nil {
		return nil, err
	}
	quote, err := content.VerifyQuoteEvidence(document)
	if err != nil {
		return nil, err
	}
	if hex.EncodeToString(quote.SeedHash()) != seed {
		return nil, errors.New("download: playback quote belongs to another Seed")
	}
	return quote.Terms(), nil
}

func livePlaybackReader(repo store.Repository, seed, execID string, state scheduler.State, loop *host.Loop,
	initial *content.FileQuoteTerms, rawQuote []byte, logf func(string, ...any)) PlaybackReader {
	terms, quoteErr := loadPlaybackTerms(repo, seed, execID, state)
	if errors.Is(quoteErr, fs.ErrNotExist) {
		terms, quoteErr = initial, nil
		if err := repo.SaveExact(playbackQuotePath(seed, execID), rawQuote); err != nil {
			logf("保存播放报价失败: %v", err)
		}
	}
	var mu sync.Mutex
	return func(ctx context.Context, offset, length int64) (PlaybackRange, error) {
		if err := validatePlaybackRange(offset, length); err != nil {
			return PlaybackRange{}, err
		}
		if err := ctx.Err(); err != nil {
			return PlaybackRange{}, err
		}
		if quoteErr != nil {
			return PlaybackRange{}, fmt.Errorf("download: load playback quote: %w", quoteErr)
		}
		mu.Lock()
		defer mu.Unlock()
		state, err := loop.State(ctx)
		if err != nil {
			return PlaybackRange{}, err
		}
		result, preferred, err := readPlayback(ctx, repo, seed, state, terms, offset, length)
		if err == nil && len(preferred) > 0 && !slices.Equal(preferred, state.PriorityBlocks) {
			if feedErr := loop.Feed(ctx, scheduler.Event{
				EventID: "playback-" + hex.EncodeToString(randomRunID()), Type: "prioritize-blocks", BlockHashes: preferred,
			}); feedErr != nil && ctx.Err() == nil {
				logf("更新播放优先级失败: %v", feedErr)
			}
		}
		return result, err
	}
}

func validatePlaybackRange(offset, length int64) error {
	if offset < 0 || length < 0 || length > playbackBlockBytes || offset > maxPlaybackInteger-length {
		return errors.New("download: invalid playback range")
	}
	return nil
}

func readPlayback(ctx context.Context, repo store.Repository, seed string, state scheduler.State, terms *content.FileQuoteTerms, offset, length int64) (PlaybackRange, []string, error) {
	if state.SeedHash != seed || state.Manifest == nil {
		return PlaybackRange{}, nil, nil
	}
	rawSeed, err := repo.Load("staging/seed")
	if errors.Is(err, fs.ErrNotExist) {
		return PlaybackRange{}, nil, nil
	}
	if err != nil {
		return PlaybackRange{}, nil, err
	}
	digest, err := masterseed.ParseDigestHex(seed)
	if err != nil {
		return PlaybackRange{}, nil, err
	}
	if _, err := masterseed.VerifySeed(ctx, bytes.NewReader(rawSeed), digest); err != nil {
		return PlaybackRange{}, nil, err
	}
	hashes := state.Manifest.BlockHashes
	if len(rawSeed) != len(hashes)*masterseed.DigestSize {
		return PlaybackRange{}, nil, errors.New("download: playback manifest does not match verified Seed")
	}
	for index, hash := range hashes {
		if hex.EncodeToString(rawSeed[index*masterseed.DigestSize:(index+1)*masterseed.DigestSize]) != hash {
			return PlaybackRange{}, nil, errors.New("download: playback manifest does not match verified Seed")
		}
	}
	if terms.FileSizeBytes > uint64(maxPlaybackInteger) ||
		(terms.FileSizeBytes+uint64(playbackBlockBytes)-1)/uint64(playbackBlockBytes) != uint64(len(hashes)) {
		return PlaybackRange{}, nil, errors.New("download: playback length does not match verified Seed")
	}
	size := int64(terms.FileSizeBytes)
	result := PlaybackRange{FileSizeBytes: &size, MediaType: terms.MediaType}
	end := min(offset+length, size)
	if offset >= end {
		return result, nil, nil
	}
	first, last := int(offset/playbackBlockBytes), int((end-1)/playbackBlockBytes)
	for _, hash := range hashes[first : last+1] {
		if state.Completed[hash] == "" {
			seen := make(map[string]bool)
			preferred := make([]string, 0, 5)
			for _, item := range hashes[first:min(len(hashes), last+4)] {
				if !seen[item] {
					seen[item] = true
					preferred = append(preferred, item)
				}
			}
			return result, preferred, nil
		}
	}
	data := make([]byte, 0, end-offset)
	for index := first; index <= last; index++ {
		hash := hashes[index]
		block, err := repo.Load("staging/" + hash)
		if errors.Is(err, fs.ErrNotExist) {
			return result, nil, nil
		}
		if err != nil {
			return PlaybackRange{}, nil, err
		}
		digest, err := masterseed.ParseDigestHex(hash)
		if err != nil {
			return PlaybackRange{}, nil, err
		}
		if _, err := masterseed.VerifyBlock(ctx, block, digest); err != nil {
			return PlaybackRange{}, nil, err
		}
		base := int64(index) * playbackBlockBytes
		if int64(len(block)) != min(playbackBlockBytes, size-base) {
			return PlaybackRange{}, nil, errors.New("download: invalid playback block length")
		}
		data = append(data, block[max(0, offset-base):min(int64(len(block)), end-base)]...)
	}
	if err := ctx.Err(); err != nil {
		return PlaybackRange{}, nil, err
	}
	result.Bytes = data
	return result, nil, nil
}
