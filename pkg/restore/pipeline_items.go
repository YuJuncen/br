// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/rtree"
	"github.com/pingcap/br/pkg/utils"
)

const (
	defaultBatcherOutputChannelSize = 1024
)

// CreatedTable is a table created on restore process,
// but not yet filled with data.
type CreatedTable struct {
	RewriteRule *RewriteRules
	Table       *model.TableInfo
	OldTable    *utils.Table
}

// TableWithRange is a CreatedTable that has been bind to some of key ranges.
type TableWithRange struct {
	CreatedTable

	Range []rtree.Range
}

// Batcher collects ranges to restore and send batching split/ingest request.
type Batcher struct {
	cachedTables   []TableWithRange
	cachedTablesMu *sync.Mutex
	rewriteRules   *RewriteRules

	// joiner is for joining the background batch sender.
	joiner             chan<- struct{}
	sendErr            chan<- error
	outCh              chan<- CreatedTable
	sender             BatchSender
	batchSizeThreshold int
	size               int32
}

// SetThreshold sets the threshold that how big the batch size reaching need to send batch.
// note this function isn't goroutine safe yet,
// just set threshold before anything starts(e.g. EnableAutoCommit), please.
func (b *Batcher) SetThreshold(newThreshold int) {
	b.batchSizeThreshold = newThreshold
}

// Exhaust drains all remaining errors in the channel, into a slice of errors.
func Exhaust(ec <-chan error) []error {
	out := make([]error, 0, len(ec))
	for {
		select {
		case err := <-ec:
			out = append(out, err)
		default:
			// errCh will *never* closed(ya see, it has multi sender-part),
			// so we just consume the current backlog of this cannel, then return.
			return out
		}
	}
}

// Len calculate the current size of this batcher.
func (b *Batcher) Len() int {
	return int(atomic.LoadInt32(&b.size))
}

// BatchSender is the abstract of how the batcher send a batch.
type BatchSender interface {
	// RestoreBatch will send the restore request.
	RestoreBatch(ctx context.Context, ranges []rtree.Range, rewriteRules *RewriteRules) error
	Close()
}

type tikvSender struct {
	client         *Client
	updateCh       glue.Progress
	rejectStoreMap map[uint64]bool
}

// NewTiKVSender make a sender that send restore requests to TiKV.
func NewTiKVSender(ctx context.Context, cli *Client, updateCh glue.Progress) (BatchSender, error) {
	tiflashStores, err := conn.GetAllTiKVStores(ctx, cli.GetPDClient(), conn.TiFlashOnly)
	if err != nil {
		// After TiFlash support restore, we can remove this panic.
		// The origin of this panic is at RunRestore, and its semantic is nearing panic, don't worry about it.
		log.Error("failed to get and remove TiFlash replicas", zap.Error(errors.Trace(err)))
		return nil, err
	}
	rejectStoreMap := make(map[uint64]bool)
	for _, store := range tiflashStores {
		rejectStoreMap[store.GetId()] = true
	}

	return &tikvSender{
		client:         cli,
		updateCh:       updateCh,
		rejectStoreMap: rejectStoreMap,
	}, nil
}

func (b *tikvSender) RestoreBatch(ctx context.Context, ranges []rtree.Range, rewriteRules *RewriteRules) error {
	if err := SplitRanges(ctx, b.client, ranges, rewriteRules, b.updateCh); err != nil {
		log.Error("failed on split range",
			zap.Any("ranges", ranges),
			zap.Error(err),
		)
		return err
	}

	files := []*backup.File{}
	for _, fs := range ranges {
		files = append(files, fs.Files...)
	}

	if err := b.client.RestoreFiles(files, rewriteRules, b.rejectStoreMap, b.updateCh); err != nil {
		return err
	}
	log.Debug("send batch done",
		zap.Int("range count", len(ranges)),
		zap.Int("file count", len(files)),
	)

	return nil
}

func (b *tikvSender) Close() {
	// don't close update channel here, since we may need it then.
}

// NewBatcher creates a new batcher by client and updateCh.
// this batcher will work background, send batches per second, or batch size reaches limit.
// and it will emit full-restored tables to the output channel returned.
func NewBatcher(
	sender BatchSender,
	errCh chan<- error,
) (*Batcher, <-chan CreatedTable) {
	output := make(chan CreatedTable, defaultBatcherOutputChannelSize)
	b := &Batcher{
		rewriteRules:       EmptyRewriteRule(),
		sendErr:            errCh,
		outCh:              output,
		sender:             sender,
		cachedTablesMu:     new(sync.Mutex),
		batchSizeThreshold: 1,
	}
	return b, output
}

// EnableAutoCommit enables the batcher commit batch periodicity even batcher size isn't big enough.
// we make this function for disable AutoCommit in some case.
func (b *Batcher) EnableAutoCommit(ctx context.Context, delay time.Duration) {
	if b.joiner != nil {
		log.Warn("enable auto commit on a batcher that is enabled auto commit, nothing will happen")
		log.Info("if desire, please disable auto commit firstly")
	}
	joiner := make(chan struct{})
	go b.workLoop(ctx, joiner, delay)
	b.joiner = joiner
}

// DisableAutoCommit blocks the current goroutine until the worker can gracefully stop,
// and then disable auto commit.
func (b *Batcher) DisableAutoCommit(ctx context.Context) {
	b.joinWorker()
	b.joiner = nil
}

// joinWorker blocks the current goroutine until the worker can gracefully stop.
// return immediately when auto commit disabled.
func (b *Batcher) joinWorker() {
	if b.joiner != nil {
		log.Info("gracefully stoping worker goroutine")
		b.joiner <- struct{}{}
		log.Info("gracefully stopped worker goroutine")
	}
}

func (b *Batcher) workLoop(ctx context.Context, joiner <-chan struct{}, delay time.Duration) {
	tick := time.NewTicker(delay)
	defer tick.Stop()
	for {
		select {
		case <-joiner:
			log.Debug("graceful stop signal received")
			return
		case <-ctx.Done():
			b.sendErr <- ctx.Err()
			return
		case <-tick.C:
			if b.Len() > 0 {
				log.Info("sending batch because time limit exceed", zap.Int("size", b.Len()))
				b.asyncSend(ctx)
			}
		}
	}
}

func (b *Batcher) asyncSend(ctx context.Context) {
	tbls, err := b.Send(ctx)
	if err != nil {
		b.sendErr <- err
		return
	}
	for _, t := range tbls {
		b.outCh <- t
	}
}

func (b *Batcher) drainRanges() (
	ranges []rtree.Range,
	emptyTables []CreatedTable,
	rewriteRules *RewriteRules,
) {
	b.cachedTablesMu.Lock()
	rewriteRules = EmptyRewriteRule()
	defer b.cachedTablesMu.Unlock()

	for offset, thisTable := range b.cachedTables {
		thisTableLen := len(thisTable.Range)
		collected := len(ranges)
		rewriteRules.Append(*thisTable.RewriteRule)

		// the batch is full, we should stop here!
		// we use strictly greater than because when we send a batch at equal, the offset should plus one.
		// (because the last table is sent, we should put it in emptyTables), and this will intrduce extra complex.
		if thisTableLen+collected > b.batchSizeThreshold {
			drainSize := b.batchSizeThreshold - collected
			thisTableRanges := thisTable.Range

			var drained []rtree.Range
			drained, b.cachedTables[offset].Range = thisTableRanges[:drainSize], thisTableRanges[drainSize:]
			log.Debug("draining partial table to batch",
				zap.Stringer("table", thisTable.Table.Name),
				zap.Stringer("database", thisTable.OldTable.Db.Name),
				zap.Int("size", thisTableLen),
				zap.Int("drained", drainSize),
			)
			ranges = append(ranges, drained...)
			b.cachedTables = b.cachedTables[offset:]
			atomic.AddInt32(&b.size, -int32(len(drained)))
			return ranges, emptyTables, rewriteRules
		}

		emptyTables = append(emptyTables, thisTable.CreatedTable)
		// let's 'drain' the ranges of current table. This op must not make the batch full.
		ranges = append(ranges, thisTable.Range...)
		// let's reduce the batcher size each time, to make a consitance view of
		atomic.AddInt32(&b.size, -int32(len(thisTable.Range)))
		// clear the table length.
		b.cachedTables[offset].Range = []rtree.Range{}
		log.Debug("draining table to batch",
			zap.Stringer("table", thisTable.Table.Name),
			zap.Stringer("database", thisTable.OldTable.Db.Name),
			zap.Int("size", thisTableLen),
		)
	}

	// all tables are drained.
	b.cachedTables = []TableWithRange{}
	return ranges, emptyTables, rewriteRules
}

// Send sends all pending requests in the batcher.
// returns tables sent in the current batch.
func (b *Batcher) Send(ctx context.Context) ([]CreatedTable, error) {
	ranges, tbs, rewriteRules := b.drainRanges()
	tableNames := make([]string, 0, len(tbs))
	for _, t := range tbs {
		tableNames = append(tableNames, fmt.Sprintf("%s.%s", t.OldTable.Db.Name, t.OldTable.Info.Name))
	}
	log.Debug("do batch send",
		zap.Strings("tables", tableNames),
		zap.Int("ranges", len(ranges)),
	)
	if err := b.sender.RestoreBatch(ctx, ranges, rewriteRules); err != nil {
		return nil, err
	}
	return tbs, nil
}

func (b *Batcher) sendIfFull(ctx context.Context) {
	// never collect the send batch request message.
	for b.Len() >= b.batchSizeThreshold {
		log.Info("sending batch because batcher is full", zap.Int("size", b.Len()))
		b.asyncSend(ctx)
	}
}

// Add adds a task to the Batcher.
func (b *Batcher) Add(ctx context.Context, tbs TableWithRange) {
	b.cachedTablesMu.Lock()
	log.Debug("adding table to batch",
		zap.Stringer("table", tbs.Table.Name),
		zap.Stringer("database", tbs.OldTable.Db.Name),
		zap.Int64("old id", tbs.OldTable.Info.ID),
		zap.Int64("new id", tbs.Table.ID),
		zap.Int("table size", len(tbs.Range)),
		zap.Int("batch size", b.Len()),
	)
	b.cachedTables = append(b.cachedTables, tbs)
	b.rewriteRules.Append(*tbs.RewriteRule)
	atomic.AddInt32(&b.size, int32(len(tbs.Range)))
	b.cachedTablesMu.Unlock()

	b.sendIfFull(ctx)
}

// Close closes the batcher, sending all pending requests, close updateCh.
func (b *Batcher) Close(ctx context.Context) {
	log.Info("sending batch lastly on close.", zap.Int("size", b.Len()))
	for b.Len() > 0 {
		b.asyncSend(ctx)
	}
	b.DisableAutoCommit(ctx)
	close(b.outCh)
	b.sender.Close()
}
