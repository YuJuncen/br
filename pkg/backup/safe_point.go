// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/v4/client"
	"go.uber.org/zap"
)

const (
	brServiceSafePointID            = "br"
	preUpdateServiceSafePointFactor = 5
	// DefaultBRGCSafePointTTL means PD keep safePoint limit at least 5min
	DefaultBRGCSafePointTTL = 5 * 60
)

// getGCSafePoint returns the current gc safe point.
// TODO: Some cluster may not enable distributed GC.
func getGCSafePoint(ctx context.Context, pdClient pd.Client) (uint64, error) {
	safePoint, err := pdClient.UpdateGCSafePoint(ctx, 0)
	if err != nil {
		return 0, err
	}
	return safePoint, nil
}

// CheckGCSafePoint checks whether the ts is older than GC safepoint.
// Note: It ignores errors other than exceed GC safepoint.
func CheckGCSafePoint(ctx context.Context, pdClient pd.Client, ts uint64) error {
	// TODO: use PDClient.GetGCSafePoint instead once PD client exports it.
	safePoint, err := getGCSafePoint(ctx, pdClient)
	if err != nil {
		log.Warn("fail to get GC safe point", zap.Error(err))
		return nil
	}
	if ts <= safePoint {
		return errors.Errorf("GC safepoint %d exceed TS %d", safePoint, ts)
	}
	return nil
}

// UpdateServiceSafePoint register backupTS to PD, to lock down backupTS as safePoint with ttl seconds.
func UpdateServiceSafePoint(ctx context.Context, pdClient pd.Client, ttl int64, backupTS uint64) error {
	log.Debug("update PD safePoint limit with ttl",
		zap.Uint64("safePoint", backupTS),
		zap.Int64("ttl", ttl))

	_, err := pdClient.UpdateServiceGCSafePoint(ctx,
		brServiceSafePointID, ttl, backupTS-1)
	return err
}

// StartServiceSafePointKeeper will run UpdateServiceSafePoint periodicity
// hence keeping service safepoint won't lose.
func StartServiceSafePointKeeper(
	ctx context.Context,
	ttl int64,
	pdClient pd.Client,
	backupTS uint64,
) {
	gapTime := time.Duration(ttl) * time.Second / preUpdateServiceSafePointFactor
	// It would be OK since ttl won't be zero, so gapTime should > 0.
	tick := time.NewTicker(gapTime)
	go func() {
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if err := UpdateServiceSafePoint(ctx, pdClient, ttl, backupTS); err != nil {
					log.Error("failed to update service safe point, backup may fail if gc triggered",
						zap.Error(err),
					)
				}
			}
		}
	}()
}
