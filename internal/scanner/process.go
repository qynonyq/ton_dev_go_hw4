package scanner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"github.com/qynonyq/ton_dev_go_hw4/internal/storage"
	"github.com/qynonyq/ton_dev_go_hw4/internal/structures"

	"github.com/sirupsen/logrus"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"golang.org/x/sync/errgroup"
	"gopkg.in/tomb.v2"

	"github.com/qynonyq/ton_dev_go_hw4/internal/app"
)

func (s *Scanner) processBlocks(ctx context.Context) {
	const (
		delayBase = 2 * time.Second
		delayMax  = 8 * time.Second
		maxRetry  = 5
	)
	delay := delayBase

	for {
		master, err := s.api.LookupBlock(
			ctx,
			s.lastBlock.Workchain,
			s.lastBlock.Shard,
			s.lastBlock.SeqNo,
		)
		if err == nil {
			delay = delayBase
		}
		if err != nil {
			if !errors.Is(err, ton.ErrBlockNotFound) {
				logrus.Errorf("[SCN] failed to lookup master block %d: %s", s.lastBlock.SeqNo, err)
			}

			time.Sleep(delay)
			delay *= 2
			if delay > delayMax {
				delay = delayMax
			}

			continue
		}

		err = s.processMcBlock(ctx, master)
		if err == nil {
			delay = delayBase
		}
		retries := 0
		if err != nil {
			if !strings.Contains(err.Error(), "is not in db") {
				logrus.Errorf("[SCN] failed to process MC block [seqno=%d] [shard=%d]: %s",
					master.SeqNo, master.Shard, err)
				retries++
				continue
			}

			time.Sleep(delay)
			delay *= 2
			if delay > delayMax {
				delay = delayMax
			}

		}
	}
}

func (s *Scanner) processMcBlock(ctx context.Context, master *ton.BlockIDExt) error {
	start := time.Now()

	currentShards, err := s.api.GetBlockShardsInfo(ctx, master)
	if err != nil {
		return err
	}

	shards := make(map[string]*ton.BlockIDExt, len(currentShards))

	for _, shard := range currentShards {
		// unique key
		key := fmt.Sprintf("%d:%d:%d", shard.Workchain, shard.Shard, shard.SeqNo)
		shards[key] = shard

		if err := s.fillWithNotSeenShards(ctx, shards, shard); err != nil {
			return err
		}
		s.lastShardsSeqNo[s.getShardID(shard)] = shard.SeqNo
	}

	txs := make([]*tlb.Transaction, 0, len(shards))
	for _, shard := range shards {
		shardTxs, err := s.getTxsFromShard(ctx, shard)
		if err != nil {
			return err
		}
		txs = append(txs, shardTxs...)
	}

	var (
		tmb tomb.Tomb
		wg  sync.WaitGroup
		mu  sync.Mutex
		//events = make([]storage.DedustSwap, 0, len(txs))
		events = make([]storage.DedustDeposit, 0, len(txs))
	)
	// process transactions
	tmb.Go(func() error {
		for _, tx := range txs {
			// break loop if there was transaction processing error
			select {
			case <-tmb.Dying():
				break
			default:
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				//ee, err := processTxDedustSwap(tx)
				ee, err := processTxDedustDeposit(tx)
				if err != nil {
					tmb.Kill(err)
					return
				}

				mu.Lock()
				defer mu.Unlock()
				events = append(events, ee...)
			}()
		}
		wg.Wait()

		return nil
	})
	if err := tmb.Wait(); err != nil {
		logrus.Errorf("[SCN] failed to process transactions: %s", err)
		// start with next block, otherwise process will get stuck
		s.lastBlock.SeqNo++
		return err
	}

	txDB := app.DB.Begin()

	for _, e := range events {
		if err := txDB.Create(&e).Error; err != nil {
			txDB.Rollback()
			return err
		}
	}

	if err := s.addBlock(master, txDB); err != nil {
		txDB.Rollback()
		return err
	}

	if err := txDB.Commit().Error; err != nil {
		logrus.Errorf("[SCN] failed to commit txDB: %s", err)
		return err
	}

	lastSeqno, err := s.getLastBlockSeqno(ctx)
	if err != nil {
		logrus.Infof("[SCN] block [%d] processed in [%.2fs] with [%d] transactions",
			master.SeqNo,
			time.Since(start).Seconds(),
			len(txs),
		)
	} else {
		logrus.Infof("[SCN] block [%d|%d] processed in [%.2fs] with [%d] transactions",
			master.SeqNo,
			lastSeqno,
			time.Since(start).Seconds(),
			len(txs),
		)
	}

	return nil
}
func (s *Scanner) getTxsFromShard(ctx context.Context, shard *ton.BlockIDExt) ([]*tlb.Transaction, error) {
	var (
		after    *ton.TransactionID3
		more     = true
		err      error
		eg       errgroup.Group
		txsShort []ton.TransactionShortInfo
		mu       sync.Mutex
		txs      []*tlb.Transaction
	)

	for more {
		txsShort, more, err = s.api.GetBlockTransactionsV2(
			ctx,
			shard,
			100,
			after,
		)
		if err != nil {
			return nil, err
		}

		if more {
			after = txsShort[len(txsShort)-1].ID3()
		}

		for _, txShort := range txsShort {
			eg.Go(func() error {
				tx, err := s.api.GetTransaction(
					ctx,
					shard,
					address.NewAddress(0, 0, txShort.Account),
					txShort.LT,
				)
				if err != nil {
					if strings.Contains(err.Error(), "is not in db") ||
						strings.Contains(err.Error(), "is not applied") {
						return nil
					}

					logrus.Errorf("[SCN] failed to load tx: %s", err)

					return err
				}

				mu.Lock()
				defer mu.Unlock()
				txs = append(txs, tx)

				return nil
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("[SCN] failed to get transactions: %w", err)
	}

	return txs, nil
}

func processTxDedustSwap(tx *tlb.Transaction) ([]storage.DedustSwap, error) {
	if tx.IO.Out == nil {
		return nil, nil
	}

	mmOut, err := tx.IO.Out.ToSlice()
	if err != nil {
		return nil, nil
	}

	events := make([]storage.DedustSwap, 0, len(mmOut))

	for _, m := range mmOut {
		if m.MsgType != tlb.MsgTypeExternalOut {
			continue
		}

		extOut := m.AsExternalOut()
		if extOut.Body == nil {
			continue
		}

		var dse structures.DedustSwapEvent
		if err := tlb.LoadFromCell(&dse, extOut.Body.BeginParse()); err != nil {
			continue
		}

		var (
			amountIn  string
			amountOut string
		)

		// assetIn
		if dse.AssetIn.Type() == "native" {
			amountIn = dse.AmountIn.String() + " TON"
		} else {
			jettonAddr := dse.AssetIn.AsJetton()
			amountIn = fmt.Sprintf("%s JETTON root [%s]",
				dse.AmountIn,
				address.NewAddress(0, byte(jettonAddr.WorkchainID), jettonAddr.AddressData))
		}

		// assetOut
		if dse.AssetOut.Type() == "native" {
			amountOut = dse.AmountOut.String() + " TON"
		} else {
			jettonAddr := dse.AssetOut.AsJetton()
			amountOut = fmt.Sprintf("%s JETTON root [%s]",
				dse.AmountOut,
				address.NewAddress(0, byte(jettonAddr.WorkchainID), jettonAddr.AddressData))
		}

		logrus.Infof("[DDST] new swap")
		logrus.Infof("[DDST] from: %s", dse.ExtraInfo.SenderAddr.String())
		logrus.Infof("[DDST] amount input: %s", amountIn)
		logrus.Infof("[DDST] amount output: %s\n\n", amountOut)

		swap := storage.DedustSwap{
			PoolAddress:   extOut.SrcAddr.String(),
			AssetIn:       dse.AssetIn.Type(),
			AmountIn:      decimal.NewFromBigInt(dse.AmountIn.Nano(), 0),
			AssetOut:      dse.AssetOut.Type(),
			AmountOut:     decimal.NewFromBigInt(dse.AmountOut.Nano(), 0),
			SenderAddress: dse.ExtraInfo.SenderAddr.String(),
			Reserve0:      decimal.NewFromBigInt(dse.ExtraInfo.Reserve0.Nano(), 0),
			Reserve1:      decimal.NewFromBigInt(dse.ExtraInfo.Reserve1.Nano(), 0),
			CreatedAt:     time.Unix(int64(extOut.CreatedAt), 0),
			ProcessedAt:   time.Now(),
		}

		events = append(events, swap)
	}

	return events, nil
}

func processTxDedustDeposit(tx *tlb.Transaction) ([]storage.DedustDeposit, error) {
	if tx.IO.Out == nil {
		return nil, nil
	}

	mmOut, err := tx.IO.Out.ToSlice()
	if err != nil {
		return nil, nil
	}

	events := make([]storage.DedustDeposit, 0, len(mmOut))

	for _, m := range mmOut {
		if m.MsgType != tlb.MsgTypeExternalOut {
			continue
		}

		extOut := m.AsExternalOut()
		if extOut.Body == nil {
			continue
		}

		var dde structures.DedustDepositEvent
		if err := tlb.LoadFromCell(&dde, extOut.Body.BeginParse()); err != nil {
			continue
		}

		logrus.Infof("[DDST] new deposit")
		logrus.Infof("[DDST] from: %s", dde.SenderAddr)
		logrus.Infof("[DDST] amount0: %s, amount1: %s", dde.Amount0, dde.Amount1)
		logrus.Infof("[DDST] reserve0: %s, reserve1: %s", dde.Reserve0, dde.Reserve1)
		logrus.Infof("[DDST] liquidity: %s\n\n", dde.Liquidity)

		deposit := storage.DedustDeposit{
			SenderAddress: dde.SenderAddr.String(),
			Amount0:       decimal.NewFromBigInt(dde.Amount0.Nano(), 0),
			Amount1:       decimal.NewFromBigInt(dde.Amount1.Nano(), 0),
			Reserve0:      decimal.NewFromBigInt(dde.Reserve0.Nano(), 0),
			Reserve1:      decimal.NewFromBigInt(dde.Reserve1.Nano(), 0),
			Liquidity:     decimal.NewFromBigInt(dde.Liquidity.Nano(), 0),
			CreatedAt:     time.Unix(int64(extOut.CreatedAt), 0),
			ProcessedAt:   time.Now(),
		}

		events = append(events, deposit)
	}

	return events, nil
}
