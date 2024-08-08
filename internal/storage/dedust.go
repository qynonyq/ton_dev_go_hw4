package storage

import (
	"time"

	"github.com/shopspring/decimal"
)

type (
	DedustSwap struct {
		ID            uint64 `gorm:"primaryKey;autoIncrement:true;"`
		PoolAddress   string
		AssetIn       string
		AmountIn      decimal.Decimal
		AssetOut      string
		AmountOut     decimal.Decimal
		SenderAddress string
		Reserve0      decimal.Decimal
		Reserve1      decimal.Decimal
		// when transaction added to blockchain
		CreatedAt time.Time
		// when it was processed
		ProcessedAt time.Time
	}

	DedustDeposit struct {
		ID            uint64 `gorm:"primaryKey;autoIncrement:true;"`
		SenderAddress string
		Amount0       decimal.Decimal
		Amount1       decimal.Decimal
		Reserve0      decimal.Decimal
		Reserve1      decimal.Decimal
		Liquidity     decimal.Decimal
		CreatedAt     time.Time
		ProcessedAt   time.Time
	}

	DedustWithdrawal struct {
		ID            uint64 `gorm:"primaryKey;autoIncrement:true;"`
		SenderAddress string
		Liquidity     decimal.Decimal
		Amount0       decimal.Decimal
		Amount1       decimal.Decimal
		Reserve0      decimal.Decimal
		Reserve1      decimal.Decimal
		CreatedAt     time.Time
		ProcessedAt   time.Time
	}
)
