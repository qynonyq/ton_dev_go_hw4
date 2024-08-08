package storage

import (
	"time"

	"github.com/shopspring/decimal"
)

type DedustSwap struct {
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
