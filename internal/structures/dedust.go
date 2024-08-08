package structures

import (
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
)

type DedustAsset interface {
	Type() string
	AsNative() DedustAssetNative
	AsJetton() DedustAssetJetton
}

// DedustAssetNative struct and methods
type DedustAssetNative struct {
	_ tlb.Magic `tlb:"$0000"`
}

func (a DedustAssetNative) Type() string { return "native" }

func (a DedustAssetNative) AsNative() DedustAssetNative { return a }

func (a DedustAssetNative) AsJetton() DedustAssetJetton { return DedustAssetJetton{} }

// DedustAssetJetton struct and methods
type DedustAssetJetton struct {
	_           tlb.Magic `tlb:"$0001"`
	WorkchainID uint64    `tlb:"## 8"`
	AddressData []byte    `tlb:"bits 256"`
}

func (a DedustAssetJetton) Type() string { return "jetton" }

func (a DedustAssetJetton) AsNative() DedustAssetNative { return DedustAssetNative{} }

func (a DedustAssetJetton) AsJetton() DedustAssetJetton { return a }

type (
	DedustEventSwap struct {
		_         tlb.Magic   `tlb:"#9c610de3"`
		AssetIn   DedustAsset `tlb:"[DedustAssetNative,DedustAssetJetton]"`
		AssetOut  DedustAsset `tlb:"[DedustAssetNative,DedustAssetJetton]"`
		AmountIn  tlb.Coins   `tlb:"."`
		AmountOut tlb.Coins   `tlb:"."`
		ExtraInfo struct {
			SenderAddr   *address.Address `tlb:"addr"`
			ReferralAddr *address.Address `tlb:"addr"`
			Reserve0     tlb.Coins        `tlb:"."`
			Reserve1     tlb.Coins        `tlb:"."`
		} `tlb:"^"`
	}

	DedustEventDeposit struct {
		_          tlb.Magic        `tlb:"#b544f4a4"`
		SenderAddr *address.Address `tlb:"addr"`
		Amount0    tlb.Coins        `tlb:"."`
		Amount1    tlb.Coins        `tlb:"."`
		Reserve0   tlb.Coins        `tlb:"."`
		Reserve1   tlb.Coins        `tlb:"."`
		Liquidity  tlb.Coins        `tlb:"."`
	}

	DedustEventWithdrawal struct {
		_          tlb.Magic        `tlb:"#3aa870a6"`
		SenderAddr *address.Address `tlb:"addr"`
		Liquidity  tlb.Coins        `tlb:"."`
		Amount0    tlb.Coins        `tlb:"."`
		Amount1    tlb.Coins        `tlb:"."`
		Reserve0   tlb.Coins        `tlb:"."`
		Reserve1   tlb.Coins        `tlb:"."`
	}
)
