package app

import (
	"github.com/xssnick/tonutils-go/tlb"

	"github.com/qynonyq/ton_dev_go_hw4/internal/structures"
)

func initTLB() {
	tlb.Register(structures.DedustAssetNative{})
	tlb.Register(structures.DedustAssetJetton{})
}
