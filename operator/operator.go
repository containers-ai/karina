package operator

import (
	"sync"
	"sync/atomic"
)

var (
	mu                sync.Mutex
	initialized       uint32
	singletonOperator *Operator
)

type Operator struct {
	Config Config
}

func NewOperatorWithConfig(config Config) {

	singletonOperator = &Operator{
		Config: config,
	}
	atomic.StoreUint32(&initialized, 1)
}

func GetOperator() *Operator {

	if atomic.LoadUint32(&initialized) == 1 {
		return singletonOperator
	}

	mu.Lock()
	defer mu.Unlock()

	if initialized == 0 {
		singletonOperator = &Operator{Config: NewDefaultConfig()}
		atomic.StoreUint32(&initialized, 1)
	}

	return singletonOperator
}
