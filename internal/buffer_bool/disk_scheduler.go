package buffer_bool

import (
	"time"
)

// diskScheduler - простой планировщик для записи страниц
type diskScheduler struct {
	isRunning bool // Флаг работы background worker
}

// NewDiskScheduler создает новый Disk Scheduler
func NewDiskScheduler() *diskScheduler {
	return &diskScheduler{
		isRunning: false,
	}
}

// StartBgWorker запускает background worker для записи dirty страниц
func (ds *diskScheduler) StartBgWorker(flushFunc func()) error {
	if ds.isRunning {
		return nil // Уже запущен
	}

	ds.isRunning = true

	// Запускаем background worker
	go ds.flushWorker(flushFunc)

	return nil
}

// flushWorker background worker для записи dirty страниц
func (ds *diskScheduler) flushWorker(flushFunc func()) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		if ds.isRunning {
			flushFunc()
		} else {
			// Финальная запись всех dirty страниц
			flushFunc()
			return
		}
	}
}
