package executors

import (
	"custom-database/internal/buffer_bool"
	"custom-database/internal/disk_manager"
	"custom-database/internal/operator_execution"
	"custom-database/internal/query_planning"
	"fmt"
)

type SeqScanExecutor struct {
	bufferPool buffer_bool.BufferPoolInterface
	plan       *query_planning.SeqScanPlan

	metaInfo    *buffer_bool.MetaInfo
	schema      *operator_execution.Schema
	pageIndex   int
	currentPage *disk_manager.Page
	currentRow  int
}

func NewSeqScanExecutor(bufferPool buffer_bool.BufferPoolInterface, plan *query_planning.SeqScanPlan) operator_execution.Executor {
	return &SeqScanExecutor{
		bufferPool: bufferPool,
		plan:       plan,
		pageIndex:  0,
		currentRow: 0,
	}
}

func (e *SeqScanExecutor) Init() error {
	metaInfo, err := e.bufferPool.ReadMetaInfo(e.plan.TableName)
	if err != nil {
		return fmt.Errorf("failed to read meta info for table %s: %w", e.plan.TableName, err)
	}

	e.metaInfo = metaInfo
	e.schema = operator_execution.NewSchema(metaInfo.MetaData.Columns)
	e.pageIndex = 0
	e.currentRow = 0
	e.currentPage = nil

	return nil
}

func (e *SeqScanExecutor) Next() (*operator_execution.Tuple, error) {
	for e.pageIndex < len(e.metaInfo.PageDirectory.Entries) {
		entry := e.metaInfo.PageDirectory.Entries[e.pageIndex]

		if e.currentPage == nil {
			pageID := disk_manager.PageID{
				PageNumber: entry.PageID,
				TableName:  e.plan.TableName,
			}

			page, err := e.bufferPool.GetPage(e.plan.TableName, pageID)
			if err != nil {
				return nil, fmt.Errorf("failed to get page %d: %w", entry.PageID, err)
			}
			e.currentPage = page
			e.currentRow = 0
		}

		for e.currentRow < len(e.currentPage.Rows) {
			row := e.currentPage.Rows[e.currentRow]
			e.currentRow++

			if len(row) == 0 {
				continue
			}

			tuple, err := operator_execution.ConvertFromDiskRow(row, e.schema)
			if err != nil {
				e.unpinCurrentPage()
				return nil, err
			}

			return tuple, nil
		}

		e.unpinCurrentPage()
		e.currentPage = nil
		e.pageIndex++
	}

	return nil, nil
}

func (e *SeqScanExecutor) unpinCurrentPage() {
	if e.currentPage != nil {
		pageID := disk_manager.PageID{
			PageNumber: e.metaInfo.PageDirectory.Entries[e.pageIndex].PageID,
			TableName:  e.plan.TableName,
		}
		e.bufferPool.Unpin(e.plan.TableName, pageID)
	}
}

func (e *SeqScanExecutor) Close() error {
	if e.currentPage != nil {
		e.unpinCurrentPage()
		e.currentPage = nil
	}

	return nil
}

func (e *SeqScanExecutor) GetSchema() *operator_execution.Schema {
	return e.schema
}
