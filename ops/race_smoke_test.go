package ops

import (
	"context"
	"database/sql"
	"sync"
	"testing"
)

// raceSmokeCache builds a Cache without a database: the race smoke test only
// exercises the lookup data and its random accessors.
func raceSmokeCache() *Cache {
	return &Cache{
		DeptNos:   []string{"000", "100", "200"},
		EmpNos:    []int{1, 2, 3, 4, 5},
		ProjIds:   []string{"VBASE", "VDEVP", "VMKTP"},
		CustNos:   []int{1001, 1002},
		Countries: []string{"USA", "Germany"},
		JobSalaries: map[string]JobSalaryRange{
			"Admin": {MinSalary: 10000, MaxSalary: 200000},
			"Eng":   {MinSalary: 20000, MaxSalary: 300000},
		},
	}
}

// TestCacheRandomConcurrent hammers every shared Cache random accessor from
// many goroutines. The cache is one instance per run shared by all workers,
// so any unsynchronized state fails here under `go test -race`.
func TestCacheRandomConcurrent(t *testing.T) {
	c := raceSmokeCache()

	actions := []func(){
		func() { _ = c.RandomDeptNo() },
		func() { _ = c.RandomEmpNo() },
		func() { _ = c.RandomProjId() },
		func() { _ = c.RandomCustNo() },
		func() { _ = c.RandomCountry() },
		func() { _, _ = c.RandomJobSalaryRange() },
		func() { _ = c.RandomSalaryInRange(1000, 2000) },
		func() { _ = c.RandomString(8) },
		func() { _ = c.RandomName() },
		func() { _ = c.RandomAddress() },
		func() { _ = c.RandomCitySimple() },
		func() { _ = c.RandomOrderStatus() },
		func() { _ = c.RandomPaid() },
		func() { _ = c.RandomOnHold() },
		func() { _ = c.RandomDiscount() },
		func() { _ = c.RandomPONumber() },
		func() { _ = c.RandomPercentChange() },
		func() { _ = c.SalaryWithinPercentCap(50000, 10000, 200000, 45) },
	}

	const goroutines = 8
	const iterations = 2000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				actions[(g+i)%len(actions)]()
			}
		}(g)
	}
	wg.Wait()
}

// TestWriteOperationsRandomConcurrent exercises the WriteOperations data
// generation helpers shared by all workers (no database involved).
func TestWriteOperationsRandomConcurrent(t *testing.T) {
	wo := NewWriteOperations(nil, raceSmokeCache())

	const goroutines = 8
	const iterations = 2000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_, _, _, _, _, _ = wo.generateCustomerAddress()
				_ = wo.getNextOrderStatus("new")
			}
		}()
	}
	wg.Wait()
}

// guard compile-time references to otherwise unused signatures
var _ = context.Background
var _ = (*sql.Tx)(nil)
