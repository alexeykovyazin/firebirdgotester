package ops

import (
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"fb-loadgen/db"
)

// Cache holds pre-fetched lookup data for operations
type Cache struct {
	// Valid values for operations
	DeptNos     []string
	EmpNos      []int
	ProjIds     []string
	CustNos     []int
	JobSalaries map[string]JobSalaryRange // job_code -> min/max salary

	// Random number generator (thread-safe)
	rng *rand.Rand
}

// JobSalaryRange holds min/max salary for a job
type JobSalaryRange struct {
	MinSalary float64
	MaxSalary float64
}

// NewCache creates and populates a new cache
func NewCache(connFactory *db.ConnectionFactory) (*Cache, error) {
	dbConn, err := connFactory.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open connection for cache: %w", err)
	}
	defer connFactory.Close(dbConn)

	cache := &Cache{
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// Load all lookup data
	if err := cache.loadDeptNos(dbConn); err != nil {
		return nil, fmt.Errorf("failed to load dept nos: %w", err)
	}

	if err := cache.loadEmpNos(dbConn); err != nil {
		return nil, fmt.Errorf("failed to load emp nos: %w", err)
	}

	if err := cache.loadProjIds(dbConn); err != nil {
		return nil, fmt.Errorf("failed to load proj ids: %w", err)
	}

	if err := cache.loadCustNos(dbConn); err != nil {
		return nil, fmt.Errorf("failed to load cust nos: %w", err)
	}

	if err := cache.loadJobSalaries(dbConn); err != nil {
		return nil, fmt.Errorf("failed to load job salaries: %w", err)
	}

	return cache, nil
}

// loadDeptNos loads valid department numbers
func (c *Cache) loadDeptNos(db *sql.DB) error {
	rows, err := db.Query("SELECT DEPT_NO FROM DEPARTMENT")
	if err != nil {
		return fmt.Errorf("query dept nos: %w", err)
	}
	defer rows.Close()

	var deptNos []string
	for rows.Next() {
		var deptNo string
		if err := rows.Scan(&deptNo); err != nil {
			return fmt.Errorf("scan dept no: %w", err)
		}
		deptNos = append(deptNos, deptNo)
	}

	if len(deptNos) == 0 {
		return fmt.Errorf("no departments found")
	}

	c.DeptNos = deptNos
	return nil
}

// loadEmpNos loads valid employee numbers
func (c *Cache) loadEmpNos(db *sql.DB) error {
	rows, err := db.Query("SELECT EMP_NO FROM EMPLOYEE")
	if err != nil {
		return fmt.Errorf("query emp nos: %w", err)
	}
	defer rows.Close()

	var empNos []int
	for rows.Next() {
		var empNo int
		if err := rows.Scan(&empNo); err != nil {
			return fmt.Errorf("scan emp no: %w", err)
		}
		empNos = append(empNos, empNo)
	}

	if len(empNos) == 0 {
		return fmt.Errorf("no employees found")
	}

	c.EmpNos = empNos
	return nil
}

// loadProjIds loads valid project IDs
func (c *Cache) loadProjIds(db *sql.DB) error {
	rows, err := db.Query("SELECT PROJ_ID FROM PROJECT")
	if err != nil {
		return fmt.Errorf("query proj ids: %w", err)
	}
	defer rows.Close()

	var projIds []string
	for rows.Next() {
		var projId string
		if err := rows.Scan(&projId); err != nil {
			return fmt.Errorf("scan proj id: %w", err)
		}
		projIds = append(projIds, projId)
	}

	if len(projIds) == 0 {
		return fmt.Errorf("no projects found")
	}

	c.ProjIds = projIds
	return nil
}

// loadCustNos loads valid customer numbers
func (c *Cache) loadCustNos(db *sql.DB) error {
	rows, err := db.Query("SELECT CUST_NO FROM CUSTOMER")
	if err != nil {
		return fmt.Errorf("query cust nos: %w", err)
	}
	defer rows.Close()

	var custNos []int
	for rows.Next() {
		var custNo int
		if err := rows.Scan(&custNo); err != nil {
			return fmt.Errorf("scan cust no: %w", err)
		}
		custNos = append(custNos, custNo)
	}

	if len(custNos) == 0 {
		return fmt.Errorf("no customers found")
	}

	c.CustNos = custNos
	return nil
}

// loadJobSalaries loads job salary ranges
func (c *Cache) loadJobSalaries(db *sql.DB) error {
	rows, err := db.Query("SELECT JOB_CODE, MIN_SALARY, MAX_SALARY FROM JOB")
	if err != nil {
		return fmt.Errorf("query job salaries: %w", err)
	}
	defer rows.Close()

	jobSalaries := make(map[string]JobSalaryRange)
	for rows.Next() {
		var jobCode string
		var minSalary, maxSalary float64
		if err := rows.Scan(&jobCode, &minSalary, &maxSalary); err != nil {
			return fmt.Errorf("scan job salary: %w", err)
		}
		jobSalaries[jobCode] = JobSalaryRange{
			MinSalary: minSalary,
			MaxSalary: maxSalary,
		}
	}

	if len(jobSalaries) == 0 {
		return fmt.Errorf("no job salary ranges found")
	}

	c.JobSalaries = jobSalaries
	return nil
}

// RandomDeptNo returns a random department number
func (c *Cache) RandomDeptNo() string {
	return c.DeptNos[c.rng.Intn(len(c.DeptNos))]
}

// RandomEmpNo returns a random employee number
func (c *Cache) RandomEmpNo() int {
	return c.EmpNos[c.rng.Intn(len(c.EmpNos))]
}

// RandomProjId returns a random project ID
func (c *Cache) RandomProjId() string {
	return c.ProjIds[c.rng.Intn(len(c.ProjIds))]
}

// RandomCustNo returns a random customer number
func (c *Cache) RandomCustNo() int {
	return c.CustNos[c.rng.Intn(len(c.CustNos))]
}

// RandomJobSalaryRange returns a random job salary range
func (c *Cache) RandomJobSalaryRange() (string, JobSalaryRange) {
	keys := make([]string, 0, len(c.JobSalaries))
	for k := range c.JobSalaries {
		keys = append(keys, k)
	}
	jobCode := keys[c.rng.Intn(len(keys))]
	return jobCode, c.JobSalaries[jobCode]
}

// RandomSalaryInRange returns a random salary within the given range
func (c *Cache) RandomSalaryInRange(min, max float64) float64 {
	if min >= max {
		return min
	}
	return min + c.rng.Float64()*(max-min)
}

// RandomString generates a random string of given length
func (c *Cache) RandomString(length int) string {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[c.rng.Intn(len(charset))]
	}
	return string(b)
}

// RandomName generates a random customer name
func (c *Cache) RandomName() string {
	firstNames := []string{"John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank"}
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"}
	return firstNames[c.rng.Intn(len(firstNames))] + " " + lastNames[c.rng.Intn(len(lastNames))]
}

// RandomAddress generates a random address
func (c *Cache) RandomAddress() string {
	streets := []string{"Main St", "Oak Ave", "Pine Rd", "Maple Dr", "Cedar Ln"}
	cities := []string{"Springfield", "Shelbyville", "Ogdenville", "North Haverbrook", "Capital City"}
	return fmt.Sprintf("%d %s, %s", c.rng.Intn(9999)+1000, streets[c.rng.Intn(len(streets))], cities[c.rng.Intn(len(cities))])
}

// RandomCountry returns a random country
func (c *Cache) RandomCountry() string {
	countries := []string{"USA", "Canada", "UK", "Germany", "France", "Japan", "Australia", "Brazil"}
	return countries[c.rng.Intn(len(countries))]
}

// RandomCitySimple returns a random city name
func (c *Cache) RandomCitySimple() string {
	cities := []string{"Springfield", "Shelbyville", "Ogdenville", "North Haverbrook", "Capital City",
		"Metropolis", "Gotham", "Star City", "Central City", "National City",
		"Boston", "Chicago", "Seattle", "Portland", "Denver"}
	return cities[c.rng.Intn(len(cities))]
}

// RandomOrderStatus returns a random order status
func (c *Cache) RandomOrderStatus() string {
	statuses := []string{"new", "open", "shipped", "waiting"}
	return statuses[c.rng.Intn(len(statuses))]
}

// RandomPaid returns a random paid status
func (c *Cache) RandomPaid() string {
	if c.rng.Intn(2) == 0 {
		return "y"
	}
	return "n"
}

// RandomOnHold returns a random on-hold status
func (c *Cache) RandomOnHold() *string {
	if c.rng.Intn(10) == 0 { // 10% chance of being on hold
		value := "*"
		return &value
	}
	return nil
}

// RandomDiscount returns a random discount between 0 and 1
func (c *Cache) RandomDiscount() float64 {
	return c.rng.Float64() * 0.1 // 0-10% discount
}

// RandomPONumber generates a random PO number starting with 'V'
func (c *Cache) RandomPONumber() string {
	return "V" + c.RandomString(7)
}

// RandomPercentChange returns a random percent change between -50 and 50
func (c *Cache) RandomPercentChange() float64 {
	return (c.rng.Float64() * 100) - 50
}

// CacheStats returns statistics about the cache
func (c *Cache) CacheStats() string {
	return fmt.Sprintf("Cache: %d depts, %d emps, %d projs, %d custs, %d jobs",
		len(c.DeptNos), len(c.EmpNos), len(c.ProjIds), len(c.CustNos), len(c.JobSalaries))
}

// GetStats returns cache statistics (for metrics compatibility)
func (c *Cache) GetStats() string {
	return c.CacheStats()
}
