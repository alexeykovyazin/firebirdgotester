package session

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/db"
	"fb-loadgen/discover"
	"fb-loadgen/errlog"
	"fb-loadgen/metrics"
	"fb-loadgen/ops"
	"fb-loadgen/profile"
	"fb-loadgen/ramp"
	"fb-loadgen/worker"
)

// Manager owns all database sessions for the web UI control plane.
type Manager struct {
	mu sync.RWMutex

	shared       *config.Config
	host         string
	port         int
	user         string
	pass         string
	discoverDir  string
	discoverMask string
	recursive    bool
	maxTotal     int
	settingsPath string

	hist *RunHistory

	reserved atomic.Int64 // sum of ConnMax for active reserved sessions

	sessions map[string]*Session // keyed by AbsPath
}

// Session is one database under management.
type Session struct {
	mu sync.Mutex

	ID     string
	Config SessionConfig
	Status Status

	LastError string
	Missing   bool

	generation int64
	reserved   int // ConnMax reserved against budget

	runID string // run this session's current activity belongs to

	timeLimitMin int // 0 = no limit; run phases are scaled to fill this total

	pauseGate  *worker.PauseGate
	scheduler  *ramp.Scheduler
	metrics    *worker.MetricsCollector
	factory    *db.ConnectionFactory
	cache      *ops.Cache
	sysMetrics *metrics.MetricsCollector
	reporter   *metrics.Reporter
	errorLog   *errlog.Logger

	runCfg        *config.Config
	reportDir     string
	lastReportDir string

	// Retained after Completed
	lastSnap Snapshot
	hasLast  bool
}

// NewManager creates a session manager with shared defaults from CLI config.
func NewManager(cfg *config.Config) *Manager {
	return &Manager{
		shared:       cfg,
		host:         cfg.Host,
		port:         cfg.Port,
		user:         cfg.User,
		pass:         cfg.Pass,
		discoverDir:  cfg.DiscoverDir,
		discoverMask: cfg.DiscoverMask,
		recursive:    cfg.DiscoverRecursive,
		maxTotal:     cfg.MaxTotalConns,
		settingsPath: config.DefaultUISettingsFile,
		hist:         NewRunHistory(DefaultRunsFile),
		sessions:     make(map[string]*Session),
	}
}

// History returns the run history registry.
func (m *Manager) History() *RunHistory {
	return m.hist
}

// SetHistoryPath points the run history at a persistence path and loads it.
func (m *Manager) SetHistoryPath(path string) error {
	if path == "" {
		return nil
	}
	m.mu.Lock()
	m.hist = NewRunHistory(path)
	hist := m.hist
	m.mu.Unlock()
	return hist.Load()
}

// SetSettingsPath sets where UI settings are persisted.
func (m *Manager) SetSettingsPath(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if path != "" {
		m.settingsPath = path
	}
}

// SettingsPath returns the persistence file path.
func (m *Manager) SettingsPath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.settingsPath
}

// SharedConfig returns the shared CLI config.
func (m *Manager) SharedConfig() *config.Config {
	return m.shared
}

// DiscoverDir returns the allowlisted discovery root.
func (m *Manager) DiscoverDir() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.discoverDir
}

// MaxTotalConns returns the connection budget limit.
func (m *Manager) MaxTotalConns() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.maxTotal
}

// ConnectionSettings returns the current shared Firebird connection settings.
func (m *Manager) ConnectionSettings() config.UISettings {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return config.UISettings{
		Version:           config.UISettingsVersion,
		Host:              m.host,
		Port:              m.port,
		User:              m.user,
		Pass:              m.pass,
		DiscoverDir:       m.discoverDir,
		DiscoverMask:      m.discoverMask,
		DiscoverRecursive: m.recursive,
		MaxTotalConns:     m.maxTotal,
	}
}

// HasRunningSessions reports whether any session is actively running.
func (m *Manager) HasRunningSessions() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.sessions {
		s.mu.Lock()
		st := s.Status
		s.mu.Unlock()
		if st == StatusRunning || st == StatusPaused || st == StatusStarting {
			return true
		}
	}
	return false
}

// UpdateConnectionSettings applies and optionally persists Firebird/UI settings.
func (m *Manager) UpdateConnectionSettings(s config.UISettings, persist bool) error {
	prev := m.ConnectionSettings()
	s = s.MergePassKeepExisting(prev)
	if s.DiscoverMask == "" {
		s.DiscoverMask = "*.fdb"
	}
	if s.MaxTotalConns < 1 {
		s.MaxTotalConns = prev.MaxTotalConns
		if s.MaxTotalConns < 1 {
			s.MaxTotalConns = 200
		}
	}
	if err := s.Validate(); err != nil {
		return err
	}

	m.mu.Lock()
	m.host = s.Host
	m.port = s.Port
	m.user = s.User
	m.pass = s.Pass
	m.discoverDir = s.DiscoverDir
	m.discoverMask = s.DiscoverMask
	m.recursive = s.DiscoverRecursive
	m.maxTotal = s.MaxTotalConns
	m.shared.Host = s.Host
	m.shared.Port = s.Port
	m.shared.User = s.User
	m.shared.Pass = s.Pass
	m.shared.DiscoverDir = s.DiscoverDir
	m.shared.DiscoverMask = s.DiscoverMask
	m.shared.DiscoverRecursive = s.DiscoverRecursive
	m.shared.MaxTotalConns = s.MaxTotalConns

	for _, sess := range m.sessions {
		sess.mu.Lock()
		if sess.Status == StatusIdle || sess.Status == StatusFailed || sess.Status == StatusCompleted {
			sess.Config.User = s.User
			sess.Config.Pass = s.Pass
			sess.Config.DSN = discover.BuildDSN(s.Host, s.Port, sess.Config.AbsPath)
		}
		sess.mu.Unlock()
	}
	m.applyEvenConnBudgetLocked()
	path := m.settingsPath
	m.mu.Unlock()

	m.rebalanceLiveBudget()

	if persist {
		return m.persistSettings(path, s)
	}
	return nil
}

// rebalanceLiveBudget applies the current even per-DB split to running
// sessions: their connection caps and budget reservations shrink or grow
// live (workers adjust within a scheduler tick). Caller must not hold m.mu.
func (m *Manager) rebalanceLiveBudget() {
	type adj struct {
		s        *Session
		from, to int
	}
	var adjs []adj

	m.mu.RLock()
	n := 0
	for _, s := range m.sessions {
		s.mu.Lock()
		if !s.Missing {
			n++
		}
		s.mu.Unlock()
	}
	per := EvenPerDBMax(m.maxTotal, n)
	for _, s := range m.sessions {
		s.mu.Lock()
		if (s.Status == StatusRunning || s.Status == StatusPaused) && s.scheduler != nil && s.reserved > 0 && s.reserved != per {
			adjs = append(adjs, adj{s, s.reserved, per})
		}
		s.mu.Unlock()
	}
	m.mu.RUnlock()

	for _, a := range adjs {
		if a.to < a.from {
			a.s.mu.Lock()
			if a.s.reserved == a.from && a.s.scheduler != nil {
				a.s.scheduler.SetConnectionMax(a.to)
				a.s.reserved = a.to
				a.s.mu.Unlock()
				m.releaseBudget(a.from - a.to)
				continue
			}
			a.s.mu.Unlock()
		} else if a.to > a.from {
			if err := m.reserveBudget(a.to - a.from); err != nil {
				continue // budget exhausted; keep the current cap
			}
			a.s.mu.Lock()
			if a.s.reserved == a.from && a.s.scheduler != nil {
				a.s.scheduler.SetConnectionMax(a.to)
				a.s.reserved = a.to
				a.s.mu.Unlock()
				continue
			}
			a.s.mu.Unlock()
			m.releaseBudget(a.to - a.from)
		}
	}
}

func (m *Manager) persistSettings(path string, base config.UISettings) error {
	m.mu.RLock()
	sessions := make(map[string]config.SessionPrefs, len(m.sessions))
	for abs, sess := range m.sessions {
		sess.mu.Lock()
		sessions[abs] = PrefsFromConfig(sess.Config)
		sess.mu.Unlock()
	}
	m.mu.RUnlock()

	// Merge with any previously saved sessions for removed rows that we still want?
	// Plan: Remove drops entry; Save connection keeps current table only.
	base.Version = config.UISettingsVersion
	base.Sessions = sessions
	return config.SaveUISettings(path, base)
}

func (m *Manager) saveSessionPrefs() {
	path := m.SettingsPath()
	s := m.ConnectionSettings()
	_ = m.persistSettings(path, s)
}

// Discover runs filesystem discovery and upserts idle sessions by AbsPath.
func (m *Manager) Discover(subdir string, mask string, recursive *bool, rootOverride string) ([]Snapshot, error) {
	m.mu.Lock()
	if rootOverride != "" {
		abs, err := filepath.Abs(rootOverride)
		if err != nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("discover dir: %w", err)
		}
		m.discoverDir = abs
		m.shared.DiscoverDir = abs
	}
	baseDir := m.discoverDir
	defaultMask := m.discoverMask
	host := m.host
	port := m.port
	user := m.user
	pass := m.pass
	recDefault := m.recursive
	settingsPath := m.settingsPath
	m.mu.Unlock()

	root := baseDir
	if subdir != "" {
		resolved, err := discover.ResolveUnderRoot(baseDir, subdir)
		if err != nil {
			return nil, err
		}
		root = resolved
	}
	if mask == "" {
		mask = defaultMask
	}
	rec := recDefault
	if recursive != nil {
		rec = *recursive
	}

	found, err := discover.Discover(discover.Options{
		Root:      root,
		Mask:      mask,
		Recursive: rec,
	})
	if err != nil {
		return nil, err
	}

	saved, _, _ := config.LoadUISettings(settingsPath)
	prefs := saved.Sessions
	if prefs == nil {
		prefs = map[string]config.SessionPrefs{}
	}

	foundSet := make(map[string]discover.DatabaseInfo, len(found))
	for _, info := range found {
		foundSet[info.AbsPath] = info
	}

	m.mu.Lock()
	m.recursive = rec
	m.shared.DiscoverRecursive = rec

	for abs, info := range foundSet {
		if existing, ok := m.sessions[abs]; ok {
			existing.mu.Lock()
			existing.Missing = false
			if existing.Status == StatusIdle || existing.Status == StatusFailed || existing.Status == StatusCompleted {
				existing.Config.Name = info.Name
				existing.Config.RelPath = info.RelPath
				existing.Config.AbsPath = info.AbsPath
				existing.Config.User = user
				existing.Config.Pass = pass
				existing.Config.DSN = discover.BuildDSN(host, port, info.AbsPath)
			}
			existing.mu.Unlock()
			continue
		}

		sc := DefaultsFromCLI(m.shared, info, host, port)
		sc.User = user
		sc.Pass = pass
		if p, ok := prefs[abs]; ok {
			ApplyPrefs(&sc, p)
		}
		m.sessions[abs] = &Session{
			ID:     IDFromAbsPath(abs),
			Config: sc,
			Status: StatusIdle,
		}
	}

	for abs, sess := range m.sessions {
		if _, ok := foundSet[abs]; ok {
			continue
		}
		sess.mu.Lock()
		if sess.Status == StatusIdle || sess.Status == StatusFailed || sess.Status == StatusCompleted {
			sess.Missing = true
		}
		sess.mu.Unlock()
	}

	m.applyEvenConnBudgetLocked()
	snaps := m.snapshotsLocked()
	m.mu.Unlock()
	m.saveSessionPrefs()
	return snaps, nil
}

// List returns snapshots of all sessions.
func (m *Manager) List() []Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snapshotsLocked()
}

// Get returns a snapshot of one session by ID.
func (m *Manager) Get(id string) (Snapshot, error) {
	s, err := m.findByID(id)
	if err != nil {
		return Snapshot{}, err
	}
	return s.Snapshot(), nil
}

func (m *Manager) snapshotsLocked() []Snapshot {
	out := make([]Snapshot, 0, len(m.sessions))
	for _, s := range m.sessions {
		out = append(out, s.Snapshot())
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].AbsPath < out[j].AbsPath
	})
	return out
}

// applyEvenConnBudgetLocked sets ConnMax = maxTotal / N for every non-missing
// editable session so the fleet can start without oversubscribing the budget.
// Caller must hold m.mu.
func (m *Manager) applyEvenConnBudgetLocked() {
	n := 0
	for _, s := range m.sessions {
		s.mu.Lock()
		if !s.Missing {
			n++
		}
		s.mu.Unlock()
	}
	per := EvenPerDBMax(m.maxTotal, n)
	for _, s := range m.sessions {
		s.mu.Lock()
		if s.Missing {
			s.mu.Unlock()
			continue
		}
		switch s.Status {
		case StatusIdle, StatusFailed, StatusCompleted:
			s.Config.ConnMax = per
			if s.Config.ConnMin > s.Config.ConnMax {
				s.Config.ConnMin = s.Config.ConnMax
			}
			if s.Config.ConnMin < 1 {
				s.Config.ConnMin = 1
			}
		}
		s.mu.Unlock()
	}
}

// PerDBMax returns the even-split ConnMax for the current fleet size.
func (m *Manager) PerDBMax() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := 0
	for _, s := range m.sessions {
		s.mu.Lock()
		if !s.Missing {
			n++
		}
		s.mu.Unlock()
	}
	return EvenPerDBMax(m.maxTotal, n)
}

func (m *Manager) findByID(id string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.sessions {
		if s.ID == id {
			return s, nil
		}
	}
	return nil, fmt.Errorf("session not found: %s", id)
}

// Patch updates editable fields when Idle/Failed/Completed.
func (m *Manager) Patch(id string, patch map[string]interface{}) (Snapshot, error) {
	s, err := m.findByID(id)
	if err != nil {
		return Snapshot{}, err
	}

	s.mu.Lock()
	switch s.Status {
	case StatusIdle, StatusFailed, StatusCompleted:
	default:
		st := s.Status
		s.mu.Unlock()
		return Snapshot{}, fmt.Errorf("can only edit session when Idle (status=%s)", st)
	}

	if v, ok := patch["profile"].(string); ok && v != "" {
		s.Config.Profile = v
	}
	if v, ok := asInt(patch["connMin"]); ok {
		s.Config.ConnMin = v
	}
	// ConnMax is managed by the even fleet budget (maxTotal / N); ignore client value.
	if v, ok := asInt(patch["warmup"]); ok {
		s.Config.Warmup = v
	}
	if v, ok := asInt(patch["main"]); ok {
		s.Config.Main = v
	}
	if v, ok := asInt(patch["cooldown"]); ok {
		s.Config.Cooldown = v
	}
	if v, ok := asInt(patch["spikeCycles"]); ok {
		s.Config.SpikeCycles = v
	}
	if v, ok := asInt(patch["spikeHold"]); ok {
		s.Config.SpikeHold = v
	}
	if v, ok := asInt(patch["thinkMs"]); ok {
		s.Config.ThinkMs = v
	}
	if v, ok := asInt(patch["txTimeout"]); ok {
		s.Config.TxTimeout = v
	}
	s.Status = StatusIdle
	s.LastError = ""
	s.mu.Unlock()

	m.mu.Lock()
	m.applyEvenConnBudgetLocked()
	m.mu.Unlock()

	s.mu.Lock()
	if err := s.Config.Validate(); err != nil {
		s.mu.Unlock()
		return Snapshot{}, err
	}
	snap := s.snapshotLocked()
	s.mu.Unlock()

	m.saveSessionPrefs()
	return snap, nil
}

func asInt(v interface{}) (int, bool) {
	switch t := v.(type) {
	case float64:
		return int(t), true
	case int:
		return t, true
	case int64:
		return int(t), true
	default:
		return 0, false
	}
}

func (m *Manager) reserveBudget(n int) error {
	limit := int64(m.MaxTotalConns())
	for {
		cur := m.reserved.Load()
		if cur+int64(n) > limit {
			return fmt.Errorf("max-total-conns budget exceeded: would need %d, limit %d", cur+int64(n), limit)
		}
		if m.reserved.CompareAndSwap(cur, cur+int64(n)) {
			return nil
		}
	}
}

func (m *Manager) releaseBudget(n int) {
	if n <= 0 {
		return
	}
	for {
		cur := m.reserved.Load()
		next := cur - int64(n)
		if next < 0 {
			next = 0
		}
		if m.reserved.CompareAndSwap(cur, next) {
			return
		}
	}
}

// Start begins a timed run for the session. A positive timeLimitMin scales
// warmup/main/cooldown proportionally so the run totals exactly that duration.
func (m *Manager) Start(id string, timeLimitMin int) (Snapshot, error) {
	return m.StartWithSpec(id, RunSpec{TimeLimitMin: timeLimitMin})
}

// StartWithSpec starts the session with a time limit plus typed overrides,
// recording a singleton manual run for history.
func (m *Manager) StartWithSpec(id string, spec RunSpec) (Snapshot, error) {
	if spec.TimeLimitMin < 0 {
		return Snapshot{}, fmt.Errorf("timeLimitMin must be >= 0")
	}
	s, err := m.findByID(id)
	if err != nil {
		return Snapshot{}, err
	}
	s.mu.Lock()
	rel := s.Config.RelPath
	s.mu.Unlock()
	run := m.hist.Create(OriginManual, "", []SessionRun{{SessionID: id, RelPath: rel}})
	snap, err := m.startInternal(s, spec, run.ID)
	m.hist.MaybeFinish(run.ID)
	return snap, err
}

// entrySkipped marks a session entry skipped with a reason (idempotent).
func (m *Manager) entrySkipped(runID, sessionID, reason string) {
	m.hist.UpdateSession(runID, sessionID, SessionRun{Status: "Skipped", Reason: reason})
}

// entryFail marks a session entry failed with a reason (idempotent).
func (m *Manager) entryFail(runID, sessionID, reason string) {
	m.hist.UpdateSession(runID, sessionID, SessionRun{Status: string(StatusFailed), LastError: reason})
}

// recordFinishEntry clears the session's run attribution and writes the
// terminal result into the run history.
func (m *Manager) recordFinishEntry(s *Session, entry SessionRun) {
	s.mu.Lock()
	runID := s.runID
	s.runID = ""
	s.mu.Unlock()
	if runID == "" {
		return
	}
	m.hist.UpdateSession(runID, s.ID, entry)
}

func runEntryFromSnap(snap Snapshot, status string) SessionRun {
	return SessionRun{
		SessionID: snap.ID,
		RelPath:   snap.RelPath,
		Status:    status,
		LastError: snap.LastError,
		ReportDir: snap.ReportDir,
		TPS:       snap.TPS,
		Success:   snap.Success,
		Errors:    snap.Errors,
	}
}

// runEntryLocked builds a history entry from live metrics; call before
// cleanup drops them.
func (s *Session) runEntryLocked(status string) SessionRun {
	return runEntryFromSnap(s.snapshotLocked(), status)
}

// applyOverrides overlays per-fire overrides onto a session config copy.
// Precedence: override > time-limit scaling > session config.
func applyOverrides(c *SessionConfig, o RunOverrides) {
	if o.Profile != nil && *o.Profile != "" {
		c.Profile = *o.Profile
	}
	if o.ThinkMs != nil && *o.ThinkMs >= 0 {
		c.ThinkMs = *o.ThinkMs
	}
	if o.TxTimeout != nil && *o.TxTimeout >= 1 {
		c.TxTimeout = *o.TxTimeout
	}
	if o.SpikeCycles != nil && *o.SpikeCycles >= 1 {
		c.SpikeCycles = *o.SpikeCycles
	}
	if o.SpikeHold != nil && *o.SpikeHold >= 1 {
		c.SpikeHold = *o.SpikeHold
	}
}

// startInternal runs the start state machine; runID is already allocated.
// Every synchronous failure path marks the run entry terminal.
func (m *Manager) startInternal(s *Session, spec RunSpec, runID string) (Snapshot, error) {
	timeLimitMin := spec.TimeLimitMin
	s.mu.Lock()
	switch s.Status {
	case StatusIdle, StatusFailed, StatusCompleted:
	default:
		st := s.Status
		s.mu.Unlock()
		m.entryFail(runID, s.ID, fmt.Sprintf("cannot start session in status %s", st))
		return Snapshot{}, fmt.Errorf("cannot start session in status %s", st)
	}
	if s.Missing {
		s.mu.Unlock()
		m.entrySkipped(runID, s.ID, "database file is missing")
		return Snapshot{}, fmt.Errorf("database file is missing")
	}
	cfgCopy := s.Config
	s.mu.Unlock()

	applyOverrides(&cfgCopy, spec.Overrides)
	if err := cfgCopy.Validate(); err != nil {
		s.mu.Lock()
		s.Status = StatusFailed
		s.LastError = err.Error()
		snap := s.snapshotLocked()
		s.mu.Unlock()
		m.entryFail(runID, s.ID, err.Error())
		return snap, err
	}
	if timeLimitMin > 0 {
		scalePhases(&cfgCopy, time.Duration(timeLimitMin)*time.Minute)
	} else {
		// No limit: fixed 1-minute warmup, then main until explicit stop.
		cfgCopy.Warmup = 60
		cfgCopy.Main = 0
		cfgCopy.Cooldown = 0
	}
	s.mu.Lock()
	s.generation++
	gen := s.generation
	s.Status = StatusStarting
	s.LastError = ""
	s.Missing = false
	s.hasLast = false
	s.timeLimitMin = timeLimitMin
	s.runID = runID
	s.mu.Unlock()

	m.saveSessionPrefs()

	if err := m.reserveBudget(cfgCopy.ConnMax); err != nil {
		s.mu.Lock()
		if s.generation == gen {
			s.Status = StatusIdle
			s.LastError = err.Error()
		}
		snap := s.snapshotLocked()
		s.mu.Unlock()
		m.entrySkipped(runID, s.ID, err.Error())
		return snap, err
	}

	fail := func(err error) (Snapshot, error) {
		m.releaseBudget(cfgCopy.ConnMax)
		s.mu.Lock()
		if s.generation == gen {
			s.Status = StatusFailed
			s.LastError = err.Error()
			s.reserved = 0
		}
		snap := s.snapshotLocked()
		s.mu.Unlock()
		m.entryFail(runID, s.ID, err.Error())
		return snap, err
	}

	reportEvery := 5
	if m.shared != nil && m.shared.ReportEvery > 0 {
		reportEvery = m.shared.ReportEvery
	}
	runCfg := cfgCopy.ToRunConfigWithReportEvery(reportEvery)
	if timeLimitMin == 0 {
		runCfg.UnboundedMain = true
	}
	factory := db.NewConnectionFactory(runCfg)

	if err := factory.ValidateSchemaGate(); err != nil {
		return fail(err)
	}

	cache, err := ops.NewCache(factory)
	if err != nil {
		return fail(err)
	}

	readOps := ops.NewReadOperations(factory, cache)
	writeOps := ops.NewWriteOperations(factory, cache)
	profFactory := profile.NewProfileFactory(readOps, writeOps, cache)
	prof, err := profFactory.CreateProfile(cfgCopy.Profile)
	if err != nil {
		return fail(err)
	}

	if sp, ok := prof.(*profile.SpikeProfile); ok {
		hold := time.Duration(cfgCopy.SpikeHold) * time.Second
		cycles := cfgCopy.SpikeCycles
		between := 30 * time.Second
		if cycles > 0 && cfgCopy.Main > 0 {
			mainDur := time.Duration(cfgCopy.Main) * time.Second
			rem := mainDur - time.Duration(cycles)*hold
			if rem > 0 {
				between = rem / time.Duration(cycles)
			}
		}
		sp.SetSpikeConfiguration(cycles, hold, between)
	}

	reportDir, err := m.newReportDir(cfgCopy.RelPath)
	if err != nil {
		return fail(err)
	}

	pause := worker.NewPauseGate()
	wMetrics := worker.NewMetricsCollector()

	errLogPath := filepath.Join(reportDir, "sql_errors.log")
	sqlErrLog, logErr := errlog.Open(errLogPath, cfgCopy.AbsPath)
	if logErr != nil {
		return fail(fmt.Errorf("error log: %w", logErr))
	}
	wMetrics.SetErrorLogger(sqlErrLog)

	sched := ramp.NewSchedulerWithPause(runCfg, factory, cache, prof, wMetrics, pause)

	sysMetrics := metrics.NewMetricsCollector(sched, prof, cache, wMetrics)
	sysMetrics.Start()

	baseName := filepath.Join(reportDir, "results")
	outFile, err := createFile(baseName + ".txt")
	if err != nil {
		sysMetrics.Stop()
		_ = sqlErrLog.Close()
		return fail(err)
	}
	reporter := metrics.NewReporter(sysMetrics, outFile, "text", time.Duration(reportEvery)*time.Second)
	reporter.Start()

	if err := sched.Start(); err != nil {
		reporter.Stop()
		sysMetrics.Stop()
		_ = outFile.Close()
		_ = sqlErrLog.Close()
		return fail(err)
	}

	s.mu.Lock()
	if s.generation != gen || s.Status != StatusStarting {
		s.mu.Unlock()
		_ = sched.Stop()
		reporter.Stop()
		sysMetrics.Stop()
		_ = outFile.Close()
		_ = sqlErrLog.Close()
		m.releaseBudget(cfgCopy.ConnMax)
		m.entryFail(runID, s.ID, "start aborted")
		s.mu.Lock()
		snap := s.snapshotLocked()
		s.mu.Unlock()
		return snap, fmt.Errorf("start aborted")
	}
	s.pauseGate = pause
	s.scheduler = sched
	s.metrics = wMetrics
	s.factory = factory
	s.cache = cache
	s.sysMetrics = sysMetrics
	s.reporter = reporter
	s.errorLog = sqlErrLog
	s.runCfg = runCfg
	s.reportDir = reportDir
	s.lastReportDir = reportDir
	s.reserved = cfgCopy.ConnMax
	s.Status = StatusRunning
	snap := s.snapshotLocked()
	s.mu.Unlock()

	go m.watchCompletion(s, gen, outFile, baseName)

	return snap, nil
}

// scalePhases resizes warmup/main/cooldown proportionally so they sum to
// total seconds; the rounding remainder lands in main. A non-positive base
// schedule puts the whole duration into main.
func scalePhases(c *SessionConfig, total time.Duration) {
	secs := int(total.Seconds())
	if secs < 1 {
		return
	}
	base := c.Warmup + c.Main + c.Cooldown
	if base <= 0 {
		c.Warmup, c.Main, c.Cooldown = 0, secs, 0
		return
	}
	f := float64(secs) / float64(base)
	w := int(math.Round(float64(c.Warmup) * f))
	cd := int(math.Round(float64(c.Cooldown) * f))
	m := secs - w - cd
	if m < 0 {
		m = 0
	}
	c.Warmup, c.Main, c.Cooldown = w, m, cd
}

func (m *Manager) watchCompletion(s *Session, gen int64, outFile *os.File, baseName string) {
	s.mu.Lock()
	sched := s.scheduler
	reporter := s.reporter
	sysMetrics := s.sysMetrics
	errorLog := s.errorLog
	s.errorLog = nil
	s.mu.Unlock()
	if sched == nil {
		return
	}
	<-sched.Done()

	if reporter != nil {
		_ = reporter.ReportAllToFile(baseName)
		reporter.Stop()
	}
	if sysMetrics != nil {
		sysMetrics.Stop()
	}
	if outFile != nil {
		_ = outFile.Close()
	}
	if errorLog != nil {
		errorLog.Flush()
		_ = errorLog.Close()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.generation != gen {
		return
	}

	if s.Status == StatusStopping || s.Status == StatusIdle {
		n := s.reserved
		s.reserved = 0
		entry := s.runEntryLocked("Idle")
		s.Status = StatusIdle
		s.cleanupLocked(false)
		s.mu.Unlock()
		m.releaseBudget(n)
		m.recordFinishEntry(s, entry)
		s.mu.Lock()
		return
	}
	if s.Status == StatusRunning || s.Status == StatusPaused {
		// Natural completion
		snap := s.snapshotLocked()
		s.lastSnap = snap
		s.lastSnap.Status = StatusCompleted
		s.hasLast = true
		entry := runEntryFromSnap(snap, string(StatusCompleted))
		reserved := s.reserved
		s.reserved = 0
		if s.scheduler == sched {
			s.mu.Unlock()
			_ = sched.Stop()
			s.mu.Lock()
		}
		s.Status = StatusCompleted
		s.cleanupLocked(true)
		s.mu.Unlock()
		m.releaseBudget(reserved)
		m.recordFinishEntry(s, entry)
		s.mu.Lock()
	}
}

func (s *Session) cleanupLocked(retainMetrics bool) {
	s.scheduler = nil
	s.pauseGate = nil
	if !retainMetrics {
		s.metrics = nil
	} else {
		// keep metrics pointer for Completed snapshot until next Start
	}
	s.factory = nil
	s.cache = nil
	s.sysMetrics = nil
	s.reporter = nil
	s.errorLog = nil
	s.runCfg = nil
	s.reportDir = ""
}

// Pause freezes workers and phase clock.
func (m *Manager) Pause(id string) (Snapshot, error) {
	s, err := m.findByID(id)
	if err != nil {
		return Snapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Status != StatusRunning {
		return Snapshot{}, fmt.Errorf("can only pause Running session (status=%s)", s.Status)
	}
	if s.pauseGate != nil {
		s.pauseGate.Pause()
	}
	s.Status = StatusPaused
	return s.snapshotLocked(), nil
}

// Resume continues a paused session.
func (m *Manager) Resume(id string) (Snapshot, error) {
	s, err := m.findByID(id)
	if err != nil {
		return Snapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Status != StatusPaused {
		return Snapshot{}, fmt.Errorf("can only resume Paused session (status=%s)", s.Status)
	}
	if s.pauseGate != nil {
		s.pauseGate.Resume()
	}
	s.Status = StatusRunning
	return s.snapshotLocked(), nil
}

// Stop fast-cancels the session and waits until workers are drained.
func (m *Manager) Stop(id string) (Snapshot, error) {
	s, err := m.findByID(id)
	if err != nil {
		return Snapshot{}, err
	}

	s.mu.Lock()
	switch s.Status {
	case StatusRunning, StatusPaused, StatusStarting:
	default:
		st := s.Status
		s.mu.Unlock()
		return Snapshot{}, fmt.Errorf("cannot stop session in status %s", st)
	}
	s.generation++ // invalidate in-flight Start
	s.Status = StatusStopping
	if s.pauseGate != nil {
		s.pauseGate.Resume()
	}
	sched := s.scheduler
	reporter := s.reporter
	sysMetrics := s.sysMetrics
	errorLog := s.errorLog
	reportDir := s.reportDir
	reserved := s.reserved
	s.reserved = 0
	s.errorLog = nil
	s.mu.Unlock()

	if sched != nil {
		_ = sched.Stop()
	}
	if reporter != nil {
		if reportDir != "" {
			_ = reporter.ReportAllToFile(filepath.Join(reportDir, "results"))
		}
		reporter.Stop()
	}
	if sysMetrics != nil {
		sysMetrics.Stop()
	}
	if errorLog != nil {
		errorLog.Flush()
		_ = errorLog.Close()
	}

	m.releaseBudget(reserved)

	s.mu.Lock()
	s.Status = StatusIdle
	entry := s.runEntryLocked("Idle")
	s.cleanupLocked(false)
	snap := s.snapshotLocked()
	s.mu.Unlock()
	m.recordFinishEntry(s, entry)
	return snap, nil
}

// StartAll starts all Idle/Failed/Completed sessions with the given time limit.
// The whole batch is recorded as one manual run.
func (m *Manager) StartAll(timeLimitMin int) []error {
	type named struct {
		id, rel string
	}
	var ids []named
	m.mu.RLock()
	for _, s := range m.sessions {
		s.mu.Lock()
		if (s.Status == StatusIdle || s.Status == StatusFailed || s.Status == StatusCompleted) && !s.Missing {
			ids = append(ids, named{s.ID, s.Config.RelPath})
		}
		s.mu.Unlock()
	}
	m.mu.RUnlock()

	run := m.hist.Create(OriginManual, "", nil)
	for _, n := range ids {
		m.hist.AddSession(run.ID, SessionRun{SessionID: n.id, RelPath: n.rel})
	}

	var errs []error
	for _, n := range ids {
		s, err := m.findByID(n.id)
		if err != nil {
			m.hist.UpdateSession(run.ID, n.id, SessionRun{Status: "Skipped", Reason: err.Error()})
			errs = append(errs, fmt.Errorf("%s: %w", n.rel, err))
			continue
		}
		if _, err := m.startInternal(s, RunSpec{TimeLimitMin: timeLimitMin}, run.ID); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", n.rel, err))
		}
	}
	m.hist.MaybeFinish(run.ID)
	return errs
}

// ResolveTargets maps a target selection to session refs. Unknown IDs are
// returned separately so callers can record them as skipped.
func (m *Manager) ResolveTargets(all bool, ids []string) (refs []TargetRef, unknown []string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if all {
		for _, s := range m.sessions {
			s.mu.Lock()
			if !s.Missing {
				refs = append(refs, TargetRef{
					ID:      s.ID,
					RelPath: s.Config.RelPath,
					Status:  s.Status,
					Missing: s.Missing,
					ConnMax: s.Config.ConnMax,
				})
			}
			s.mu.Unlock()
		}
		sort.Slice(refs, func(i, j int) bool { return refs[i].RelPath < refs[j].RelPath })
		return refs, nil
	}
	for _, id := range ids {
		found := false
		for _, s := range m.sessions {
			s.mu.Lock()
			if s.ID == id {
				refs = append(refs, TargetRef{
					ID:      s.ID,
					RelPath: s.Config.RelPath,
					Status:  s.Status,
					Missing: s.Missing,
					ConnMax: s.Config.ConnMax,
				})
				found = true
			}
			s.mu.Unlock()
			if found {
				break
			}
		}
		if !found {
			unknown = append(unknown, id)
		}
	}
	return refs, unknown
}

// AnyBusy returns the subset of IDs currently active (Running/Starting/Paused/Stopping).
func (m *Manager) AnyBusy(ids []string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var busy []string
	for _, id := range ids {
		for _, s := range m.sessions {
			s.mu.Lock()
			ok := s.ID == id
			st := s.Status
			s.mu.Unlock()
			if ok {
				switch st {
				case StatusRunning, StatusStarting, StatusPaused, StatusStopping:
					busy = append(busy, id)
				}
				break
			}
		}
	}
	return busy
}

// BudgetFree returns the unreserved connection budget.
func (m *Manager) BudgetFree() int {
	return m.MaxTotalConns() - int(m.reserved.Load())
}

// StopSessions stops the given sessions (used by IfRunning=stopAndRun).
func (m *Manager) StopSessions(ids []string) []error {
	var errs []error
	for _, id := range ids {
		if _, err := m.Stop(id); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// CancelRun cancels a run: pending entries are marked Cancelled and any
// sessions still active under it are stopped. Entry updates from those
// stops are ignored (already terminal).
func (m *Manager) CancelRun(runID string) (Run, bool) {
	run, ok := m.hist.Get(runID)
	if !ok {
		return run, false
	}
	if run.FinishedAt != "" {
		return run, false
	}
	if !m.hist.Cancel(runID, "cancelled via API") {
		return run, false
	}
	for _, e := range run.Sessions {
		if s, err := m.findByID(e.SessionID); err == nil {
			s.mu.Lock()
			st := s.Status
			s.mu.Unlock()
			switch st {
			case StatusRunning, StatusPaused, StatusStarting, StatusStopping:
				_, _ = m.Stop(e.SessionID)
			}
		}
	}
	out, _ := m.hist.Get(runID)
	return out, true
}

// SkipTarget is a target that will never start, recorded upfront in the run.
type SkipTarget struct {
	ID      string `json:"id"`
	RelPath string `json:"relPath,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

// StartBulk starts a set of targets as one run, sequentially with optional
// stagger, honoring the busy policy. It blocks until all starts have been
// attempted; started sessions finish asynchronously. Used by the schedule
// engine (which calls it from its own fire goroutine).
func (m *Manager) StartBulk(origin RunOrigin, scheduleID string, refs []TargetRef, skips []SkipTarget, spec RunSpec, stagger time.Duration, stopBusy bool) Run {
	entries := make([]SessionRun, 0, len(refs)+len(skips))
	for _, ref := range refs {
		entries = append(entries, SessionRun{SessionID: ref.ID, RelPath: ref.RelPath})
	}
	for _, sk := range skips {
		entries = append(entries, SessionRun{SessionID: sk.ID, RelPath: sk.RelPath, Status: "Skipped", Reason: sk.Reason})
	}
	run := m.hist.Create(origin, scheduleID, entries)

	for i, ref := range refs {
		if i > 0 && stagger > 0 {
			time.Sleep(stagger)
		}
		s, err := m.findByID(ref.ID)
		if err != nil {
			m.hist.SkipSession(run.ID, ref.ID, ref.RelPath, "session removed")
			continue
		}
		s.mu.Lock()
		st, missing := s.Status, s.Missing
		s.mu.Unlock()
		if missing {
			m.hist.SkipSession(run.ID, ref.ID, ref.RelPath, "database file is missing")
			continue
		}
		switch st {
		case StatusIdle, StatusFailed, StatusCompleted:
		default:
			if !stopBusy {
				m.hist.SkipSession(run.ID, ref.ID, ref.RelPath, "busy: status "+string(st))
				continue
			}
			if _, err := m.Stop(ref.ID); err != nil {
				m.hist.SkipSession(run.ID, ref.ID, ref.RelPath, "cannot stop before start: "+err.Error())
				continue
			}
		}
		_, _ = m.startInternal(s, spec, run.ID)
	}
	m.hist.MaybeFinish(run.ID)
	out, _ := m.hist.Get(run.ID)
	return out
}

// StopAll stops all active sessions.
func (m *Manager) StopAll() []error {
	ids := m.activeIDs()
	type result struct {
		rel string
		err error
	}
	ch := make(chan result, len(ids))
	for _, id := range ids {
		go func(id string) {
			s, _ := m.findByID(id)
			rel := id
			if s != nil {
				s.mu.Lock()
				rel = s.Config.RelPath
				s.mu.Unlock()
			}
			_, err := m.Stop(id)
			ch <- result{rel, err}
		}(id)
	}
	var errs []error
	for range ids {
		r := <-ch
		if r.err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.rel, r.err))
		}
	}
	return errs
}

// PauseAll pauses all Running sessions.
func (m *Manager) PauseAll() []error {
	m.mu.RLock()
	var ids []string
	for _, s := range m.sessions {
		s.mu.Lock()
		if s.Status == StatusRunning {
			ids = append(ids, s.ID)
		}
		s.mu.Unlock()
	}
	m.mu.RUnlock()
	var errs []error
	for _, id := range ids {
		if _, err := m.Pause(id); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// Remove stops a session if active, then deletes it from the table.
func (m *Manager) Remove(id string) error {
	s, err := m.findByID(id)
	if err != nil {
		return err
	}

	s.mu.Lock()
	st := s.Status
	abs := s.Config.AbsPath
	s.mu.Unlock()

	if st == StatusRunning || st == StatusPaused || st == StatusStarting || st == StatusStopping {
		if _, err := m.Stop(id); err != nil {
			s.mu.Lock()
			st = s.Status
			s.mu.Unlock()
			if st != StatusIdle && st != StatusFailed && st != StatusCompleted {
				return err
			}
		}
	}

	m.mu.Lock()
	delete(m.sessions, abs)
	m.applyEvenConnBudgetLocked()
	m.mu.Unlock()
	m.saveSessionPrefs()
	return nil
}

// PurgeMissing removes Idle/Failed/Completed sessions marked missing.
func (m *Manager) PurgeMissing() int {
	m.mu.Lock()
	var remove []string
	for abs, s := range m.sessions {
		s.mu.Lock()
		if s.Missing && (s.Status == StatusIdle || s.Status == StatusFailed || s.Status == StatusCompleted) {
			remove = append(remove, abs)
		}
		s.mu.Unlock()
	}
	for _, abs := range remove {
		delete(m.sessions, abs)
	}
	if len(remove) > 0 {
		m.applyEvenConnBudgetLocked()
	}
	m.mu.Unlock()
	if len(remove) > 0 {
		m.saveSessionPrefs()
	}
	return len(remove)
}

// ValidateSession runs schema validation without starting load.
func (m *Manager) ValidateSession(id string) (Snapshot, error) {
	s, err := m.findByID(id)
	if err != nil {
		return Snapshot{}, err
	}
	s.mu.Lock()
	cfg := s.Config
	st := s.Status
	s.mu.Unlock()
	if st != StatusIdle && st != StatusFailed && st != StatusCompleted {
		return Snapshot{}, fmt.Errorf("can only validate Idle session")
	}
	runCfg := cfg.ToRunConfig()
	factory := db.NewConnectionFactory(runCfg)
	if err := factory.ValidateSchemaGate(); err != nil {
		s.mu.Lock()
		s.Status = StatusFailed
		s.LastError = err.Error()
		snap := s.snapshotLocked()
		s.mu.Unlock()
		return snap, err
	}
	s.mu.Lock()
	s.Status = StatusIdle
	s.LastError = ""
	snap := s.snapshotLocked()
	s.mu.Unlock()
	return snap, nil
}

// ValidateAll schema-checks all Idle/Failed/Completed rows.
func (m *Manager) ValidateAll() []error {
	ids := m.idleIDs()
	var errs []error
	for _, id := range ids {
		s, _ := m.findByID(id)
		rel := id
		if s != nil {
			s.mu.Lock()
			rel = s.Config.RelPath
			s.mu.Unlock()
		}
		if _, err := m.ValidateSession(id); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", rel, err))
		}
	}
	return errs
}

// Fleet returns aggregated fleet metrics.
func (m *Manager) Fleet() FleetSummary {
	snaps := m.List()
	dbs := 0
	for _, s := range snaps {
		if !s.Missing {
			dbs++
		}
	}
	limit := m.MaxTotalConns()
	f := FleetSummary{
		Databases:   dbs,
		PerDBMax:    EvenPerDBMax(limit, dbs),
		BudgetLimit: limit,
		BudgetUsed:  int(m.reserved.Load()),
		TopErrors:   map[string]int{},
	}
	for _, s := range snaps {
		switch s.Status {
		case StatusRunning:
			f.Running++
		case StatusPaused:
			f.Paused++
		case StatusIdle:
			f.Idle++
		case StatusFailed:
			f.Failed++
		case StatusCompleted:
			f.Completed++
		}
		if s.Missing {
			f.Missing++
		}
		f.TotalConns += s.CurrentConns
		f.TotalTPS += s.TPS
	}
	return f
}

func (m *Manager) idleIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var ids []string
	for _, s := range m.sessions {
		s.mu.Lock()
		if s.Status == StatusIdle || s.Status == StatusFailed || s.Status == StatusCompleted {
			ids = append(ids, s.ID)
		}
		s.mu.Unlock()
	}
	return ids
}

func (m *Manager) activeIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var ids []string
	for _, s := range m.sessions {
		s.mu.Lock()
		if s.Status == StatusRunning || s.Status == StatusPaused || s.Status == StatusStarting {
			ids = append(ids, s.ID)
		}
		s.mu.Unlock()
	}
	return ids
}

// Snapshot builds a UI snapshot.
func (s *Session) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Session) snapshotLocked() Snapshot {
	if s.Status == StatusCompleted && s.hasLast {
		out := s.lastSnap
		out.Status = StatusCompleted
		out.UpdatedAt = nowStamp()
		return out
	}

	phase := "—"
	target, current := 0, 0
	tps := 0.0
	var errors, success int64
	var expected, unexpected int64
	elapsed := 0.0
	var p50, p95, p99 int64
	var topOps []OpCount
	phaseProgress := 0.0
	remaining := 0.0

	if s.scheduler != nil {
		phase = s.scheduler.GetCurrentPhase().String()
		current = s.scheduler.GetCurrentWorkerCount()
		target = s.scheduler.GetTargetWorkerCount()
		elapsed = s.scheduler.GetElapsedTime().Seconds()
		total := float64(s.Config.Warmup + s.Config.Main + s.Config.Cooldown)
		if s.runCfg != nil {
			// Run totals come from the (possibly time-limit-scaled) run config
			total = float64(s.runCfg.Warmup + s.runCfg.Main + s.runCfg.Cooldown)
		}
		if total > 0 {
			phaseProgress = elapsed / total * 100
			if phaseProgress > 100 {
				phaseProgress = 100
			}
		}
		if s.timeLimitMin > 0 {
			remaining = total - elapsed
			if remaining < 0 {
				remaining = 0
			}
		}
	}
	if s.metrics != nil {
		tps = s.metrics.GetTPS()
		success = s.metrics.GetTxSuccess()
		errors = s.metrics.GetTxError()
		p50, p95, p99 = s.metrics.GetLatencyPercentiles()
		if es := s.metrics.ErrorStats(); es != nil {
			_, exp, unexp, _, _ := es.Snapshot()
			expected = int64(exp)
			unexpected = int64(unexp)
		}
		opsMap := s.metrics.GetOpCounts()
		type kv struct {
			k string
			v int64
		}
		var list []kv
		for k, v := range opsMap {
			list = append(list, kv{k, v})
		}
		sort.Slice(list, func(i, j int) bool { return list[i].v > list[j].v })
		for i := 0; i < len(list) && i < 5; i++ {
			topOps = append(topOps, OpCount{Name: list[i].k, Count: list[i].v})
		}
	}

	reportDir := s.reportDir
	if reportDir == "" {
		reportDir = s.lastReportDir
	}

	return Snapshot{
		ID:               s.ID,
		Name:             s.Config.Name,
		RelPath:          s.Config.RelPath,
		AbsPath:          s.Config.AbsPath,
		DSN:              s.Config.DSN,
		Status:           s.Status,
		Profile:          s.Config.Profile,
		ConnMin:          s.Config.ConnMin,
		ConnMax:          s.Config.ConnMax,
		Warmup:           s.Config.Warmup,
		Main:             s.Config.Main,
		Cooldown:         s.Config.Cooldown,
		SpikeCycles:      s.Config.SpikeCycles,
		SpikeHold:        s.Config.SpikeHold,
		ThinkMs:          s.Config.ThinkMs,
		TxTimeout:        s.Config.TxTimeout,
		Phase:            phase,
		PhaseProgress:    phaseProgress,
		TargetConns:      target,
		CurrentConns:     current,
		TPS:              tps,
		Errors:           errors,
		ExpectedErrors:   expected,
		UnexpectedErrors: unexpected,
		Success:          success,
		LastError:        s.LastError,
		ElapsedSec:       elapsed,
		TimeLimitMin:     s.timeLimitMin,
		RemainingSec:     remaining,
		LatencyP50:       p50,
		LatencyP95:       p95,
		LatencyP99:       p99,
		TopOps:           topOps,
		ReportDir:        reportDir,
		Missing:          s.Missing,
		UpdatedAt:        nowStamp(),
	}
}
