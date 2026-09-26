module fb-loadgen

go 1.24.5

require (
	github.com/nakagami/firebirdsql v0.9.17
	github.com/robfig/cron/v3 v3.0.1
)

require (
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/nakagami/chacha20 v0.1.0 // indirect
	gitlab.com/nyarla/go-crypt v0.0.0-20160106005555-d9a5dc2b789b // indirect
	golang.org/x/text v0.22.0 // indirect
)

// IBSurgeon fork with the extended-load TPB/completion-intent work
// (branch extended-load-intents, tip 63dc5d0). The fork keeps the upstream
// module path github.com/nakagami/firebirdsql, so the replace maps it back.
replace github.com/nakagami/firebirdsql => github.com/IBSurgeon/firebirdsql-go v0.0.0-20260926185108-63dc5d092c73
