module fb-loadgen

go 1.24.5

require github.com/nakagami/firebirdsql v0.9.17

require (
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/nakagami/chacha20 v0.1.0 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	gitlab.com/nyarla/go-crypt v0.0.0-20160106005555-d9a5dc2b789b // indirect
	golang.org/x/text v0.22.0 // indirect
)

// Local development checkout of the IBSurgeon fork with the extended-load
// TPB/completion-intent work (branches extended-load-intents):
//   replace github.com/nakagami/firebirdsql => ../firebirdsql
// When the fork branch is pushed, bump the pin below to the new pseudo-version
// and delete the local replace.
replace github.com/nakagami/firebirdsql => ../firebirdsql
