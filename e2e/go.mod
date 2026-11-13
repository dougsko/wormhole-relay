module github.com/dougsko/wormhole-relay/e2e

go 1.26.0

require (
	github.com/dougsko/wormhole-william v1.0.8
	modernc.org/sqlite v1.62.0
)

require (
	github.com/dustin/go-humanize v1.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.1.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	modernc.org/libc v1.80.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
	nhooyr.io/websocket v1.8.17 // indirect
)

// The E2E suite deliberately tests the wormhole-william library from the
// local checkout so that uncommitted client changes are exercised too.
// The repos are expected to be siblings (see README.md); if they are not,
// point the replace directive at your wormhole-william clone instead.
replace github.com/dougsko/wormhole-william => ../../wormhole-william
