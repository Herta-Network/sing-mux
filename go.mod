module github.com/metacubex/sing-mux

go 1.20

require (
	github.com/metacubex/sing v0.5.7
	github.com/metacubex/smux v0.0.0-20250503055512-501391591dee
	github.com/metacubex/yamux v0.0.0-20250918083631-dd5f17c0be49
	golang.org/x/net v0.34.0
	golang.org/x/sys v0.30.0
)

require golang.org/x/text v0.21.0 // indirect

replace github.com/metacubex/sing => github.com/Herta-Network/sing v0.0.0-20261101103341-a402b0b490ba
