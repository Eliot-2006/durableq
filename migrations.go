// Package durableq is a PostgreSQL-backed durable job queue.
package durableq

import "embed"

// Migrations holds the embedded schema migrations, applied in filename order.
//
//go:embed migrations/*.sql
var Migrations embed.FS
