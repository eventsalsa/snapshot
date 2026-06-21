package migrations

import "embed"

// FS embeds the SQL migration files for the postgres snapshot store.
//
//go:embed *.sql
var FS embed.FS
