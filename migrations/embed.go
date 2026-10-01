// Package migrations embeds the SQL migration files into the binary so the service can
// migrate itself on start-up without a separate tool in the container.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
