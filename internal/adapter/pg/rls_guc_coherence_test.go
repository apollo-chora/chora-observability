// rls_guc_coherence_test.go — the STRUCTURAL GUARD for the CHO-2140 defect
// class: a repository whose transaction seam SETs one Postgres GUC while the
// RLS policy on the table it writes READs a different one. The write is then
// evaluated against NULL and Postgres refuses it with 42501 — on the FIRST real
// insert that table ever attempts.
//
// What went wrong
// ---------------
// migrations/0015_rls_guc_rekey re-keyed 5 policies (4 familiar-growth from
// 0007 + ritual_run_audit from 0013) from the legacy `app.current_tenant_id` to
// the canonical `chora.tenant_id`. The Go seam that SET the legacy GUC
// (pg.WithAppTenantTx) was never updated, so those 5 tables became unwritable.
//
// It stayed invisible for weeks because:
//   - the familiar-growth lane's ingress was independently broken (CHO-2257), so
//     those 4 tables never attempted a write — 0 rows read as "no traffic yet",
//     not as "cannot write";
//   - ritual_run_audit held 3 rows written BEFORE the re-key, so it looked alive;
//   - WithAppTenantTx's godoc asserted those tables keyed on the legacy GUC —
//     true when written, false after 0015 — and nothing compared the claim to
//     the migrations.
//
// Why this guard is PER-TABLE and not global
// ------------------------------------------
// A global "is this GUC read by SOME policy?" check passes falsely: migration
// 0004's analytics policies still read the legacy GUC, which would satisfy a
// global check while the familiar-growth tables stayed broken. (Those analytics
// tables do not exist in the live DB and no pg repository writes them — they are
// inert, which is exactly why they must not be allowed to vouch for a GUC.)
// The invariant that actually matters is per-table:
//
//	for every table a repository WRITES, the GUC that repository's tx seam SETs
//	must be one the table's own RLS policy READs.
//
// Both sides are DERIVED — the seam→GUC map is parsed out of runtime.go, the
// table→GUC map out of migrations/ in lex order (a later DROP+CREATE overrides
// an earlier one, exactly as the runner applies them), and the repo→table map
// out of each repository's own SQL. Nothing here is a hand-maintained list, so
// it cannot drift the way the godoc did. This test would have gone red the
// moment 0015 landed.
package pg_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var (
	createPolicyRe   = regexp.MustCompile(`(?is)CREATE\s+POLICY\s+([a-z0-9_]+)\s+ON\s+([a-z0-9_.]+)(.*?);`)
	currentSettingRe = regexp.MustCompile(`(?i)current_setting\(\s*'([a-z0-9_.]+)'`)
	setLocalRe       = regexp.MustCompile(`SET\s+LOCAL\s+([a-z0-9_.]+)\s*=`)
	seamFuncRe       = regexp.MustCompile(`func\s+\(q\s+\*PgxPoolQuerier\)\s+(With[A-Za-z]*Tx)\(`)
	seamCallRe       = regexp.MustCompile(`\.(With[A-Za-z]*Tx)\(`)
	writeStmtRe      = regexp.MustCompile(`(?i)(?:INSERT\s+INTO|UPDATE)\s+([a-z0-9_]+)`)
	// unsetPermissiveRe matches the disjunct a policy uses to ALLOW the unset
	// case: `current_setting('<guc>', TRUE) IS NULL`. outbox_events (0006) is the
	// live example: it is deliberately permissive when app.current_tenant is
	// unset so the dispatcher (and any same-transaction producer under another
	// seam) can write it.
	unsetPermissiveRe = regexp.MustCompile(`(?i)current_setting\(\s*'([a-z0-9_.]+)'\s*,\s*TRUE\s*\)\s+IS\s+NULL`)
)

// permissiveWhenUnsetGUCs parses migrations in lex order → table → the GUCs
// whose policy explicitly permits the UNSET case. A seam that sets a DIFFERENT
// GUC leaves such a policy evaluating its own GUC as NULL, which the policy
// allows by construction, so the write cannot 42501: the guard must not flag it
// (the companion-suspension repository's same-transaction audit publish into
// outbox_events under chora.tenant_id relies on exactly this, as does the model
// gateway's egress-audit insert into the same table).
func permissiveWhenUnsetGUCs(t *testing.T) map[string]map[string]bool {
	t.Helper()
	dir := filepath.Join("..", "..", "..", "migrations")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	names := []string{}
	for _, e := range entries {
		if n := e.Name(); strings.HasSuffix(n, ".sql") && !strings.Contains(n, ".down.") {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	perPolicy := map[string][]string{}
	policyTable := map[string]string{}
	for _, n := range names {
		b, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			t.Fatalf("read %s: %v", n, err)
		}
		for _, m := range createPolicyRe.FindAllStringSubmatch(string(b), -1) {
			policy, table, body := m[1], m[2], m[3]
			gucs := []string{}
			for _, g := range unsetPermissiveRe.FindAllStringSubmatch(body, -1) {
				gucs = append(gucs, g[1])
			}
			key := table + "." + policy
			perPolicy[key] = gucs // last definition wins (a re-keyed policy may drop the disjunct)
			policyTable[key] = table
		}
	}
	out := map[string]map[string]bool{}
	for key, gucs := range perPolicy {
		tbl := policyTable[key]
		if out[tbl] == nil {
			out[tbl] = map[string]bool{}
		}
		for _, g := range gucs {
			out[tbl][g] = true
		}
	}
	return out
}

// tableGUCs parses migrations in lex order → table → set of GUCs its policies
// read. Later policy definitions replace earlier ones for the same policy name.
func tableGUCs(t *testing.T) map[string]map[string]bool {
	t.Helper()
	dir := filepath.Join("..", "..", "..", "migrations")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	names := []string{}
	for _, e := range entries {
		if n := e.Name(); strings.HasSuffix(n, ".sql") && !strings.Contains(n, ".down.") {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		t.Fatal("no migrations found — the guard would vacuously pass")
	}

	// (table.policy) → gucs, last definition wins.
	perPolicy := map[string][]string{}
	policyTable := map[string]string{}
	for _, n := range names {
		b, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			t.Fatalf("read %s: %v", n, err)
		}
		for _, m := range createPolicyRe.FindAllStringSubmatch(string(b), -1) {
			policy, table, body := m[1], m[2], m[3]
			gucs := []string{}
			for _, g := range currentSettingRe.FindAllStringSubmatch(body, -1) {
				gucs = append(gucs, g[1])
			}
			key := table + "." + policy
			perPolicy[key] = gucs
			policyTable[key] = table
		}
	}

	out := map[string]map[string]bool{}
	for key, gucs := range perPolicy {
		tbl := policyTable[key]
		if out[tbl] == nil {
			out[tbl] = map[string]bool{}
		}
		for _, g := range gucs {
			out[tbl][g] = true
		}
	}
	return out
}

// seamGUCs parses runtime.go → tx-seam method name → the GUC it SET LOCALs.
func seamGUCs(t *testing.T) map[string]string {
	t.Helper()
	b, err := os.ReadFile("runtime.go")
	if err != nil {
		t.Fatalf("read runtime.go: %v", err)
	}
	src := string(b)
	locs := seamFuncRe.FindAllStringSubmatchIndex(src, -1)
	if len(locs) == 0 {
		t.Fatal("no *PgxPoolQuerier With...Tx seam found in runtime.go — the guard would vacuously pass")
	}
	out := map[string]string{}
	for i, loc := range locs {
		name := src[loc[2]:loc[3]]
		end := len(src)
		if i+1 < len(locs) {
			end = locs[i+1][0]
		}
		body := src[loc[1]:end]
		if m := setLocalRe.FindStringSubmatch(body); m != nil {
			out[name] = m[1]
		}
	}
	return out
}

// repoWrites parses each repository → (tx seams it calls, tables it writes).
func repoWrites(t *testing.T) map[string]struct {
	Seams  []string
	Tables []string
} {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read pg dir: %v", err)
	}
	out := map[string]struct {
		Seams  []string
		Tables []string
	}{}
	for _, e := range entries {
		n := e.Name()
		if !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") || n == "runtime.go" {
			continue
		}
		b, err := os.ReadFile(n)
		if err != nil {
			t.Fatalf("read %s: %v", n, err)
		}
		src := string(b)
		seams := map[string]bool{}
		for _, m := range seamCallRe.FindAllStringSubmatch(src, -1) {
			seams[m[1]] = true
		}
		tables := map[string]bool{}
		for _, m := range writeStmtRe.FindAllStringSubmatch(src, -1) {
			tables[strings.ToLower(m[1])] = true
		}
		if len(seams) == 0 || len(tables) == 0 {
			continue
		}
		e := out[n]
		for s := range seams {
			e.Seams = append(e.Seams, s)
		}
		for tb := range tables {
			e.Tables = append(e.Tables, tb)
		}
		sort.Strings(e.Seams)
		sort.Strings(e.Tables)
		out[n] = e
	}
	return out
}

// liveGUCs returns the GUCs read by a policy on a table that SOME adapter in
// this service actually writes.
//
// Scoped service-wide (internal/adapter/**), not to this package: outbox_events
// is written by internal/adapter/outbox and its policy reads `app.current_tenant`
// (deliberately permissive when unset so the dispatcher can run). A pg-only scan
// would wrongly classify that GUC as retired. The distinction that matters is
// "does a policy that governs a table someone writes read this GUC?" — a policy
// on a table nobody writes (migration 0004's analytics tables, which do not even
// exist live) must never vouch for a GUC.
func liveGUCs(t *testing.T) map[string]bool {
	t.Helper()
	policies := tableGUCs(t)
	written := map[string]bool{}
	root := filepath.Join("..")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == "vendor" || n == ".git" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		for _, m := range writeStmtRe.FindAllStringSubmatch(string(b), -1) {
			written[strings.ToLower(m[1])] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk adapters: %v", err)
	}
	if len(written) == 0 {
		t.Fatal("no written tables found across internal/adapter — the guard would be vacuous")
	}
	live := map[string]bool{}
	for tbl := range written {
		for g := range policies[tbl] {
			live[g] = true
		}
	}
	return live
}

// TestRLSGucCoherence_RepoSeamGucMatchesEveryTableItWrites is the guard.
func TestRLSGucCoherence_RepoSeamGucMatchesEveryTableItWrites(t *testing.T) {
	policies := tableGUCs(t)
	seams := seamGUCs(t)
	repos := repoWrites(t)
	permissive := permissiveWhenUnsetGUCs(t)

	if len(policies) == 0 || len(seams) == 0 || len(repos) == 0 {
		t.Fatalf("guard is vacuous: policies=%d seams=%d repos=%d", len(policies), len(seams), len(repos))
	}

	checked := 0
	for file, rw := range repos {
		// The GUCs this repo can possibly have set on its transaction.
		setGUCs := map[string]bool{}
		for _, s := range rw.Seams {
			if g, ok := seams[s]; ok {
				setGUCs[g] = true
			}
		}
		if len(setGUCs) == 0 {
			continue // repo calls no GUC-setting seam (e.g. a bare-pool writer)
		}
		for _, tbl := range rw.Tables {
			readGUCs, hasPolicy := policies[tbl]
			if !hasPolicy {
				continue // no RLS policy on this table ⇒ no GUC constraint
			}
			checked++
			ok := false
			for g := range setGUCs {
				if readGUCs[g] {
					ok = true
					break
				}
			}
			if !ok {
				// A policy that explicitly permits ITS GUC being unset cannot refuse a
				// write made under a seam that sets a different GUC: the policy sees
				// NULL and allows by construction (outbox_events, 0006). Record it,
				// do not flag it.
				allUnsetPermissive := len(readGUCs) > 0
				for g := range readGUCs {
					if !permissive[tbl][g] {
						allUnsetPermissive = false
					}
				}
				if allUnsetPermissive {
					t.Logf("%s writes %q via seam(s) %v (SET %v); the table's policy permits its GUC(s) %v being unset, so the write is allowed by construction",
						file, tbl, rw.Seams, keys(setGUCs), keys(readGUCs))
					continue
				}
				t.Errorf(
					"%s writes %q via seam(s) %v which SET %v,\n"+
						"  but %q's RLS policy READs %v.\n"+
						"  The policy evaluates against NULL ⇒ Postgres refuses the write with 42501 on the first real insert.\n"+
						"  (CHO-2140 class: 0015_rls_guc_rekey re-keyed the policy and the Go seam was left behind.)",
					file, tbl, rw.Seams, keys(setGUCs), tbl, keys(readGUCs),
				)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no (repo, RLS table) pair was checked — the guard would vacuously pass")
	}
	t.Logf("guard checked %d (repository, RLS-protected table) pairs", checked)
}

// TestRLSGucCoherence_NoSeamSetsAGucNoPolicyAnywhereReads catches a seam that is
// dead weight platform-wide: it sets a GUC that no policy on any table a
// repository writes reads. Kept separate from the per-table guard so a dead seam
// is named even before someone wires a repository onto it.
func TestRLSGucCoherence_NoSeamSetsAGucNoPolicyAnywhereReads(t *testing.T) {
	seams := seamGUCs(t)
	repos := repoWrites(t)
	live := liveGUCs(t)
	if len(live) == 0 {
		t.Fatal("no live policy GUCs resolved — the guard would vacuously pass")
	}

	used := map[string]bool{}
	for _, rw := range repos {
		for _, s := range rw.Seams {
			used[s] = true
		}
	}
	for seam, guc := range seams {
		if !used[seam] {
			continue // an unused seam is dead code, not a live 42501 writer
		}
		if !live[guc] {
			t.Errorf(
				"seam %s SETs GUC %q, which NO policy on any table a repository writes READs.\n"+
					"  Every write through it is refused with 42501. GUCs that are actually read: %v",
				seam, guc, keys(live),
			)
		}
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestRLSGucCoherence_NoStringLiteralNamesARetiredGuc extends the guard to the
// artifact class that outlived the code: a STRING that names a GUC the binary no
// longer sets.
//
// After the seam was fixed, cmd/server still booted with
//
//	"pgx FamiliarGrowthRepository wired (... RLS guc=app.current_tenant_id)"
//
// The code was correct and the log LIED. That is worse than a stale comment: the
// boot log is precisely what an operator greps to check this seam, the container
// is distroless so the binary cannot be inspected, and the reviewer nearly
// re-opened a closed diagnosis over it. A log line asserting a GUC name is a
// checkable claim, so it is checked here.
//
// "Retired" is DERIVED, not hand-listed: a GUC that some policy in migrations/
// reads, but that no policy on any table a repository writes reads. That is
// exactly `app.current_tenant_id` today — kept alive only by 0004's analytics
// policies, whose tables no repository writes (and which do not exist live).
//
// Scoped to string LITERALS via go/ast, never comments: the retirement history
// is deliberately recorded in prose across runtime.go and the repositories, and
// that documentation is worth keeping. Only claims the program can emit are
// checked.
func TestRLSGucCoherence_NoStringLiteralNamesARetiredGuc(t *testing.T) {
	policies := tableGUCs(t)
	live := liveGUCs(t)

	// Every GUC any policy mentions, minus the live ones ⇒ retired.
	retired := map[string]bool{}
	for _, gucs := range policies {
		for g := range gucs {
			if !live[g] {
				retired[g] = true
			}
		}
	}
	if len(retired) == 0 {
		t.Skip("no retired GUC in migrations — nothing for this guard to protect against")
	}

	root := filepath.Join("..", "..", "..")
	fset := token.NewFileSet()
	scanned := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == "vendor" || n == "migrations" || n == ".git" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0) // 0 ⇒ comments NOT parsed
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		scanned++
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			for guc := range retired {
				if strings.Contains(lit.Value, guc) {
					t.Errorf(
						"%s: string literal names the RETIRED GUC %q.\n"+
							"  No policy on any table a repository writes reads it, and no seam sets it — so this string is a false claim to whoever reads it.\n"+
							"  Literal: %s\n"+
							"  (CHO-2140: the boot log kept naming this GUC after the seam was fixed, making a working seam look broken.)",
						fset.Position(lit.Pos()), guc, truncLit(lit.Value),
					)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if scanned == 0 {
		t.Fatal("scanned no Go files — the guard would vacuously pass")
	}
	t.Logf("guard scanned %d Go files for literals naming retired GUC(s) %v", scanned, keys(retired))
}

func truncLit(s string) string {
	if len(s) <= 120 {
		return s
	}
	return s[:120] + `…"`
}
