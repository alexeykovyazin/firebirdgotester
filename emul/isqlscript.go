package emul

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// StmtKind classifies a statement produced by ParseScript.
type StmtKind int

const (
	// StmtSQL is an ordinary SQL statement (DML, DDL, PSQL execute block, ...).
	StmtSQL StmtKind = iota
	// StmtCommit is a top-level COMMIT statement.
	StmtCommit
	// StmtRollback is a top-level ROLLBACK statement.
	StmtRollback
	// StmtExit is a standalone isql EXIT statement (commit semantics).
	StmtExit
	// StmtQuit is a standalone isql QUIT statement (rollback semantics).
	StmtQuit
	// StmtSkip is an isql-only command that must not be sent to the server
	// (SET BAIL/LIST/ECHO/..., IN/OUT/SHOW/SHELL, standalone EXIT/QUIT use
	// their own kinds instead).
	StmtSkip
)

// Statement is one executable unit extracted from an isql script.
type Statement struct {
	Kind StmtKind
	SQL  string // trimmed SQL text, terminator removed
	Line int    // 1-based line where the statement starts (for error messages)
}

var (
	reSetTerm = regexp.MustCompile(`(?is)^set\s+term\s+(.+)$`)
	reSet     = regexp.MustCompile(`(?is)^set\b(.*)$`)
	reIn      = regexp.MustCompile(`(?is)^(in|input|output|out)\b`)
	reShow    = regexp.MustCompile(`(?is)^(show|shell|help)\b`)
	reExit    = regexp.MustCompile(`(?is)^exit\b`)
	reQuit    = regexp.MustCompile(`(?is)^quit\b`)
	reCommit  = regexp.MustCompile(`(?is)^commit\b`)
	reRollbck = regexp.MustCompile(`(?is)^rollback\b`)
)

// ParseScript splits an isql-format script into statements. It tracks the
// current SET TERM terminator (scripts toggle between ';' and '^'), ignores
// terminators inside string literals, quoted identifiers and comments, and
// classifies isql-only commands as StmtSkip so the executor can drop them.
//
// The scripts this package vendors are terminated with ';' outside PSQL and
// '^' inside, switching via "set term ^;" / "^set term ;^" (note the leading
// terminator gluing the directive to the previous statement - the scanner
// handles that naturally because only the CURRENT terminator ends a chunk).
func ParseScript(script string) ([]Statement, error) {
	var stmts []Statement
	term := ";"
	line := 1
	start := 0     // start offset of current chunk (for reporting)
	chunkLine := 1 // line the current chunk starts on (tracked incrementally)

	i := 0
	n := len(script)
	for i < n {
		c := script[i]
		switch c {
		case '\n':
			line++
			i++
		case '-':
			if i+1 < n && script[i+1] == '-' {
				for i < n && script[i] != '\n' {
					i++
				}
			} else {
				i++
			}
		case '/':
			if i+1 < n && script[i+1] == '*' {
				end := strings.Index(script[i+2:], "*/")
				if end < 0 {
					return nil, fmt.Errorf("line %d: unterminated block comment", line)
				}
				line += strings.Count(script[i:i+2+end], "\n")
				i += 2 + end + 2
			} else {
				i++
			}
		case '\'':
			end, nl, ok := scanQuoted(script, i, '\'')
			if !ok {
				return nil, fmt.Errorf("line %d: unterminated string literal", line)
			}
			line += nl
			i = end
		case '"':
			end, nl, ok := scanQuoted(script, i, '"')
			if !ok {
				return nil, fmt.Errorf("line %d: unterminated quoted identifier", line)
			}
			line += nl
			i = end
		default:
			if strings.HasPrefix(script[i:], term) {
				chunk := script[start:i]
				stmt, isTermDirective, err := classify(chunk, chunkLine, term, &term)
				if err != nil {
					return nil, err
				}
				if !isTermDirective && stmt != nil {
					stmts = append(stmts, *stmt)
				}
				i += len(term)
				start = i
				chunkLine = line // next chunk starts on the terminator's line
			} else {
				i++
			}
		}
	}

	// Trailing chunk after the last terminator.
	if rest := strings.TrimSpace(script[start:]); rest != "" {
		stmt, isTerm, err := classify(script[start:], chunkLine, term, &term)
		if err != nil {
			return nil, err
		}
		if !isTerm && stmt != nil {
			stmts = append(stmts, *stmt)
		}
	}
	return stmts, nil
}

// classify decides what a chunk between terminators is. Leading comments are
// stripped before classification (a chunk frequently begins with a comment
// glued to the previous statement's terminator, e.g. "^ -- srv_test_work"),
// so keyword matching sees the actual statement. When the chunk is a SET TERM
// directive it updates *term and returns isTermDirective=true.
func classify(chunk string, lineNo int, curTerm string, term *string) (*Statement, bool, error) {
	text, err := stripLeadingComments(chunk)
	if err != nil {
		return nil, false, fmt.Errorf("line %d: %v", lineNo, err)
	}
	if text == "" {
		return nil, false, nil
	}
	if m := reSetTerm.FindStringSubmatch(text); m != nil {
		newTerm := strings.TrimSpace(m[1])
		// "set term ^" / "set term ;" - the new terminator is the last token.
		// It may still be glued to trailing text of the next statement in
		// exotic scripts; the vendored ones never do this, so treat any
		// remainder as an error rather than silently corrupting parsing.
		if strings.ContainsAny(newTerm, "\n") {
			return nil, false, fmt.Errorf("line %d: unsupported multi-line SET TERM directive: %q", lineNo, text)
		}
		*term = newTerm
		return nil, true, nil
	}
	switch {
	case reSet.MatchString(text):
		return &Statement{Kind: StmtSkip, SQL: text, Line: lineNo}, false, nil
	case reIn.MatchString(text):
		return &Statement{Kind: StmtSkip, SQL: text, Line: lineNo}, false, nil
	case reShow.MatchString(text):
		return &Statement{Kind: StmtSkip, SQL: text, Line: lineNo}, false, nil
	case reCommit.MatchString(text):
		// "commit" or "commit work" - keep as explicit commit marker.
		return &Statement{Kind: StmtCommit, SQL: text, Line: lineNo}, false, nil
	case reRollbck.MatchString(text):
		return &Statement{Kind: StmtRollback, SQL: text, Line: lineNo}, false, nil
	case reExit.MatchString(text):
		return &Statement{Kind: StmtExit, SQL: text, Line: lineNo}, false, nil
	case reQuit.MatchString(text):
		return &Statement{Kind: StmtQuit, SQL: text, Line: lineNo}, false, nil
	default:
		return &Statement{Kind: StmtSQL, SQL: text, Line: lineNo}, false, nil
	}
}

// stripLeadingComments removes whitespace and leading `--` line comments and
// `/* */` block comments from the start of a chunk, so that the first real
// token of the statement is at the front.
func stripLeadingComments(text string) (string, error) {
	for {
		text = strings.TrimLeft(text, " \t\r\n")
		switch {
		case strings.HasPrefix(text, "--"):
			nl := strings.IndexByte(text, '\n')
			if nl < 0 {
				return "", nil
			}
			text = text[nl+1:]
		case strings.HasPrefix(text, "/*"):
			end := strings.Index(text[2:], "*/")
			if end < 0 {
				return "", errors.New("unterminated block comment in statement chunk")
			}
			text = text[2+end+2:]
		default:
			return text, nil
		}
	}
}

// scanQuoted returns the offset just past a quote-delimited region opened at
// i (quote byte q), handling doubled quotes (” / "") as escapes, plus the
// number of newlines consumed.
func scanQuoted(s string, i int, q byte) (end int, newlines int, ok bool) {
	i++ // skip opening quote
	for i < len(s) {
		switch {
		case s[i] == q && i+1 < len(s) && s[i+1] == q:
			i += 2
		case s[i] == q:
			return i + 1, newlines, true
		case s[i] == '\n':
			newlines++
			i++
		default:
			i++
		}
	}
	return 0, newlines, false
}
