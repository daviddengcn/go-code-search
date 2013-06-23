package gocode

import (
	"bytes"
	"io"
)

type RuneType int

const (
	TokenSep   RuneType = iota // token breaker, should ignored
	TokenStart                 // start of a new token
	TokenBody                  // body of a token. It's ok for the first rune to be a TokenBody
)

/*
	Tokenize a rune sequence to out channel with a RuneType function.
	Testcase: last token is missed
*/
func Tokenize(runeType func(last, current rune) RuneType,
	in io.RuneReader, out func(token string)) {
	last := rune(0)
	var outBuf bytes.Buffer
	for {
		current, _, err := in.ReadRune()
		if err != nil {
			break
		}
		tp := runeType(last, current)
		if tp == TokenStart || tp == TokenSep {
			// finish current
			if outBuf.Len() > 0 {
				out(outBuf.String())
				outBuf.Reset()
			}
		}

		if tp == TokenStart || tp == TokenBody {
			outBuf.WriteRune(current)
		}
		last = current
	}

	// finish last, if any
	if outBuf.Len() > 0 {
		out(outBuf.String())
	}
	return
}
