package glob

import "strings"

// The character which is treated like a glob
const GLOB = "*"

type GlobMatches struct
{
	Matches []string
}

func (self *GlobMatches) append(item string){
	self.Matches = append(self.Matches, item)
}

// Glob will test a string pattern, potentially containing globs, against a
// subject string. The result is a simple true/false, determining whether or
// not the glob pattern matched the subject text.
func Glob(pattern, subj string, matches *GlobMatches) bool {
	// Empty pattern can only match empty subject
	if pattern == "" {
		return subj == pattern
	}

	// If the pattern _is_ a glob, it matches everything
	if pattern == GLOB {

		matches.append(subj)

		return true
	}

	parts := strings.Split(pattern, GLOB)

	if len(parts) == 1 {
		// No globs in pattern, so test for equality
		return subj == pattern
	}

	leadingGlob := strings.HasPrefix(pattern, GLOB)
	trailingGlob := strings.HasSuffix(pattern, GLOB)
	end := len(parts) - 1

	matchBuf := &GlobMatches{}

	for i, part := range parts {
		switch i {
		case 0:
			if leadingGlob {
				continue
			}
			if !strings.HasPrefix(subj, part) {
				return false
			}
		case end:
			if len(subj) > 0 {

				if trailingGlob {
					matchBuf.append(subj)
				} else if strings.HasSuffix(subj, part){
					matchBuf.append(subj[0:strings.Index(subj, part)])
				}else{
					return false
				}
			}
		default:
			if !strings.Contains(subj, part) {
				return false
			}

			matchBuf.append(subj[:strings.Index(subj, part)])
		}

		// Trim evaluated text from subj as we loop over the pattern.
		idx := strings.Index(subj, part) + len(part)

		subj = subj[idx:]
	}

	matches.Matches = matchBuf.Matches

	// All parts of the pattern matched
	return true
}
