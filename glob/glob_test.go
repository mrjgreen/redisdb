package glob

import "testing"
//import "reflect"

type matchExpectation struct{
	Pattern string
	Matches []string
}

func testGlobMatch(t *testing.T, pattern matchExpectation, subj string) {

	matches := &GlobMatches{}

	if !Glob(pattern.Pattern, subj, matches) {
		t.Fatalf("%s should match %s", pattern, subj)
	}

	println(pattern.Pattern)

	for i, match := range pattern.Matches{

		if len(matches.Matches) <= i{
			t.Fatalf("Matches not long enough. Match %d '%s' has no match in results %v", i, match, matches.Matches)
		}

		if match != matches.Matches[i]{
			t.Fatalf("Match %d '%s' not equal to '%s'", i, matches.Matches[i], match)
		}
	}
}

func testGlobNoMatch(t *testing.T, pattern , subj string) {

	matches := &GlobMatches{}

	if Glob(pattern, subj, matches) {
		t.Fatalf("%s should not match %s", pattern, subj)
	}

	if(len(matches.Matches) > 0){
		t.Fatalf("length of match buffer should be zero")
	}
}

func TestEmptyPattern(t *testing.T) {
	testGlobMatch(t, matchExpectation{"", []string{}}, "")
	testGlobNoMatch(t, "", "test")

}

func TestPatternWithoutGlobs(t *testing.T) {
	testGlobMatch(t, matchExpectation{"test", []string{}}, "test")  // []
}

func TestGlobMatch(t *testing.T) {
	for _, pattern := range []matchExpectation{
		matchExpectation{"*test", []string{"this is a "}}, // Leading glob
		matchExpectation{"this*", []string{" is a test"}}, // Trailing glob
		matchExpectation{"*is *", []string{"th","is a test"}}, // String in between two globs
		matchExpectation{"*is*a*", []string{"th"," is "," test"}},  // Lots of globs
		matchExpectation{"**test**", []string{"", "this is a ", ""}},  // Double glob characters
		matchExpectation{"**is**a***test*", []string{"", "th","", " is ",""}},  // Varying number of globs
	} {
		testGlobMatch(t, pattern, "this is a test")
	}
}

func TestGlobNoMatch(t *testing.T) {
	for _, pattern := range []string{
		"test*", // Implicit substring match should fail
		"*is", // Partial match should fail
		"*no*", // Globs without a match between them should fail
	} {
		testGlobNoMatch(t, pattern, "this is a test")
	}
}
