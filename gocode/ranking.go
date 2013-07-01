package gocode

import (
	"github.com/daviddengcn/go-villa"
	"math"
	"strings"
)

// TODO: author's authority

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	
	return b
}

func scoreOfPkgByProject(n int, sameProj bool) float64 {
	vl := 1. / math.Sqrt(float64(n)) // sqrt(n) / n
	if sameProj {
		vl *= 0.1
	}
	
	return vl
}

func scoreOfPkgByAuthor(n int, sameAuthor bool) float64 {
	vl := 1. / math.Sqrt(float64(n)) // sqrt(n) / n
	if sameAuthor {
		vl *= 0.5
	}
	
	return vl
}


func projectOfPackage(pkg string) string {
	parts := strings.Split(pkg, "/")
	if len(parts) == 0 {
		return ""
	}

	switch parts[0] {
	case "llamaslayers.net", "bazil.org":
		if len(parts) > 1 {
			return parts[1]
		}
	case "github.com", "code.google.com", "bitbucket.org", "labix.org":
		if len(parts) > 2 {
			return parts[2]
		}
	case "golanger.com":
		return "golangers"

	case "launchpad.net":
		if len(parts) > 2 && strings.HasPrefix(parts[1], "~") {
			return parts[2]
		}
		if len(parts) > 1 {
			return parts[1]
		}
	case "cgl.tideland.biz":
		return "tcgl"
	}
	return pkg
}

func calcStaticRank(doc *DocInfo) float64 {
	s := float64(1)

	author := doc.Author
	if author == "" {
		author = authorOfPackage(doc.Package)
	}

	project := projectOfPackage(doc.Package)

	authorCount := make(map[string]int)
	projectCount := make(map[string]int)
	for _, imp := range doc.ImportedPkgs {
		impProject := projectOfPackage(imp)
		projectCount[impProject] = projectCount[impProject] + 1

		impAuthor := authorOfPackage(imp)
		if impAuthor != "" {
			authorCount[impAuthor] = authorCount[impAuthor] + 1
		}
	}

	for _, imp := range doc.ImportedPkgs {
		impProject := projectOfPackage(imp)
		
		vl := scoreOfPkgByProject(projectCount[impProject], impProject == project)

		impAuthor := authorOfPackage(imp)
		if impAuthor != "" {
			vl = minFloat(vl, scoreOfPkgByAuthor(authorCount[impAuthor], impAuthor == author))
		}

		s += vl
	}

	desc := strings.TrimSpace(doc.Description)
	if len(desc) > 0 {
		s += 1
		if len(desc) > 100 {
			s += 0.5
		}

		if strings.HasPrefix(desc, "Package "+doc.Name) {
			s += 0.5
		} else if strings.HasPrefix(desc, "package "+doc.Name) {
			s += 0.4
		}
	}
	
	if doc.Name != "" && doc.Name != "main" {
		s += 0.1
	}

	starCount := doc.StarCount - 3
	if starCount < 0 {
		starCount = 0
	}
	s += math.Sqrt(float64(starCount)) * 0.5

	return s
}

func calcMatchScore(doc *DocInfo, tokens villa.StrSet) float64 {
	if len(tokens) == 0 {
		return 1.
	}

	s := float64(0.02 * float64(len(tokens)))

	filteredSyn := filterURLs(doc.Synopsis)
	synopsis := strings.ToLower(filteredSyn)
	synTokens := appendTokens(nil, filteredSyn)
	name := strings.ToLower(doc.Name)
	nameTokens := appendTokens(nil, name)
	pkg := strings.ToLower(doc.Package)
	pkgTokens := appendTokens(nil, doc.Package)

	for token := range tokens {
		if matchToken(token, synopsis, synTokens) {
			s += 0.25
		}

		if matchToken(token, name, nameTokens) {
			s += 0.4
		}

		if matchToken(token, pkg, pkgTokens) {
			s += 0.1
		}
	}

	return s
}
