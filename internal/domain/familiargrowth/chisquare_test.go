package familiargrowth_test

import (
	"testing"

	fg "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/familiargrowth"
)

func TestChiSquare_PerfectFit(t *testing.T) {
	t.Parallel()
	// 1000 rolls, 4 categories, claimed = 25/25/25/25, observed matches
	// exactly. Statistic should be 0; p-value should be 1.
	observed := map[string]int{
		"common":    250,
		"uncommon":  250,
		"rare":      250,
		"legendary": 250,
	}
	claimed := map[string]float64{
		"common":    0.25,
		"uncommon":  0.25,
		"rare":      0.25,
		"legendary": 0.25,
	}
	r := fg.ChiSquareGoodnessOfFit(observed, claimed)
	if r.Statistic != 0 {
		t.Fatalf("expected stat 0; got %v", r.Statistic)
	}
	if r.PValue < 0.99 {
		t.Fatalf("expected p-value ~1; got %v", r.PValue)
	}
	if r.SampleSize != 1000 {
		t.Fatalf("expected n=1000; got %d", r.SampleSize)
	}
	if r.DegreesOfFreedom != 3 {
		t.Fatalf("expected dof=3; got %d", r.DegreesOfFreedom)
	}
}

func TestChiSquare_KnownDivergent(t *testing.T) {
	t.Parallel()
	// Classic textbook fixture: 60 trials, 4 categories, expected
	// distribution 9:3:3:1 (Mendelian dihybrid cross).
	// Expected counts:  33.75, 11.25, 11.25, 3.75
	// Observed counts:  35, 10, 13, 2
	// Chi-square ≈ ( (35-33.75)^2/33.75 + (10-11.25)^2/11.25 +
	//              (13-11.25)^2/11.25 + (2-3.75)^2/3.75 ) ≈ 1.273
	// dof = 3; p-value ≈ 0.736 (rough agreement).
	observed := map[string]int{
		"A": 35,
		"B": 10,
		"C": 13,
		"D": 2,
	}
	claimed := map[string]float64{
		"A": 9.0 / 16.0,
		"B": 3.0 / 16.0,
		"C": 3.0 / 16.0,
		"D": 1.0 / 16.0,
	}
	r := fg.ChiSquareGoodnessOfFit(observed, claimed)
	if r.Statistic < 1.0 || r.Statistic > 1.6 {
		t.Fatalf("statistic should be ~1.273; got %v", r.Statistic)
	}
	if r.DegreesOfFreedom != 3 {
		t.Fatalf("expected dof=3; got %d", r.DegreesOfFreedom)
	}
	if r.PValue < 0.5 || r.PValue > 0.9 {
		t.Fatalf("expected p-value ~0.74; got %v", r.PValue)
	}
}

func TestChiSquare_HighlyDivergent(t *testing.T) {
	t.Parallel()
	// 1000 trials, claimed 25/25/25/25, observed shifted to 900/50/30/20.
	// Chi-square should be very large; p-value ~ 0.
	observed := map[string]int{
		"common":    900,
		"uncommon":  50,
		"rare":      30,
		"legendary": 20,
	}
	claimed := map[string]float64{
		"common":    0.25,
		"uncommon":  0.25,
		"rare":      0.25,
		"legendary": 0.25,
	}
	r := fg.ChiSquareGoodnessOfFit(observed, claimed)
	if r.Statistic < 500 {
		t.Fatalf("expected very large stat; got %v", r.Statistic)
	}
	if r.PValue > 0.001 {
		t.Fatalf("expected p ~ 0; got %v", r.PValue)
	}
}

func TestChiSquare_InsufficientSample(t *testing.T) {
	t.Parallel()
	observed := map[string]int{
		"common":   2,
		"uncommon": 1,
	}
	claimed := map[string]float64{
		"common":   0.7,
		"uncommon": 0.3,
	}
	r := fg.ChiSquareGoodnessOfFit(observed, claimed)
	if !r.InsufficientSample {
		t.Fatalf("expected InsufficientSample = true")
	}
	// PValue is forced to 0 (not NaN) so the result is JSON-safe; the
	// InsufficientSample flag is the canonical signal that the test is not
	// meaningful.
	if r.PValue != 0 {
		t.Fatalf("expected PValue=0 when InsufficientSample; got %v", r.PValue)
	}
}

func TestChiSquare_EmptyObserved(t *testing.T) {
	t.Parallel()
	r := fg.ChiSquareGoodnessOfFit(map[string]int{}, map[string]float64{"A": 1.0})
	if r.SampleSize != 0 {
		t.Fatalf("expected n=0; got %d", r.SampleSize)
	}
	if !r.InsufficientSample {
		t.Fatalf("expected InsufficientSample on n=0")
	}
}

func TestChiSquare_CategoriesUnion(t *testing.T) {
	t.Parallel()
	// Observed has a category not in claimed (expected = 0 contribution
	// drops, but the category is still listed in result.Categories).
	observed := map[string]int{
		"alpha": 100,
		"beta":  50,
		"gamma": 0,
	}
	claimed := map[string]float64{
		"alpha": 0.6,
		"beta":  0.3,
		// "gamma" missing; "delta" present.
		"delta": 0.1,
	}
	r := fg.ChiSquareGoodnessOfFit(observed, claimed)
	// Categories should include alpha, beta, delta, gamma — sorted.
	expectCats := []string{"alpha", "beta", "delta", "gamma"}
	if len(r.Categories) != len(expectCats) {
		t.Fatalf("expected %d categories; got %d (%v)", len(expectCats), len(r.Categories), r.Categories)
	}
	for i, c := range expectCats {
		if r.Categories[i] != c {
			t.Fatalf("category[%d]: expected %q; got %q", i, c, r.Categories[i])
		}
	}
}
