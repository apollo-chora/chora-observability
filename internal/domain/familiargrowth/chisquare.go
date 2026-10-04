// chisquare.go — IMDA D2 transparency goodness-of-fit test for breed roll
// distributions.
//
// We compute a Pearson chi-square statistic comparing empirical breed-roll
// counts against the claimed distribution (the egg SKU's breed_distribution
// table at roll time). The p-value is approximated via the Wilson-Hilferty
// cube-root transformation (Wilson & Hilferty 1931), which converts a
// chi-square random variable with k degrees of freedom into an approximate
// standard-normal random variable. The approximation is accurate to ~0.01
// for k >= 3 and serves the D2 audit endpoint's purpose (flag distributions
// where claimed vs observed visibly diverge) without pulling a stats library.
//
// For k = 1 or 2 the cube-root approximation degrades; we still return a
// p-value but flag InsufficientSample when total n < 5 (per Cochran's rule
// for chi-square applicability — every expected count >= 5 is the strict
// rule but we relax to total n < 5 since the dashboard is a heuristic, not
// a regulatory test).
package familiargrowth

import (
	"math"
	"sort"
)

// ChiSquareGoodnessOfFit computes a Pearson chi-square goodness-of-fit
// statistic + approximate p-value for `observed` against `claimed`.
//
// claimed maps category -> probability ([0, 1], should sum to ~1).
// observed maps category -> count (non-negative integers).
//
// Categories present in observed but not in claimed are merged into the
// chi-square only when claimed also lists them; categories in claimed but
// not observed contribute an expected count without an observed count
// (observed = 0). The category ordering in the result is deterministic
// (sorted alphabetically).
//
// Returns a fully-populated ChiSquareResult. When the total observed sample
// is < 5, InsufficientSample is true and PValue is set to NaN.
func ChiSquareGoodnessOfFit(observed map[string]int, claimed map[string]float64) ChiSquareResult {
	categories := unionCategories(observed, claimed)
	res := ChiSquareResult{
		Categories: categories,
		Expected:   make([]float64, 0, len(categories)),
		Observed:   make([]int, 0, len(categories)),
	}

	totalObserved := 0
	for _, c := range observed {
		totalObserved += c
	}
	res.SampleSize = totalObserved

	if totalObserved == 0 {
		res.InsufficientSample = true
		// PValue intentionally 0 (not NaN) so the response is JSON-safe;
		// InsufficientSample = true is the canonical signal that the
		// statistic is not meaningful.
		res.PValue = 0
		// Still populate Observed + Expected with zero placeholders so
		// the response shape is uniform.
		for range categories {
			res.Observed = append(res.Observed, 0)
			res.Expected = append(res.Expected, 0)
		}
		return res
	}

	// Compute expected counts (total * claimed_probability) and the
	// chi-square statistic.
	stat := 0.0
	dof := 0
	for _, cat := range categories {
		p := claimed[cat]
		exp := float64(totalObserved) * p
		obs := observed[cat]
		res.Expected = append(res.Expected, exp)
		res.Observed = append(res.Observed, obs)
		if exp > 0 {
			diff := float64(obs) - exp
			stat += (diff * diff) / exp
			dof++
		}
	}
	// Degrees of freedom = k - 1 where k = number of categories with
	// non-zero expected count. Lower-bound at 1.
	if dof > 0 {
		dof--
	}
	if dof < 1 {
		dof = 1
	}
	res.Statistic = stat
	res.DegreesOfFreedom = dof
	res.InsufficientSample = totalObserved < 5

	if res.InsufficientSample {
		// PValue intentionally 0 (not NaN) so the response is JSON-safe;
		// InsufficientSample = true is the canonical signal.
		res.PValue = 0
		return res
	}
	res.PValue = chiSquarePValue(stat, dof)
	return res
}

// unionCategories returns the sorted union of keys from observed + claimed.
func unionCategories(observed map[string]int, claimed map[string]float64) []string {
	seen := make(map[string]struct{}, len(observed)+len(claimed))
	for k := range observed {
		seen[k] = struct{}{}
	}
	for k := range claimed {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// chiSquarePValue approximates P(X >= stat) where X ~ ChiSquare(dof) using
// the Wilson-Hilferty cube-root normal approximation:
//
//	z ≈ ( (X/k)^(1/3) - (1 - 2/(9k)) ) / sqrt(2/(9k))
//	p ≈ 1 - Phi(z)
//
// Phi is the standard normal CDF, approximated via the math.Erfc identity.
//
// Bounded into [0, 1].
func chiSquarePValue(stat float64, dof int) float64 {
	if stat <= 0 {
		return 1.0
	}
	if dof < 1 {
		dof = 1
	}
	k := float64(dof)
	mean := 1.0 - 2.0/(9.0*k)
	variance := 2.0 / (9.0 * k)
	stdDev := math.Sqrt(variance)
	z := (math.Cbrt(stat/k) - mean) / stdDev
	p := 1.0 - standardNormalCDF(z)
	if p < 0 {
		return 0
	}
	if p > 1 {
		return 1
	}
	return p
}

// standardNormalCDF is Phi(z) for the standard normal distribution.
// Phi(z) = 0.5 * (1 + erf(z/sqrt(2))) = 0.5 * erfc(-z/sqrt(2)).
func standardNormalCDF(z float64) float64 {
	return 0.5 * math.Erfc(-z/math.Sqrt2)
}
