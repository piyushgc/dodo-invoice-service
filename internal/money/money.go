// Package money holds the only arithmetic done on amounts. Amounts are int64 USD cents;
// float64 never appears in the money path. Every operation reports overflow instead of
// silently wrapping.
package money

import "math"

// MaxInvoiceTotalCents caps a single invoice at $99,999,999.99. It keeps totals far away
// from int64 overflow and rejects obviously wrong input (e.g. quantity typed into price).
const MaxInvoiceTotalCents int64 = 99_999_999_99

// Mul returns a*b, or ok=false if the result would overflow int64. Inputs must be >= 0.
func Mul(a, b int64) (int64, bool) {
	if a < 0 || b < 0 {
		return 0, false
	}
	if a != 0 && b > math.MaxInt64/a {
		return 0, false
	}
	return a * b, true
}

// Add returns a+b, or ok=false if the result would overflow int64. Inputs must be >= 0.
func Add(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}
