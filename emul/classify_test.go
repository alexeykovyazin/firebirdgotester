package emul

import (
	"errors"
	"fmt"
	"testing"
)

func TestContainsExceptionName(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"exception 12 EX_NO_ROWS_IN_SHOPPING_CART", true},
		{"unit sp_order: rejected: EX_STOCK_IS_EMPTY", true},
		{"violation of CONSTRAINT INTEG_15 on index RDB$INDEX_15", false},
		{"unsuccessful metadata update; index INDEX_PREFIX_x missing", false},
		{"syntax error", false},
		{"", false},
	}
	for _, c := range cases {
		if got := containsExceptionName(c.msg); got != c.want {
			t.Errorf("containsExceptionName(%q) = %v, want %v", c.msg, got, c.want)
		}
	}
}

func TestClassifyUnitErrorIndexNotRejected(t *testing.T) {
	// An index-name-bearing constraint failure must classify as Failure,
	// not as a business Rejection (workers swallow rejections as expected).
	err := errors.New("violation of CONSTRAINT INTEG_15 on index RDB$INDEX_15")
	if got := classifyUnitError(err); got != OutcomeFailure {
		t.Errorf("classifyUnitError(index message) = %v, want %v", got, OutcomeFailure)
	}
	err = fmt.Errorf("unit sp_pay: rejected: EX_NO_MONEY: balance is negative")
	if got := classifyUnitError(err); got != OutcomeRejected {
		t.Errorf("classifyUnitError(EX_ message) = %v, want %v", got, OutcomeRejected)
	}
}
