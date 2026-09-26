package emul

import (
	"errors"
	"testing"
)

// A reader that hits rows of a prepared-but-unresolved 2PC transaction gets
// "record from transaction N is stuck in limbo" from the engine. This is a
// normal, transient event when the completion axis draws limbo completions:
// the recovery sidecar resolves the transaction within seconds, so the unit
// must classify as a conflict (rollback, retry, no worker impact), not a
// failure. Seen live on FB4: without this classification a single limbo
// window failed hundreds of units as "unexpected".
func TestClassifyUnitErrorStuckInLimbo(t *testing.T) {
	err := errors.New("record from transaction 4364 is stuck in limbo At procedure 'SP_GET_TEST_TIME_DTS' line: 19, col: 9 At procedure 'SP_CHECK_TO_STOP_WORK' line: 19, col: 5")
	if got := classifyUnitError(err); got != OutcomeConflict {
		t.Fatalf("stuck-in-limbo classified as %v, want conflict", got)
	}
}
