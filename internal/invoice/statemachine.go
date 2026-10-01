package invoice

import (
	"net/http"
	"sort"

	"invoicesvc/internal/httpx"
)

// Status is an invoice state.
//
//	create ──► open ──── payment_succeeded ────► paid (terminal)
//	            │  └──── mark_uncollectible ───► uncollectible ── payment_succeeded ──► paid
//	            └─────── void ─────────────────► void (terminal) ◄── void ── uncollectible
//
// A failed payment attempt is not a transition: the invoice stays open (or uncollectible)
// and can be paid again. See DESIGN.md §2 for the reasoning.
type Status string

const (
	StatusOpen          Status = "open"
	StatusPaid          Status = "paid"
	StatusVoid          Status = "void"
	StatusUncollectible Status = "uncollectible"
)

// Valid reports whether s is a known status (used to validate ?status= filters).
func (s Status) Valid() bool {
	switch s {
	case StatusOpen, StatusPaid, StatusVoid, StatusUncollectible:
		return true
	}
	return false
}

// Terminal reports whether no further transitions are possible.
func (s Status) Terminal() bool {
	return len(transitions[s]) == 0
}

// Action is something that asks the invoice to change state.
type Action string

const (
	ActionPaymentSucceeded  Action = "payment_succeeded"  // system: PSP confirmed a charge
	ActionVoid              Action = "void"               // business: POST /invoices/{id}/void
	ActionMarkUncollectible Action = "mark_uncollectible" // business: POST /invoices/{id}/mark-uncollectible
)

// transitions is the whole state machine. Anything not listed here is rejected.
var transitions = map[Status]map[Action]Status{
	StatusOpen: {
		ActionPaymentSucceeded:  StatusPaid,
		ActionVoid:              StatusVoid,
		ActionMarkUncollectible: StatusUncollectible,
	},
	StatusUncollectible: {
		ActionPaymentSucceeded: StatusPaid,
		ActionVoid:             StatusVoid,
	},
	StatusPaid: {},
	StatusVoid: {},
}

// Transition returns the state that action leads to from `from`, or a 409
// invalid_state_transition error that tells the caller what is allowed instead.
func Transition(from Status, action Action) (Status, error) {
	if to, ok := transitions[from][action]; ok {
		return to, nil
	}
	allowed := []string{}
	for a := range transitions[from] {
		allowed = append(allowed, string(a))
	}
	sort.Strings(allowed)
	return "", httpx.NewError(http.StatusConflict, "invalid_state_transition",
		"cannot "+string(action)+" an invoice that is "+string(from)).
		WithDetails(map[string]any{"current_status": from, "action": action, "allowed_actions": allowed})
}

// AcceptsPayment reports whether a payment may be attempted in status s.
func AcceptsPayment(s Status) bool {
	_, ok := transitions[s][ActionPaymentSucceeded]
	return ok
}
