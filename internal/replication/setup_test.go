package replication

import "testing"

func TestPublicationDrifted(t *testing.T) {
	tests := []struct {
		name         string
		publishesAll bool
		want         []string
		published    []string
		drifted      bool
	}{
		{name: "all tables on both sides", publishesAll: true, published: []string{"public.a"}},
		{name: "same table list", want: []string{"public.a"}, published: []string{"public.a"}},
		{name: "different table list", want: []string{"public.a"}, published: []string{"public.b"}, drifted: true},
		{name: "config lists tables, publication is all tables", publishesAll: true, want: []string{"public.a"}, published: []string{"public.a"}, drifted: true},
		{name: "config wants all, publication is table scoped", published: []string{"public.a"}, drifted: true},
		{name: "config wants all, publication is table scoped and empty", drifted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := publicationDrifted(tt.publishesAll, tt.want, tt.published); got != tt.drifted {
				t.Fatalf("drifted = %v, want %v", got, tt.drifted)
			}
		})
	}
}

func TestQualified(t *testing.T) {
	got := qualified([]string{"users", "audit.log"})
	if len(got) != 2 || got[0] != "audit.log" || got[1] != "public.users" {
		t.Fatalf("got %v", got)
	}
}

func TestDecideSlot(t *testing.T) {
	tests := []struct {
		name      string
		exists    bool
		state     slotState
		requested int
		want      slotAction
	}{
		{"first start", false, slotState{}, 0, slotCreate},
		{"first start with a stray override", false, slotState{}, 1, slotCreate},
		{"normal restart", true, slotState{known: true}, 0, slotKeep},
		{"restart with an armed override keeps the slot", true, slotState{known: true}, 1, slotKeep},
		{"slot without a row is adopted", true, slotState{}, 0, slotAdopt},
		{"lost slot is refused", false, slotState{known: true}, 0, slotRefuse},
		{"lost slot with the next generation is recreated", false, slotState{known: true}, 1, slotRecreate},
		{"spent generation is refused", false, slotState{known: true, generation: 1}, 1, slotRefuse},
		{"skipping a generation is refused", false, slotState{known: true, generation: 1}, 3, slotRefuse},
		{"second loss with the next generation is recreated", false, slotState{known: true, generation: 1}, 2, slotRecreate},
		{"crash after the bump and before the slot refuses the same value", false, slotState{known: true, generation: 2}, 2, slotRefuse},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decideSlot(tt.exists, tt.state, tt.requested); got != tt.want {
				t.Fatalf("decideSlot(%v, %+v, %d) = %d, want %d", tt.exists, tt.state, tt.requested, got, tt.want)
			}
		})
	}
}
