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
