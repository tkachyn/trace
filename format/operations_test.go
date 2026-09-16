package format

import (
	"strings"
	"testing"
)

func TestDecodeJSONLProducesOneBatch(t *testing.T) {
	request, err := DecodeJSONL(strings.NewReader(`{"type":"header","protocol":"trace.batch","version":1,"request_id":"jsonl-1","actor":"agent:test"}
{"type":"event","client_id":"event-1","event_type":"tool.observation","payload":{"value":1},"namespace":"test"}
{"type":"memory","client_id":"memory-1","kind":"fact","content":"value is one","namespace":"test","derived_from":["event-1"]}
{"type":"commit"}`))
	if err != nil {
		t.Fatalf("decode JSONL: %v", err)
	}
	if request.RequestID != "jsonl-1" || len(request.Events) != 1 || len(request.Memories) != 1 {
		t.Fatalf("decoded request = %#v", request)
	}
	if request.Memories[0].DerivedFrom[0] != "event-1" {
		t.Fatalf("derived-from = %#v, want event-1", request.Memories[0].DerivedFrom)
	}
}

func TestExtractionProposalConvertsToBatch(t *testing.T) {
	proposal := ExtractionProposal{
		Type:       ExtractionProtocol,
		Version:    ProtocolVersion,
		ProposalID: "proposal-1",
		Namespace:  "user:tim",
		Actor:      "agent:extractor",
		Memories: []MemoryProposal{{
			ClientID: "memory-1",
			Kind:     "fact",
			Content:  "Tim prefers Go",
		}},
	}
	request, err := proposal.ToBatchRequest()
	if err != nil {
		t.Fatalf("convert proposal: %v", err)
	}
	if request.Protocol != BatchProtocol || request.RequestID != proposal.ProposalID {
		t.Fatalf("batch request = %#v", request)
	}
	if request.Memories[0].Namespace != "user:tim" {
		t.Fatalf("proposal namespace = %q, want user:tim", request.Memories[0].Namespace)
	}
}

func TestUnsupportedBatchVersionIsRejected(t *testing.T) {
	err := (BatchRequest{
		Protocol:  BatchProtocol,
		Version:   ProtocolVersion + 1,
		RequestID: "unsupported",
	}).Validate()
	if err == nil {
		t.Fatal("unsupported batch version was accepted")
	}
}
