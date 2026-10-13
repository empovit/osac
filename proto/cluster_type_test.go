package proto_test

import (
	"testing"

	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
	publicv1 "github.com/osac-project/osac/proto/gen/osac/public/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestClusterUpgradeHistoryContract(t *testing.T) {
	accepted := timestamppb.Now()
	status := &privatev1.ClusterStatus{
		UpgradeHistory: []*privatev1.ClusterUpgradeHistoryEntry{{
			AttemptId:      "attempt-1",
			Component:      "control_plane",
			SourceVersion:  "4.21.0",
			TargetVersion:  "4.22.0",
			AcceptedTime:   accepted,
			LastStateTime:  accepted,
			State:          privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING,
			Message:        "Awaiting control plane",
		}},
	}

	encoded, err := protojson.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	var decoded privatev1.ClusterStatus
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.GetUpgradeHistory()) != 1 || decoded.GetUpgradeHistory()[0].GetAttemptId() != "attempt-1" {
		t.Fatalf("upgrade attempt identity lost across serialization: %+v", decoded.GetUpgradeHistory())
	}
	if decoded.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name("upgrade")) != nil {
		t.Fatal("separate upgrade status remains in ClusterStatus")
	}
	if (&publicv1.ClusterUpgradeHistoryEntry{}).ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name("attempt_id")) != nil {
		t.Fatal("private attempt ID leaked into public history")
	}
}

func TestClusterNodeSetVersionContract(t *testing.T) {
	nodeSet := &privatev1.ClusterNodeSet{
		Version:         &privatev1.ClusterVersionReference{Name: "4-21-0"},
		ObservedVersion: "4.21.0",
	}
	encoded, err := protojson.Marshal(nodeSet)
	if err != nil {
		t.Fatal(err)
	}
	var decoded privatev1.ClusterNodeSet
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.GetVersion().GetName() != nodeSet.GetVersion().GetName() || decoded.GetObservedVersion() != nodeSet.GetObservedVersion() {
		t.Fatalf("node set version fields lost across serialization: %+v", &decoded)
	}
	if (&privatev1.ClusterNodeSet{}).ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name("release_image")) != nil {
		t.Fatal("node set stores a release image alongside its version reference")
	}
}
