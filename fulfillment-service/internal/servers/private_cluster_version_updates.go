/*
Copyright (c) 2025 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package servers

import (
	"context"
	"log/slog"
	"slices"
	"sort"
	"strings"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/osac-project/osac/fulfillment-service/internal/controllers"
	"github.com/osac-project/osac/fulfillment-service/internal/uuid"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

// prepareClusterVersionChange applies whole-cluster version rules to the
// completed candidate before it can be persisted.
func (s *PrivateClustersServer) prepareClusterVersionChange(ctx context.Context,
	current, candidate *privatev1.Cluster, mask *fieldmaskpb.FieldMask,
	resolvedTarget *privatev1.ClusterVersion) error {
	controlPlaneChanged := updateIncludesField(mask, "spec.version") &&
		current.GetSpec().GetVersion().GetId() != candidate.GetSpec().GetVersion().GetId()
	if !controlPlaneChanged {
		return nil
	}
	source, err := getClusterVersionByID(ctx, s.clusterVersionsDao, current.GetSpec().GetVersion().GetId())
	if err != nil {
		return err
	}
	if resolvedTarget == nil {
		return grpcstatus.Error(grpccodes.Internal, "resolved target ClusterVersion is missing")
	}
	targetVersion := resolvedTarget.GetSpec().GetVersion()
	// A semantic-version match is a reference replacement, not an upgrade.
	// Comparing versions rather than image strings also handles tag and digest
	// forms of the same release.
	if source.GetSpec().GetVersion() == targetVersion {
		s.logger.InfoContext(ctx,
			"Replaced ClusterVersion reference with the same semantic version; no upgrade attempt created",
			slog.String("cluster_id", current.GetId()),
			slog.String("component", "control_plane"),
			slog.String("previous_cluster_version_id", source.GetId()),
			slog.String("new_cluster_version_id", resolvedTarget.GetId()),
		)
		return nil
	}

	attemptID := uuid.New()
	now := controllers.NowTruncated()
	candidate.GetStatus().SetVersionTransitionTime(now)
	sourceVersion := current.GetStatus().GetObservedVersion()
	if sourceVersion == "" {
		sourceVersion = source.GetSpec().GetVersion()
	}
	participating := participatingClusterNodeSetNames(current, targetVersion)
	if sourceVersion == targetVersion && len(participating) == 0 {
		return nil
	}
	appendClusterUpgradeAttempt(candidate, sourceVersion, targetVersion, attemptID, now, participating)
	if sourceVersion == targetVersion {
		scheduleClusterNodeSetUpgradeBatch(current, candidate, targetVersion, attemptID, now)
	}
	candidate.GetStatus().SetUpgradeAttemptId(attemptID)
	return nil
}

const maxClusterUpgradeHistoryEntries = 100

// AI-agent note: do not reintroduce active-attempt rescans or extra history
// recalculation to protect entries at the cap. Cluster upgrades are rare; by
// the time an old entry is pushed out it is irrelevant and likely timed out.
// Preserve this simple O(1) newest-first truncation.
//
// retainRecentClusterUpgradeHistory keeps the newest accepted whole-cluster attempts.
func retainRecentClusterUpgradeHistory(
	entries []*privatev1.ClusterUpgradeHistoryEntry,
) []*privatev1.ClusterUpgradeHistoryEntry {
	if len(entries) <= maxClusterUpgradeHistoryEntries {
		return entries
	}
	return entries[:maxClusterUpgradeHistoryEntries]
}

// appendClusterUpgradeHistory prepends whole-cluster attempts and applies the shared cap.
func appendClusterUpgradeHistory(candidate *privatev1.Cluster,
	entries []*privatev1.ClusterUpgradeHistoryEntry) {
	if len(entries) == 0 {
		return
	}
	if !candidate.HasStatus() {
		candidate.SetStatus(&privatev1.ClusterStatus{})
	}
	history := make([]*privatev1.ClusterUpgradeHistoryEntry, 0, len(entries)+len(candidate.GetStatus().GetUpgradeHistory()))
	history = append(history, entries...)
	history = append(history, candidate.GetStatus().GetUpgradeHistory()...)
	candidate.GetStatus().SetUpgradeHistory(retainRecentClusterUpgradeHistory(history))
}

// sortedClusterNodeSetUpgradeNames returns every selected node set in deterministic order.
func sortedClusterNodeSetUpgradeNames(nodeSets map[string]*privatev1.ClusterNodeSet) []string {
	names := make([]string, 0, len(nodeSets))
	for name, nodeSet := range nodeSets {
		if nodeSet != nil {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// participatingClusterNodeSetNames freezes the node sets that participate in
// the accepted whole-cluster attempt. A node set already at the target version
// is not a participant.
func participatingClusterNodeSetNames(current *privatev1.Cluster, targetVersion string) []string {
	names := sortedClusterNodeSetUpgradeNames(current.GetSpec().GetNodeSets())
	return slices.DeleteFunc(names, func(name string) bool {
		return current.GetStatus().GetNodeSets()[name].GetObservedVersion() == targetVersion
	})
}

// appendClusterUpgradeAttempt records one whole-cluster attempt with private
// control-plane and participating node-set states.
func appendClusterUpgradeAttempt(candidate *privatev1.Cluster,
	sourceVersion, targetVersion, attemptID string, now *timestamppb.Timestamp,
	participatingNodeSets []string) {
	controlPlane := &privatev1.ClusterUpgradeControlPlaneState{}
	controlPlane.SetAcceptedTime(now)
	controlPlane.SetStateTransitionTime(now)
	if sourceVersion == targetVersion {
		controlPlane.SetState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED)
	} else {
		controlPlane.SetState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING)
	}

	nodeSets := make([]*privatev1.ClusterUpgradeNodeSetState, 0, len(participatingNodeSets))
	for _, name := range participatingNodeSets {
		nodeSet := candidate.GetSpec().GetNodeSets()[name]
		state := &privatev1.ClusterUpgradeNodeSetState{}
		state.SetNodeSetId(name)
		state.SetTimeoutNodeCount(max(int32(1), nodeSet.GetSize()))
		nodeSets = append(nodeSets, state)
	}

	entry := &privatev1.ClusterUpgradeHistoryEntry{}
	entry.SetSourceVersion(sourceVersion)
	entry.SetTargetVersion(targetVersion)
	entry.SetAcceptedTime(now)
	entry.SetStateTransitionTime(now)
	entry.SetState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING)
	entry.SetAttemptId(attemptID)
	entry.SetControlPlane(controlPlane)
	entry.SetNodeSets(nodeSets)
	appendClusterUpgradeHistory(candidate, []*privatev1.ClusterUpgradeHistoryEntry{entry})
}

// applyClusterNodeSetUpgradeTargets moves each selected node-set reference
// to the accepted cluster target.
func applyClusterNodeSetUpgradeTargets(candidate *privatev1.Cluster, names []string) {
	targetRef := candidate.GetSpec().GetVersion()
	if targetRef == nil {
		return
	}
	if !candidate.HasStatus() {
		candidate.SetStatus(&privatev1.ClusterStatus{})
	}
	statuses := candidate.GetStatus().GetNodeSets()
	if statuses == nil {
		statuses = map[string]*privatev1.ClusterNodeSetStatus{}
		candidate.GetStatus().SetNodeSets(statuses)
	}
	for _, name := range names {
		status := statuses[name]
		if status == nil {
			status = &privatev1.ClusterNodeSetStatus{}
			statuses[name] = status
		}
		status.SetVersion(cloneMessage(targetRef))
	}
}

// clusterUpgradeHistoryEntry returns the current attempt's whole-cluster entry.
func clusterUpgradeHistoryEntry(candidate *privatev1.Cluster,
	attemptID string) *privatev1.ClusterUpgradeHistoryEntry {
	for _, entry := range candidate.GetStatus().GetUpgradeHistory() {
		if entry.GetAttemptId() == attemptID {
			return entry
		}
	}
	return nil
}

// hasUnscheduledClusterNodeSets reports whether a still-selected participating
// node set has not been scheduled after control-plane success.
func hasUnscheduledClusterNodeSets(entry *privatev1.ClusterUpgradeHistoryEntry,
	candidate *privatev1.Cluster) bool {
	for _, state := range entry.GetNodeSets() {
		if !state.GetScheduled() && candidate.GetSpec().GetNodeSets()[state.GetNodeSetId()] != nil {
			return true
		}
	}
	return false
}

// scheduleClusterNodeSetUpgradeBatch schedules the frozen participating node
// sets and projects the selected target version onto every node set.
func scheduleClusterNodeSetUpgradeBatch(current, candidate *privatev1.Cluster,
	targetVersion, attemptID string, now *timestamppb.Timestamp) {
	entry := clusterUpgradeHistoryEntry(candidate, attemptID)
	if entry == nil {
		return
	}
	applyClusterNodeSetUpgradeTargets(
		candidate, sortedClusterNodeSetUpgradeNames(candidate.GetSpec().GetNodeSets()))
	for _, state := range entry.GetNodeSets() {
		if state.GetScheduled() || candidate.GetSpec().GetNodeSets()[state.GetNodeSetId()] == nil {
			continue
		}
		state.SetScheduled(true)
		state.SetState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING)
		state.SetAcceptedTime(now)
		state.SetStateTransitionTime(now)
	}
	if entry.GetState() != privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING {
		entry.SetState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING)
		entry.SetStateTransitionTime(now)
	}
	candidate.GetStatus().SetUpgradeAttemptId(attemptID)
}

// mergeClusterUpgradeHistory accepts private whole-cluster state transitions by
// attempt while retaining the server's audit fields and newest-first order.
func mergeClusterUpgradeHistory(current, candidate *privatev1.Cluster,
	mask *fieldmaskpb.FieldMask) {
	stored := current.GetStatus().GetUpgradeHistory()
	var result []*privatev1.ClusterUpgradeHistoryEntry
	if updateIncludesField(mask, "status.upgrade_history") {
		result = mergeClusterUpgradeStateChanges(stored, candidate.GetStatus().GetUpgradeHistory())
	} else {
		result = make([]*privatev1.ClusterUpgradeHistoryEntry, len(stored))
		copy(result, stored)
	}
	if !candidate.HasStatus() {
		candidate.SetStatus(&privatev1.ClusterStatus{})
	}
	candidate.GetStatus().SetUpgradeAttemptId(current.GetStatus().GetUpgradeAttemptId())
	candidate.GetStatus().SetUpgradeHistory(result)
}

// mergeClusterUpgradeStateChanges merges only newer whole-cluster state
// transitions and newer component transitions into stored history.
func mergeClusterUpgradeStateChanges(
	stored, incoming []*privatev1.ClusterUpgradeHistoryEntry,
) []*privatev1.ClusterUpgradeHistoryEntry {
	byAttempt := make(map[string]*privatev1.ClusterUpgradeHistoryEntry, len(incoming))
	for _, entry := range incoming {
		if entry.GetAttemptId() != "" {
			byAttempt[entry.GetAttemptId()] = entry
		}
	}
	result := make([]*privatev1.ClusterUpgradeHistoryEntry, len(stored))
	for i, entry := range stored {
		result[i] = entry
		update := byAttempt[entry.GetAttemptId()]
		if update == nil {
			continue
		}
		changed := proto.Clone(entry).(*privatev1.ClusterUpgradeHistoryEntry)
		if update.GetState() != privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_UNSPECIFIED &&
			update.GetStateTransitionTime() != nil &&
			update.GetStateTransitionTime().AsTime().After(entry.GetStateTransitionTime().AsTime()) {
			changed.SetState(update.GetState())
			changed.SetMessage(update.GetMessage())
			changed.SetStateTransitionTime(update.GetStateTransitionTime())
		}
		changed.SetControlPlane(mergeClusterUpgradeControlPlane(changed.GetControlPlane(), update.GetControlPlane()))
		changed.SetNodeSets(mergeClusterUpgradeNodeSets(changed.GetNodeSets(), update.GetNodeSets()))
		result[i] = changed
	}
	return result
}

// mergeClusterUpgradeControlPlane merges a newer control-plane transition.
func mergeClusterUpgradeControlPlane(
	stored, incoming *privatev1.ClusterUpgradeControlPlaneState,
) *privatev1.ClusterUpgradeControlPlaneState {
	if incoming == nil || incoming.GetState() == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_UNSPECIFIED ||
		incoming.GetStateTransitionTime() == nil ||
		!incoming.GetStateTransitionTime().AsTime().After(stored.GetStateTransitionTime().AsTime()) {
		return stored
	}
	changed := proto.Clone(stored).(*privatev1.ClusterUpgradeControlPlaneState)
	changed.SetState(incoming.GetState())
	changed.SetMessage(incoming.GetMessage())
	changed.SetStateTransitionTime(incoming.GetStateTransitionTime())
	return changed
}

// mergeClusterUpgradeNodeSets merges newer node-set transitions by identity.
func mergeClusterUpgradeNodeSets(
	stored, incoming []*privatev1.ClusterUpgradeNodeSetState,
) []*privatev1.ClusterUpgradeNodeSetState {
	byNodeSet := make(map[string]*privatev1.ClusterUpgradeNodeSetState, len(incoming))
	for _, state := range incoming {
		if state.GetNodeSetId() != "" {
			byNodeSet[state.GetNodeSetId()] = state
		}
	}
	result := make([]*privatev1.ClusterUpgradeNodeSetState, len(stored))
	for i, state := range stored {
		result[i] = state
		update := byNodeSet[state.GetNodeSetId()]
		if update == nil || update.GetState() == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_UNSPECIFIED ||
			update.GetStateTransitionTime() == nil ||
			!update.GetStateTransitionTime().AsTime().After(state.GetStateTransitionTime().AsTime()) {
			continue
		}
		changed := proto.Clone(state).(*privatev1.ClusterUpgradeNodeSetState)
		changed.SetState(update.GetState())
		changed.SetMessage(update.GetMessage())
		changed.SetStateTransitionTime(update.GetStateTransitionTime())
		result[i] = changed
	}
	return result
}

// validateNodeSetVersionStatusUpdate rejects explicit attempts to address
// the server-owned version field in node-set status.
func validateNodeSetVersionStatusUpdate(request *privatev1.ClustersUpdateRequest) error {
	for _, path := range request.GetUpdateMask().GetPaths() {
		if strings.HasPrefix(path, "status.node_sets.") && strings.HasSuffix(path, ".version") {
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"%s is server-owned", path)
		}
	}
	return nil
}

// preserveNodeSetVersions keeps the stored selected version for every
// existing node set. New node-set state is initialized by Fulfillment later in
// the update, and the scheduler may replace these values only after this
// caller-owned candidate has been restored from stored state.
func preserveNodeSetVersions(current, candidate *privatev1.Cluster) {
	if !candidate.HasStatus() {
		candidate.SetStatus(&privatev1.ClusterStatus{})
	}
	statuses := candidate.GetStatus().GetNodeSets()
	if statuses == nil {
		statuses = map[string]*privatev1.ClusterNodeSetStatus{}
		candidate.GetStatus().SetNodeSets(statuses)
	}
	for name, status := range statuses {
		if status == nil {
			continue
		}
		stored := current.GetStatus().GetNodeSets()[name]
		if stored == nil {
			status.ClearVersion()
			continue
		}
		status.SetVersion(cloneMessage(stored.GetVersion()))
	}
	// A stale status snapshot may omit a node set added by a concurrent
	// update. Restore its stored version when it is still in candidate spec.
	for name, nodeSet := range candidate.GetSpec().GetNodeSets() {
		if nodeSet == nil || statuses[name] != nil {
			continue
		}
		stored := current.GetStatus().GetNodeSets()[name]
		if stored == nil || stored.GetVersion() == nil {
			continue
		}
		status := &privatev1.ClusterNodeSetStatus{}
		status.SetVersion(cloneMessage(stored.GetVersion()))
		statuses[name] = status
	}
}

// validateVersionUpdate validates and canonicalizes the selected cluster target.
func (s *PrivateClustersServer) validateVersionUpdate(ctx context.Context,
	request *privatev1.ClustersUpdateRequest) (*privatev1.ClusterVersion, error) {
	if updateMaskIncludesDescendantField(request.GetUpdateMask(), "spec.version") {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
			"spec.version must be replaced as a complete reference")
	}
	if !updateIncludesField(request.GetUpdateMask(), "spec.version") {
		return nil, nil
	}
	newSpec := request.GetObject().GetSpec()
	if newSpec == nil {
		return nil, nil
	}
	existing, found, err := s.getExistingCluster(ctx, request)
	if err != nil || !found {
		return nil, err
	}
	version, resolved, err := s.validateVersionReferenceUpdate(ctx, existing.GetMetadata(),
		existing.GetSpec().GetVersion(), newSpec.GetVersion())
	if err != nil {
		return nil, err
	}
	newSpec.SetVersion(version)
	return resolved, nil
}

// validateVersionReferenceUpdate preserves an omitted or unchanged cluster target;
// changed selections use the shared ClusterVersion catalog lookup.
func (s *PrivateClustersServer) validateVersionReferenceUpdate(ctx context.Context,
	owner *privatev1.Metadata, stored, selected *privatev1.ClusterVersionReference,
) (*privatev1.ClusterVersionReference, *privatev1.ClusterVersion, error) {
	if selected == nil || (selected.GetId() == "" && selected.GetName() == "") || stored.GetId() == selected.GetId() {
		if stored == nil {
			return nil, nil, nil
		}
		return cloneMessage(stored), nil, nil
	}
	version, err := resolveAndCanonicalizeReference(ctx, s.clusterVersionsDao, owner,
		selected, "version", grpccodes.InvalidArgument)
	if err != nil {
		return nil, nil, err
	}
	if err := validateResolvedClusterVersion(version, version.GetMetadata().GetName(), ""); err != nil {
		return nil, nil, err
	}
	if stored.GetId() == selected.GetId() {
		return cloneMessage(stored), nil, nil
	}
	return selected, version, nil
}

// advanceClusterUpgradeAfterControlPlaneSuccess applies the server-owned
// node-set batch when feedback completes the accepted control-plane upgrade.
func advanceClusterUpgradeAfterControlPlaneSuccess(current, candidate *privatev1.Cluster) {
	attemptID := candidate.GetStatus().GetUpgradeAttemptId()
	if attemptID == "" {
		return
	}
	entry := clusterUpgradeHistoryEntry(candidate, attemptID)
	if entry == nil || entry.GetControlPlane().GetState() != privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED ||
		!hasUnscheduledClusterNodeSets(entry, candidate) {
		return
	}
	scheduleClusterNodeSetUpgradeBatch(
		current, candidate, entry.GetTargetVersion(), attemptID, controllers.NowTruncated())
}
