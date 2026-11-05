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
	"sort"
	"strings"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
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
	controlPlaneChanged := pinUnsetNodeSetVersionsForUpdate(current, candidate, mask)
	if err := validateExclusiveVersionUpdate(current, candidate); err != nil {
		return err
	}
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
	if sourceVersion == targetVersion {
		scheduleClusterNodeSetUpgradeBatch(current, candidate, targetVersion, attemptID, now)
	} else {
		appendClusterUpgradeAttempt(candidate, sourceVersion, targetVersion, attemptID, now)
	}
	candidate.GetSpec().SetUpgradeAttemptId(attemptID)
	return nil
}

const maxClusterUpgradeHistoryEntries = 100

// retainRecentClusterUpgradeHistory keeps the newest accepted entries.
func retainRecentClusterUpgradeHistory(
	entries []*privatev1.ClusterUpgradeHistoryEntry,
) []*privatev1.ClusterUpgradeHistoryEntry {
	if len(entries) <= maxClusterUpgradeHistoryEntries {
		return entries
	}
	return entries[:maxClusterUpgradeHistoryEntries]
}

// appendClusterUpgradeHistory prepends entries and applies the shared cap for
// every upgrade component.
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

// appendClusterUpgradeAttempt records the accepted control-plane component
// of a whole-cluster attempt.
func appendClusterUpgradeAttempt(candidate *privatev1.Cluster,
	sourceVersion, targetVersion, attemptID string, now *timestamppb.Timestamp) {
	component := &privatev1.ClusterUpgradeComponent{}
	component.SetControlPlane(&emptypb.Empty{})
	entry := &privatev1.ClusterUpgradeHistoryEntry{}
	entry.SetComponent(component)
	entry.SetSourceVersion(sourceVersion)
	entry.SetTargetVersion(targetVersion)
	entry.SetAcceptedTime(now)
	entry.SetStateTransitionTime(now)
	entry.SetState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING)
	entry.SetAttemptId(attemptID)
	appendClusterUpgradeHistory(candidate, []*privatev1.ClusterUpgradeHistoryEntry{entry})
}

// sortedClusterNodeSetUpgradeNames returns every participating node set in
// deterministic order.
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

// applyClusterNodeSetUpgradeTargets moves each participating node-set reference
// to the accepted cluster target.
func applyClusterNodeSetUpgradeTargets(candidate *privatev1.Cluster, names []string) {
	targetRef := candidate.GetSpec().GetVersion()
	if targetRef == nil {
		return
	}
	for _, name := range names {
		candidate.GetSpec().GetNodeSets()[name].SetVersion(cloneMessage(targetRef))
	}
}

// appendClusterNodeSetUpgradeHistory records one entry per participating node
// set under the shared attempt ID.
func appendClusterNodeSetUpgradeHistory(current, candidate *privatev1.Cluster,
	names []string, targetVersion, attemptID string, now *timestamppb.Timestamp) {
	entries := make([]*privatev1.ClusterUpgradeHistoryEntry, 0, len(names))
	for _, name := range names {
		nodeSet := candidate.GetSpec().GetNodeSets()[name]
		sourceVersion := current.GetStatus().GetNodeSets()[name].GetObservedVersion()
		component := &privatev1.ClusterUpgradeComponent{}
		component.SetNodeSetId(name)
		entry := &privatev1.ClusterUpgradeHistoryEntry{}
		entry.SetComponent(component)
		entry.SetSourceVersion(sourceVersion)
		entry.SetTargetVersion(targetVersion)
		entry.SetAcceptedTime(now)
		entry.SetStateTransitionTime(now)
		entry.SetAttemptId(attemptID)
		entry.SetTimeoutNodeCount(max(int32(1), nodeSet.GetSize()))
		if sourceVersion == targetVersion {
			entry.SetState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED)
		} else {
			entry.SetState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING)
		}
		entries = append(entries, entry)
	}
	appendClusterUpgradeHistory(candidate, entries)
}

// scheduleClusterNodeSetUpgradeBatch moves every participating node-set
// reference and history entry to one shared cluster attempt.
func scheduleClusterNodeSetUpgradeBatch(current, candidate *privatev1.Cluster,
	targetVersion, attemptID string, now *timestamppb.Timestamp) {
	names := sortedClusterNodeSetUpgradeNames(candidate.GetSpec().GetNodeSets())
	if len(names) == 0 {
		return
	}
	applyClusterNodeSetUpgradeTargets(candidate, names)
	appendClusterNodeSetUpgradeHistory(current, candidate, names, targetVersion, attemptID, now)
	candidate.GetSpec().SetUpgradeAttemptId(attemptID)
}

// mergeClusterUpgradeHistory accepts private status transitions by attempt and
// component while retaining the server's audit fields and newest-first order.
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
	candidate.GetStatus().SetUpgradeHistory(result)
}

// mergeClusterUpgradeStateChanges merges only newer state
// transitions into stored history.
func mergeClusterUpgradeStateChanges(
	stored, incoming []*privatev1.ClusterUpgradeHistoryEntry,
) []*privatev1.ClusterUpgradeHistoryEntry {
	byAttemptAndComponent := make(map[clusterUpgradeHistoryKey]*privatev1.ClusterUpgradeHistoryEntry, len(incoming))
	for _, entry := range incoming {
		if entry.GetAttemptId() != "" && entry.GetComponent() != nil {
			byAttemptAndComponent[upgradeHistoryKey(entry)] = entry
		}
	}
	result := make([]*privatev1.ClusterUpgradeHistoryEntry, len(stored))
	for i, entry := range stored {
		result[i] = entry
		update := byAttemptAndComponent[upgradeHistoryKey(entry)]
		if update == nil || update.GetState() == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_UNSPECIFIED ||
			update.GetState() == entry.GetState() || update.GetStateTransitionTime() == nil ||
			!update.GetStateTransitionTime().AsTime().After(entry.GetStateTransitionTime().AsTime()) {
			continue
		}
		changed := proto.Clone(entry).(*privatev1.ClusterUpgradeHistoryEntry)
		changed.SetState(update.GetState())
		changed.SetMessage(update.GetMessage())
		changed.SetStateTransitionTime(update.GetStateTransitionTime())
		result[i] = changed
	}
	return result
}

type clusterUpgradeHistoryKey struct {
	attemptID    string
	nodeSetID    string
	controlPlane bool
}

// upgradeHistoryKey distinguishes components that share an attempt ID.
func upgradeHistoryKey(entry *privatev1.ClusterUpgradeHistoryEntry) clusterUpgradeHistoryKey {
	return clusterUpgradeHistoryKey{
		attemptID:    entry.GetAttemptId(),
		nodeSetID:    entry.GetComponent().GetNodeSetId(),
		controlPlane: entry.GetComponent().GetControlPlane() != nil,
	}
}

func populateUnsetNodeSetVersions(nodeSets map[string]*privatev1.ClusterNodeSet, version *privatev1.ClusterVersionReference) {
	if version == nil {
		return
	}
	for _, nodeSet := range nodeSets {
		if nodeSet != nil && refKey(nodeSet.GetVersion()) == "" {
			nodeSet.SetVersion(cloneMessage(version))
		}
	}
}

func (s *PrivateClustersServer) validateCreatedNodeSetVersions(ctx context.Context, cluster *privatev1.Cluster) error {
	spec := cluster.GetSpec()
	for name, nodeSet := range spec.GetNodeSets() {
		version, err := s.validateNewNodeSetVersion(ctx, cluster.GetMetadata(), spec.GetVersion(), nodeSet.GetVersion(), name)
		if err != nil {
			return err
		}
		nodeSet.SetVersion(version)
	}
	return nil
}

// retainOmittedNodeSetVersion keeps the stored node-set version, or the
// control-plane version for a newly added node set, when no version is selected.
func retainOmittedNodeSetVersion(controlPlaneVersion *privatev1.ClusterVersionReference,
	existing, nodeSet *privatev1.ClusterNodeSet) {
	if nodeSet != nil && refKey(nodeSet.GetVersion()) == "" {
		version := controlPlaneVersion
		if existing != nil {
			version = existing.GetVersion()
		}
		if version != nil {
			nodeSet.SetVersion(cloneMessage(version))
		}
	}
}

// pinUnsetNodeSetVersionsForUpdate preserves the established control-plane
// version for node sets that have no stored version of their own.
func pinUnsetNodeSetVersionsForUpdate(current, candidate *privatev1.Cluster, mask *fieldmaskpb.FieldMask) bool {
	versionOnlyNodeSet := len(mask.GetPaths()) == 1 &&
		strings.HasPrefix(mask.GetPaths()[0], "spec.node_sets.") &&
		strings.HasSuffix(mask.GetPaths()[0], ".version")
	controlPlaneChanged := updateIncludesField(mask, "spec.version") &&
		!sameClusterVersionReference(current.GetSpec().GetVersion(), candidate.GetSpec().GetVersion())
	if (updateIncludesField(mask, "spec.node_sets") && !versionOnlyNodeSet) || controlPlaneChanged {
		populateUnsetNodeSetVersions(candidate.GetSpec().GetNodeSets(), current.GetSpec().GetVersion())
	}
	return controlPlaneChanged
}

// validateExclusiveVersionUpdate makes spec.version the only version a caller
// can change. Node-set versions remain internal until Fulfillment schedules a
// whole-cluster batch.
func validateExclusiveVersionUpdate(current, candidate *privatev1.Cluster) error {
	for name, nodeSet := range candidate.GetSpec().GetNodeSets() {
		stored, exists := current.GetSpec().GetNodeSets()[name]
		if !exists || nodeSet == nil {
			continue
		}
		if nodeSetVersionChanged(stored, nodeSet, current.GetSpec().GetVersion()) {
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"node set '%s': version is system-managed; update spec.version", name)
		}
	}
	return nil
}

func nodeSetVersionChanged(stored, candidate *privatev1.ClusterNodeSet,
	controlPlaneVersion *privatev1.ClusterVersionReference) bool {
	baseline := stored.GetVersion()
	if refKey(baseline) == "" {
		baseline = controlPlaneVersion
	}
	target := candidate.GetVersion()
	return refKey(target) != "" && !sameClusterVersionReference(baseline, target)
}

func sameClusterVersionReference(a, b *privatev1.ClusterVersionReference) bool {
	if a.GetId() != "" && b.GetId() != "" {
		return a.GetId() == b.GetId()
	}
	return a.GetName() == b.GetName()
}

// validateVersionUpdate validates the selected cluster target and canonicalizes
// versions on newly added node sets. Existing node-set versions are
// system-managed.
func (s *PrivateClustersServer) validateVersionUpdate(ctx context.Context,
	request *privatev1.ClustersUpdateRequest) (*privatev1.ClusterVersion, error) {
	mask := request.GetUpdateMask()
	controlPlaneUpdated := updateIncludesField(mask, "spec.version")
	nodeSetsUpdated := s.updateAffectsNodeSets(mask)
	if !controlPlaneUpdated && !nodeSetsUpdated {
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
	if err := validateNodeSetVersionMask(mask, existing.GetSpec().GetNodeSets()); err != nil {
		return nil, err
	}
	var resolvedTarget *privatev1.ClusterVersion
	if controlPlaneUpdated {
		version, resolved, err := s.validateVersionReferenceUpdate(ctx, existing.GetMetadata(),
			existing.GetSpec().GetVersion(), newSpec.GetVersion())
		if err != nil {
			return nil, err
		}
		newSpec.SetVersion(version)
		resolvedTarget = resolved
	}
	if !nodeSetsUpdated || (len(mask.GetPaths()) > 0 && s.isUpdatingOnlySizes(mask)) {
		return resolvedTarget, nil
	}
	if err := s.validateNodeSetVersionUpdates(ctx, existing, newSpec); err != nil {
		return nil, err
	}
	return resolvedTarget, nil
}

func validateNodeSetVersionMask(mask *fieldmaskpb.FieldMask,
	existingNodeSets map[string]*privatev1.ClusterNodeSet) error {
	for _, path := range mask.GetPaths() {
		if !strings.HasPrefix(path, "spec.node_sets.") || !strings.HasSuffix(path, ".version") {
			continue
		}
		name := strings.TrimSuffix(strings.TrimPrefix(path, "spec.node_sets."), ".version")
		if existingNodeSets[name] == nil {
			return grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown node set '%s'", name)
		}
		return grpcstatus.Error(grpccodes.InvalidArgument,
			"node-set versions are system-managed; update spec.version")
	}
	return nil
}

// validateNodeSetVersionUpdates canonicalizes new node-set selections and
// rejects changes to an existing node set's internal version.
func (s *PrivateClustersServer) validateNodeSetVersionUpdates(ctx context.Context,
	existing *privatev1.Cluster, newSpec *privatev1.ClusterSpec) error {
	controlPlaneVersion := existing.GetSpec().GetVersion()
	for name, nodeSet := range newSpec.GetNodeSets() {
		if nodeSet == nil {
			continue
		}
		storedNodeSet := existing.GetSpec().GetNodeSets()[name]
		if storedNodeSet == nil {
			version, err := s.validateNewNodeSetVersion(ctx, existing.GetMetadata(), controlPlaneVersion, nodeSet.GetVersion(), name)
			if err != nil {
				return err
			}
			nodeSet.SetVersion(version)
			continue
		}
		if nodeSetVersionChanged(storedNodeSet, nodeSet, controlPlaneVersion) {
			return grpcstatus.Errorf(grpccodes.InvalidArgument,
				"node set '%s': version is system-managed; update spec.version", name)
		}
	}
	return nil
}

// validateNewNodeSetVersion validates a newly added node set and canonicalizes
// its selected reference.
func (s *PrivateClustersServer) validateNewNodeSetVersion(ctx context.Context, owner *privatev1.Metadata,
	controlPlaneVersion, selected *privatev1.ClusterVersionReference, name string) (*privatev1.ClusterVersionReference, error) {
	version, _, err := s.validateVersionReferenceUpdate(ctx, owner, controlPlaneVersion, selected)
	if err != nil {
		return nil, grpcstatus.Errorf(grpcstatus.Code(err), "node set '%s': %s", name, grpcstatus.Convert(err).Message())
	}
	if !sameClusterVersionReference(controlPlaneVersion, version) {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument,
			"node set '%s': version must match the control-plane version", name)
	}
	return version, nil
}

// validateVersionReferenceUpdate preserves an omitted or unchanged selection;
// changed selections use the same catalog lookup for control planes and node sets.
func (s *PrivateClustersServer) validateVersionReferenceUpdate(ctx context.Context,
	owner *privatev1.Metadata, stored, selected *privatev1.ClusterVersionReference,
) (*privatev1.ClusterVersionReference, *privatev1.ClusterVersion, error) {
	if refKey(selected) == "" || sameClusterVersionReference(stored, selected) {
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
	if sameClusterVersionReference(stored, selected) {
		return cloneMessage(stored), nil, nil
	}
	return selected, version, nil
}
