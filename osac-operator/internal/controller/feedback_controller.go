/*
Copyright (c) 2025 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/go-logr/logr"
	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	clnt "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"

	ckv1alpha1 "github.com/osac-project/osac/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac/osac-operator/internal/controller/baremetalworker"
	"github.com/osac-project/osac/osac-operator/internal/controller/feedback"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

// FeedbackReconciler sends updates to the fulfillment service.
type FeedbackReconciler struct {
	bridge                *feedback.Bridge[*ckv1alpha1.ClusterOrder, *privatev1.Cluster]
	clusterOrderNamespace string
}

// FeedbackReconcilerConfig holds configuration for ClusterOrder feedback.
type FeedbackReconcilerConfig struct {
	ClusterOrderNamespace string
	UpgradeTimeouts       ClusterUpgradeTimeouts
}

// ClusterUpgradeTimeouts controls upgrade observation deadlines.
type ClusterUpgradeTimeouts struct {
	Pending         time.Duration
	ControlPlane    time.Duration
	NodePoolPerNode time.Duration
}

// DefaultClusterUpgradeTimeouts returns the deployment defaults for upgrade
// observation deadlines.
func DefaultClusterUpgradeTimeouts() ClusterUpgradeTimeouts {
	return ClusterUpgradeTimeouts{
		Pending: 10 * time.Minute, ControlPlane: 40 * time.Minute,
		NodePoolPerNode: 20 * time.Minute,
	}
}

// withDefaults replaces unset or invalid timeout values with
// deployment defaults.
func (t ClusterUpgradeTimeouts) withDefaults() ClusterUpgradeTimeouts {
	defaults := DefaultClusterUpgradeTimeouts()
	if t.Pending <= 0 {
		t.Pending = defaults.Pending
	}
	if t.ControlPlane <= 0 {
		t.ControlPlane = defaults.ControlPlane
	}
	if t.NodePoolPerNode <= 0 {
		t.NodePoolPerNode = defaults.NodePoolPerNode
	}
	return t
}

// NewFeedbackReconciler creates a reconciler that sends to the fulfillment service updates about cluster orders.
func NewFeedbackReconciler(hubClient clnt.Client, grpcConn grpc.ClientConnInterface,
	config FeedbackReconcilerConfig) *FeedbackReconciler {
	return &FeedbackReconciler{
		bridge:                newClusterOrderFeedbackBridge(hubClient, privatev1.NewClustersClient(grpcConn), config),
		clusterOrderNamespace: config.ClusterOrderNamespace,
	}
}

// SetupWithManager adds the reconciler to the controller manager.
func (r *FeedbackReconciler) SetupWithManager(mgr mcmanager.Manager) error {
	localMgr := mgr.GetLocalManager()
	if localMgr == nil {
		return fmt.Errorf("local manager is nil")
	}

	return ctrl.NewControllerManagedBy(localMgr).
		Named("clusterorder-feedback").
		For(&ckv1alpha1.ClusterOrder{}, builder.WithPredicates(NamespacePredicate(r.clusterOrderNamespace))).
		Complete(r)
}

// Reconcile delegates to the shared feedback Bridge.
func (r *FeedbackReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	return r.bridge.Reconcile(ctx, request)
}

// hasUpgradeAwaitingTimeout reports whether a current attempt still needs
// timeout evaluation or continued monitoring after an earlier timeout.
func hasUpgradeAwaitingTimeout(remote *privatev1.Cluster) bool {
	for _, entry := range remote.GetStatus().GetUpgradeHistory() {
		if entry.GetAttemptId() != remote.GetStatus().GetUpgradeAttemptId() {
			continue
		}
		if entry.GetState() != privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED &&
			entry.GetState() != privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_FAILED &&
			hasUnscheduledClusterNodeSets(remote, entry) {
			return true
		}
		if isPendingOrProgressingUpgrade(entry.GetControlPlane().GetState()) {
			return true
		}
		for _, state := range entry.GetNodeSets() {
			if clusterUpgradeNodeSetParticipates(remote, state) &&
				state.GetScheduled() && isPendingOrProgressingUpgrade(state.GetState()) {
				return true
			}
		}
	}
	return false
}

// newClusterOrderFeedbackBridge creates a Bridge wired to the given clients. Used by both
// the constructor and tests.
func newClusterOrderFeedbackBridge(hubClient clnt.Client, clustersClient privatev1.ClustersClient,
	config FeedbackReconcilerConfig) *feedback.Bridge[*ckv1alpha1.ClusterOrder, *privatev1.Cluster] {
	return &feedback.Bridge[*ckv1alpha1.ClusterOrder, *privatev1.Cluster]{
		Client:    hubClient,
		Finalizer: osacClusterOrderFeedbackFinalizer,
		IDLabel:   osacClusterOrderIDLabel,
		Kind:      "ClusterOrder",
		IDKey:     "clusterID",
		NewObject: func() *ckv1alpha1.ClusterOrder { return &ckv1alpha1.ClusterOrder{} },
		Fetch: func(ctx context.Context, id string) (*privatev1.Cluster, error) {
			response, err := clustersClient.Get(ctx, privatev1.ClustersGetRequest_builder{Id: id}.Build())
			if err != nil {
				return nil, err
			}
			cluster := response.GetObject()
			if cluster == nil {
				return nil, errors.New("cluster response contained nil object")
			}
			if !cluster.HasSpec() {
				cluster.SetSpec(&privatev1.ClusterSpec{})
			}
			if !cluster.HasStatus() {
				cluster.SetStatus(&privatev1.ClusterStatus{})
			}
			return cluster, nil
		},
		Save: func(ctx context.Context, remote *privatev1.Cluster) error {
			_, err := clustersClient.Update(ctx, privatev1.ClustersUpdateRequest_builder{
				Object: remote,
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{
					feedbackStatusConditionsPath, feedbackStatusStatePath, "status.api_url", "status.console_url", "status.api_endpoint",
					"status.ingress_endpoint", feedbackStatusStateTransitionTimePath, "status.kubeconfig_secret", "status.password_secret", "status.hub",
					"status.node_sets", "status.observed_version", "status.add_on_operators", "status.upgrade_history",
				}},
			}.Build())
			return err
		},
		Signal: func(ctx context.Context, id string) error {
			_, err := clustersClient.Signal(ctx, privatev1.ClustersSignalRequest_builder{
				Id: id,
			}.Build())
			return err
		},
		SyncUpdate: newClusterOrderSyncUpdate(hubClient, config),
		SyncDelete: syncClusterOrderDelete,
		ResultForUpdate: func(_ *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) ctrl.Result {
			if hasUpgradeAwaitingTimeout(remote) {
				return ctrl.Result{RequeueAfter: time.Minute}
			}
			return ctrl.Result{}
		},
	}
}

// newClusterOrderSyncUpdate returns a SyncUpdate function that captures hubClient
// for HostedCluster URL lookups on the Ready phase.
func newClusterOrderSyncUpdate(hubClient clnt.Client,
	config FeedbackReconcilerConfig) func(context.Context, *ckv1alpha1.ClusterOrder, *privatev1.Cluster) error {
	timeouts := config.UpgradeTimeouts.withDefaults()
	return func(ctx context.Context, clusterOrder *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) error {
		syncClusterOrderConditions(ctx, clusterOrder, remote)
		syncClusterOrderWorkerConditions(clusterOrder, remote)
		syncClusterOrderPhase(ctx, clusterOrder, remote)
		if err := syncClusterOrderURLs(ctx, hubClient, clusterOrder, remote); err != nil {
			return err
		}
		syncClusterOrderNodeRequests(ctx, clusterOrder, remote)
		syncClusterOrderObservedVersions(clusterOrder, remote)
		syncClusterOrderUpgradeObservationsAt(clusterOrder, remote, time.Now(), timeouts)
		if err := syncRemovedNodeSets(ctx, hubClient, clusterOrder, remote); err != nil {
			return err
		}
		syncClusterOrderAddOnOperators(clusterOrder, remote)
		syncClusterOrderVIPEndpoints(clusterOrder, remote)
		return nil
	}
}

// syncClusterOrderUpgradeObservationsAt applies component timeouts and copies
// observations into the current whole-cluster attempt.
func syncClusterOrderUpgradeObservationsAt(order *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster,
	now time.Time, timeouts ...ClusterUpgradeTimeouts) {
	config := DefaultClusterUpgradeTimeouts()
	if len(timeouts) > 0 {
		config = timeouts[0].withDefaults()
	}
	entry := currentClusterUpgradeEntry(remote)
	if entry == nil {
		return
	}
	applyUpgradeTimeouts(remote, entry, now, config)
	syncControlPlaneUpgradeObservationAt(order.Status.UpgradeObservation, remote, entry, now)
	for _, request := range order.Status.NodeRequests {
		syncNodeSetUpgradeObservationAt(request, remote, entry, now)
	}
	aggregateClusterUpgradeEntry(remote, entry, now)
}

// currentClusterUpgradeEntry returns the whole-cluster history entry for the
// current private attempt.
func currentClusterUpgradeEntry(remote *privatev1.Cluster) *privatev1.ClusterUpgradeHistoryEntry {
	attemptID := remote.GetStatus().GetUpgradeAttemptId()
	if attemptID == "" {
		return nil
	}
	for _, entry := range remote.GetStatus().GetUpgradeHistory() {
		if entry.GetAttemptId() == attemptID {
			return entry
		}
	}
	return nil
}

// clusterUpgradeNodeSetState returns the private state for a node-set ID.
func clusterUpgradeNodeSetState(entry *privatev1.ClusterUpgradeHistoryEntry,
	name string) *privatev1.ClusterUpgradeNodeSetState {
	for _, state := range entry.GetNodeSets() {
		if state.GetNodeSetId() == name {
			return state
		}
	}
	return nil
}

// syncControlPlaneUpgradeObservationAt copies the control-plane observation
// into the private control-plane state.
func syncControlPlaneUpgradeObservationAt(observation *ckv1alpha1.UpgradeObservation,
	remote *privatev1.Cluster, entry *privatev1.ClusterUpgradeHistoryEntry, now time.Time) {
	if observation == nil || observation.AttemptID == "" ||
		observation.AttemptID != remote.GetStatus().GetUpgradeAttemptId() {
		return
	}
	syncControlPlaneUpgradeState(entry, entry.GetControlPlane(), observation, now)
}

// syncNodeSetUpgradeObservationAt copies a node-set observation into its
// matching private node-set state.
func syncNodeSetUpgradeObservationAt(request ckv1alpha1.NodeRequestStatus,
	remote *privatev1.Cluster, entry *privatev1.ClusterUpgradeHistoryEntry, now time.Time) {
	observation := request.UpgradeObservation
	if observation == nil || observation.AttemptID == "" ||
		observation.AttemptID != remote.GetStatus().GetUpgradeAttemptId() {
		return
	}
	state := clusterUpgradeNodeSetState(entry, request.NodeSet)
	if state == nil {
		return
	}
	syncNodeSetUpgradeState(entry, state, observation, now)
}

// syncControlPlaneUpgradeState advances the private control-plane state. A
// timed-out control plane can still receive a later success or failure.
func syncControlPlaneUpgradeState(entry *privatev1.ClusterUpgradeHistoryEntry,
	state *privatev1.ClusterUpgradeControlPlaneState, observation *ckv1alpha1.UpgradeObservation, now time.Time) {
	current := state.GetState()
	if current == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED ||
		current == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_FAILED {
		return
	}
	next := current
	message := state.GetMessage()
	switch {
	case observationSucceededAtTarget(observation, entry):
		next = privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED
		message = ""
	case observation.State == ckv1alpha1.UpgradeObservationFailed:
		next = privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_FAILED
		message = observation.Message
	case current == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING &&
		observation.State == ckv1alpha1.UpgradeObservationProgressing &&
		observation.Version == entry.GetTargetVersion():
		next = privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING
		message = ""
	}
	setControlPlaneUpgradeState(state, next, message, now)
}

// syncNodeSetUpgradeState advances a private node-set state. A timed-out node
// set can still receive a later success or failure.
func syncNodeSetUpgradeState(entry *privatev1.ClusterUpgradeHistoryEntry,
	state *privatev1.ClusterUpgradeNodeSetState, observation *ckv1alpha1.UpgradeObservation, now time.Time) {
	current := state.GetState()
	if current == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED ||
		current == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_FAILED {
		return
	}
	next := current
	message := state.GetMessage()
	switch {
	case observationSucceededAtTarget(observation, entry):
		next = privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED
		message = ""
	case observation.State == ckv1alpha1.UpgradeObservationFailed:
		next = privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_FAILED
		message = observation.Message
	case current == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING &&
		observation.State == ckv1alpha1.UpgradeObservationProgressing:
		next = privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING
		message = ""
	}
	setNodeSetUpgradeState(state, next, message, now)
}

// observationSucceededAtTarget reports whether the observation proves success
// at the whole-cluster target version.
func observationSucceededAtTarget(observation *ckv1alpha1.UpgradeObservation,
	entry *privatev1.ClusterUpgradeHistoryEntry) bool {
	return observation.State != ckv1alpha1.UpgradeObservationFailed &&
		(observation.State == ckv1alpha1.UpgradeObservationSucceeded || observation.ReadyAtVersion) &&
		observation.Version != "" && observation.Version == entry.GetTargetVersion()
}

// applyUpgradeTimeouts marks overdue participating states as timed out while
// observation continues.
func applyUpgradeTimeouts(remote *privatev1.Cluster, entry *privatev1.ClusterUpgradeHistoryEntry,
	now time.Time, config ClusterUpgradeTimeouts) {
	if state := entry.GetControlPlane(); isPendingOrProgressingUpgrade(state.GetState()) {
		origin, deadline, message := controlPlaneUpgradeTimeoutDetails(state, config)
		if !now.Before(origin.Add(deadline)) {
			setControlPlaneUpgradeState(state, privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT,
				message, now)
		}
	}
	for _, state := range entry.GetNodeSets() {
		if !clusterUpgradeNodeSetParticipates(remote, state) || !state.GetScheduled() ||
			!isPendingOrProgressingUpgrade(state.GetState()) {
			continue
		}
		origin, deadline, message := nodeSetUpgradeTimeoutDetails(state, config)
		if !now.Before(origin.Add(deadline)) {
			setNodeSetUpgradeState(state, privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT,
				message, now)
		}
	}
}

// isPendingOrProgressingUpgrade reports whether the private state is open.
func isPendingOrProgressingUpgrade(state privatev1.ClusterUpgradeState) bool {
	return state == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING ||
		state == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING
}

// clusterUpgradeNodeSetParticipates reports whether the node set still exists
// in the Cluster spec.
func clusterUpgradeNodeSetParticipates(remote *privatev1.Cluster,
	state *privatev1.ClusterUpgradeNodeSetState) bool {
	name := state.GetNodeSetId()
	return name != "" && remote.GetSpec().GetNodeSets()[name] != nil
}

// controlPlaneUpgradeTimeoutDetails chooses the acceptance or progress clock
// and deadline for the control plane.
func controlPlaneUpgradeTimeoutDetails(state *privatev1.ClusterUpgradeControlPlaneState,
	config ClusterUpgradeTimeouts) (time.Time, time.Duration, string) {
	if state.GetState() == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING {
		return state.GetAcceptedTime().AsTime(), config.Pending,
			fmt.Sprintf("Control plane upgrade progress was not observed within %s of acceptance; observation continues", config.Pending)
	}
	origin := state.GetAcceptedTime().AsTime()
	if last := state.GetStateTransitionTime(); last != nil {
		origin = last.AsTime()
	}
	return origin, config.ControlPlane,
		fmt.Sprintf("Control plane upgrade completion was not observed within %s of progressing; observation continues", config.ControlPlane)
}

// nodeSetUpgradeTimeoutDetails chooses the scheduling or progress clock and
// deadline for a node set.
func nodeSetUpgradeTimeoutDetails(state *privatev1.ClusterUpgradeNodeSetState,
	config ClusterUpgradeTimeouts) (time.Time, time.Duration, string) {
	name := fmt.Sprintf("Node set %q", state.GetNodeSetId())
	if state.GetState() == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING {
		return state.GetAcceptedTime().AsTime(), config.Pending,
			fmt.Sprintf("%s upgrade progress was not observed within %s of acceptance; observation continues", name, config.Pending)
	}
	origin := state.GetAcceptedTime().AsTime()
	if last := state.GetStateTransitionTime(); last != nil {
		origin = last.AsTime()
	}
	deadline := nodeSetCompletionTimeout(state, config.NodePoolPerNode)
	return origin, deadline,
		fmt.Sprintf("%s upgrade completion was not observed within %s of progressing; observation continues", name, deadline)
}

// nodeSetCompletionTimeout uses the frozen node-set count and caps overflow.
func nodeSetCompletionTimeout(state *privatev1.ClusterUpgradeNodeSetState,
	perNode time.Duration) time.Duration {
	nodes := max(1, state.GetTimeoutNodeCount())
	const maxDuration = time.Duration(1<<63 - 1)
	if time.Duration(nodes) > maxDuration/perNode {
		return maxDuration
	}
	return time.Duration(nodes) * perNode
}

// setControlPlaneUpgradeState records a control-plane transition and preserves
// monotonic transition times.
func setControlPlaneUpgradeState(state *privatev1.ClusterUpgradeControlPlaneState,
	next privatev1.ClusterUpgradeState, message string, now time.Time) {
	current := state.GetState()
	if next != current {
		if last := state.GetStateTransitionTime(); last != nil && !now.After(last.AsTime()) {
			now = last.AsTime().Add(time.Nanosecond)
		}
		state.SetState(next)
		state.SetMessage(message)
		state.SetStateTransitionTime(timestamppb.New(now))
	}
}

// setNodeSetUpgradeState records a node-set transition and preserves monotonic
// transition times.
func setNodeSetUpgradeState(state *privatev1.ClusterUpgradeNodeSetState,
	next privatev1.ClusterUpgradeState, message string, now time.Time) {
	current := state.GetState()
	if next != current {
		if last := state.GetStateTransitionTime(); last != nil && !now.After(last.AsTime()) {
			now = last.AsTime().Add(time.Nanosecond)
		}
		state.SetState(next)
		state.SetMessage(message)
		state.SetStateTransitionTime(timestamppb.New(now))
	}
}

// aggregateClusterUpgradeEntry updates the whole-cluster state from its private
// control-plane and participating node-set states.
func aggregateClusterUpgradeEntry(remote *privatev1.Cluster,
	entry *privatev1.ClusterUpgradeHistoryEntry, now time.Time) {
	controlPlane := entry.GetControlPlane()
	nodeSets := participatingClusterUpgradeNodeSets(remote, entry)

	next := privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING
	message := ""
	allSucceeded := controlPlane.GetState() == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED
	anyTimedOut := controlPlane.GetState() == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT
	anyFailed := controlPlane.GetState() == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_FAILED
	anyProgressing := controlPlane.GetState() == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING
	if anyFailed && message == "" {
		message = controlPlane.GetMessage()
	}
	if anyTimedOut && message == "" {
		message = controlPlane.GetMessage()
	}

	for _, state := range nodeSets {
		switch state.GetState() {
		case privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_FAILED:
			anyFailed = true
			if message == "" {
				message = state.GetMessage()
			}
		case privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT:
			anyTimedOut = true
			if message == "" {
				message = state.GetMessage()
			}
		case privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING:
			anyProgressing = true
		case privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED:
		case privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING:
		}
		if state.GetState() != privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED {
			allSucceeded = false
		}
	}

	// A successful control plane schedules the frozen node-set batch. Until
	// those node sets are scheduled, the whole attempt remains open.
	if controlPlane.GetState() == privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED &&
		hasUnscheduledClusterNodeSets(remote, entry) {
		anyProgressing = true
		allSucceeded = false
	}

	switch {
	case anyFailed:
		next = privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_FAILED
	case allSucceeded:
		next = privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED
	case anyTimedOut:
		next = privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT
	case anyProgressing:
		next = privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING
	}
	setUpgradeEntryState(entry, next, message, now)
}

// hasUnscheduledClusterNodeSets reports whether a still-selected participating
// node set has not been scheduled.
func hasUnscheduledClusterNodeSets(remote *privatev1.Cluster,
	entry *privatev1.ClusterUpgradeHistoryEntry) bool {
	for _, state := range entry.GetNodeSets() {
		if !state.GetScheduled() && clusterUpgradeNodeSetParticipates(remote, state) {
			return true
		}
	}
	return false
}

// participatingClusterUpgradeNodeSets returns scheduled node-set states that
// still affect the whole-cluster result.
func participatingClusterUpgradeNodeSets(remote *privatev1.Cluster,
	entry *privatev1.ClusterUpgradeHistoryEntry) []*privatev1.ClusterUpgradeNodeSetState {
	nodeSets := make([]*privatev1.ClusterUpgradeNodeSetState, 0, len(entry.GetNodeSets()))
	for _, state := range entry.GetNodeSets() {
		if clusterUpgradeNodeSetParticipates(remote, state) && state.GetScheduled() {
			nodeSets = append(nodeSets, state)
		}
	}
	return nodeSets
}

// setUpgradeEntryState records a whole-cluster state transition and preserves
// monotonic transition times.
func setUpgradeEntryState(entry *privatev1.ClusterUpgradeHistoryEntry,
	next privatev1.ClusterUpgradeState, message string, now time.Time) {
	state := entry.GetState()
	if next != state {
		if last := entry.GetStateTransitionTime(); last != nil && !now.After(last.AsTime()) {
			now = last.AsTime().Add(time.Nanosecond)
		}
		entry.SetState(next)
		entry.SetMessage(message)
		entry.SetStateTransitionTime(timestamppb.New(now))
	}
}

// A successful NodePool list is the evidence that a removed NodeSet has
// finished deleting. Until then, retain its observed status.
func syncRemovedNodeSets(ctx context.Context, hubClient clnt.Client,
	order *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) error {
	removed := removedNodeSetsWithStatus(remote)
	if len(removed) == 0 || order.GetClusterReferenceNamespace() == "" {
		return nil
	}
	pools := &hypershiftv1beta1.NodePoolList{}
	if err := hubClient.List(ctx, pools, clnt.InNamespace(order.GetClusterReferenceNamespace()),
		labelSelectorFromInstance(order)); err != nil {
		return err
	}
	for i := range pools.Items {
		if name, ok := nodePoolNodeSet(&pools.Items[i]); ok {
			delete(removed, name)
		}
	}
	for name := range removed {
		delete(remote.GetStatus().GetNodeSets(), name)
	}
	return nil
}

// removedNodeSetsWithStatus includes removed node sets whose status still
// needs pruning.
func removedNodeSetsWithStatus(remote *privatev1.Cluster) map[string]bool {
	removed := map[string]bool{}
	for name := range remote.GetStatus().GetNodeSets() {
		if remote.GetSpec().GetNodeSets()[name] == nil {
			removed[name] = true
		}
	}
	return removed
}

// syncClusterOrderVIPEndpoints copies the observed management API and guest ingress
// addresses from ClusterOrder status to the Fulfillment Cluster.
func syncClusterOrderVIPEndpoints(clusterOrder *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) {
	if clusterOrder.Status.ApiEndpoint != "" {
		remote.GetStatus().SetApiEndpoint(clusterOrder.Status.ApiEndpoint)
	}
	if clusterOrder.Status.IngressEndpoint != "" {
		remote.GetStatus().SetIngressEndpoint(clusterOrder.Status.IngressEndpoint)
	}
}

// syncClusterOrderAddOnOperators publishes one status for each requested operator.
// The latest recorded attempt is used when an operator has multiple jobs; operators
// without a job are reported as pending.
func syncClusterOrderAddOnOperators(clusterOrder *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) {
	latestJobs := make(map[string]*ckv1alpha1.AddOnOperatorJobStatus, len(clusterOrder.Status.AddOnOperatorJobs))
	for i := range clusterOrder.Status.AddOnOperatorJobs {
		job := &clusterOrder.Status.AddOnOperatorJobs[i]
		latest, ok := latestJobs[job.Name]
		if !ok || job.Timestamp.Time.After(latest.Timestamp.Time) {
			latestJobs[job.Name] = job
		}
	}

	statuses := make([]*privatev1.AddOnOperatorStatus, 0, len(clusterOrder.Spec.AddOnOperators))
	for _, name := range clusterOrder.Spec.AddOnOperators {
		state := privatev1.AddOnOperatorInstallState_ADD_ON_OPERATOR_INSTALL_STATE_PENDING
		message := ""
		if job, ok := latestJobs[name]; ok {
			state = addOnOperatorInstallState(job.State)
			message = sanitizeFeedbackText(job.Message)
		}
		statuses = append(statuses, privatev1.AddOnOperatorStatus_builder{
			Name:    name,
			State:   state,
			Message: message,
		}.Build())
	}
	remote.GetStatus().SetAddOnOperators(statuses)
}

// addOnOperatorInstallState converts an AAP job state to the public operator state.
func addOnOperatorInstallState(state ckv1alpha1.JobState) privatev1.AddOnOperatorInstallState {
	switch state {
	case ckv1alpha1.JobStatePending, ckv1alpha1.JobStateWaiting, ckv1alpha1.JobStateRunning:
		return privatev1.AddOnOperatorInstallState_ADD_ON_OPERATOR_INSTALL_STATE_INSTALLING
	case ckv1alpha1.JobStateSucceeded:
		return privatev1.AddOnOperatorInstallState_ADD_ON_OPERATOR_INSTALL_STATE_INSTALLED
	case ckv1alpha1.JobStateFailed, ckv1alpha1.JobStateCanceled:
		return privatev1.AddOnOperatorInstallState_ADD_ON_OPERATOR_INSTALL_STATE_FAILED
	case ckv1alpha1.JobStateUnknown:
		return privatev1.AddOnOperatorInstallState_ADD_ON_OPERATOR_INSTALL_STATE_INSTALLING
	default:
		return privatev1.AddOnOperatorInstallState_ADD_ON_OPERATOR_INSTALL_STATE_UNSPECIFIED
	}
}

func syncClusterOrderDelete(ctx context.Context, clusterOrder *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) error {
	syncClusterOrderConditions(ctx, clusterOrder, remote)
	syncClusterOrderWorkerConditions(clusterOrder, remote)
	// The state is forced to DELETING regardless of the CR phase, so the READY condition
	// must go too: the CR may still report Ready when feedback handles the deletion first,
	// and the reported condition must agree with the reported state.
	clearClusterReadyCondition(remote)
	syncClusterOrderNodeRequests(ctx, clusterOrder, remote)
	setClusterDeletingStateWithTransitionTime(ctx, clusterOrder, remote)
	return nil
}

// setClusterStateWithTransitionTime records the state and stamps its transition time when
// the stored state changes. The time is the ClusterOrder's recorded phase transition time
// (PhaseTransitionTime), not the moment feedback observed the change: feedback can run long
// after the phase moved, and those hours belong to the state that was actually current. The
// metering source contract rejects a state change whose status.state_transition_time is not
// later than the stored one, so an update that changes the state must carry a fresh time in
// the same sync, and a CR stamp that is not later than the stored time is bumped by one
// microsecond (see copyPhaseTransitionTime); an unchanged state keeps the stored time. A CR
// that predates PhaseTransitionTime carries none; feedback then falls back to its own
// observation time so the change is still stamped later than the stored one.
func setClusterStateWithTransitionTime(ctx context.Context, clusterOrder *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster, state privatev1.ClusterState) {
	if remote.GetStatus().GetState() != state {
		remote.GetStatus().SetStateTransitionTime(copyPhaseTransitionTime(
			ctrllog.FromContext(ctx), clusterOrder.Status.PhaseTransitionTime, remote.GetStatus().GetStateTransitionTime()))
	}
	remote.GetStatus().SetState(state)
}

// setClusterDeletingStateWithTransitionTime forces the DELETING state on the delete path.
// The ClusterOrder CR may not have recorded Phase=Deleting yet when feedback first syncs a
// deletion; the CR's PhaseTransitionTime then belongs to the previous phase (for example a
// READY stamped hours ago), and copying it would date DELETING at that old moment, billing
// the entire prior state's span as DELETING. When the CR has not recorded Deleting, the
// transition is dated at the CR's deletionTimestamp when present, else at the feedback
// observation time, so DELETING is stamped at or after the deletion began (see
// deletionTransitionTime). When the CR has recorded Deleting, its stamp is copied like the
// update path. The delete path does not map the CR phase to a state at all: the state is
// DELETING regardless of the phase the CR still carries, so no intermediate state is
// stamped in memory within one pass.
func setClusterDeletingStateWithTransitionTime(ctx context.Context, clusterOrder *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) {
	state := privatev1.ClusterState_CLUSTER_STATE_DELETING
	if remote.GetStatus().GetState() != state {
		log := ctrllog.FromContext(ctx)
		var stamp *timestamppb.Timestamp
		if clusterOrder.Status.Phase == ckv1alpha1.ClusterOrderPhaseDeleting {
			stamp = copyPhaseTransitionTime(log, clusterOrder.Status.PhaseTransitionTime, remote.GetStatus().GetStateTransitionTime())
		} else {
			stamp = deletionTransitionTime(log, clusterOrder, remote.GetStatus().GetStateTransitionTime())
		}
		remote.GetStatus().SetStateTransitionTime(stamp)
	}
	remote.GetStatus().SetState(state)
}

// deletionTransitionTime dates a deletion that the ClusterOrder CR has not yet recorded as
// Phase=Deleting. The CR's deletionTimestamp, when present, is when the deletion began;
// otherwise the feedback observation time is used. The result is never earlier than what
// the metering source contract accepts: a time not later than the stored transition time
// is bumped by one microsecond, mirroring copyPhaseTransitionTime.
func deletionTransitionTime(log logr.Logger, clusterOrder *ckv1alpha1.ClusterOrder, stored *timestamppb.Timestamp) *timestamppb.Timestamp {
	var observed *timestamppb.Timestamp
	if clusterOrder.DeletionTimestamp != nil && !clusterOrder.DeletionTimestamp.IsZero() {
		observed = timestamppb.New(clusterOrder.DeletionTimestamp.Time)
	} else {
		observed = timestamppb.Now()
	}
	if stored == nil || observed.AsTime().After(stored.AsTime()) {
		return observed
	}
	log.Info("deletion transition time is not later than the stored one, bumping it by one microsecond",
		"deletionTransitionTime", observed.AsTime(), "storedTransitionTime", stored.AsTime())
	return timestamppb.New(stored.AsTime().Add(time.Microsecond))
}

// clusterOrderConditionMappings maps a ClusterOrder condition to the fulfillment
// condition whose status it drives. Each fulfillment condition has exactly one source
// condition, so the derived status never depends on the order of the conditions. The
// fulfillment API has a single PROGRESSING condition, and only "Progressing" drives its
// status (True while the cluster is still being installed, False once it is ready or has
// failed); "ClusterAvailable" drives READY only when the overall ClusterOrder
// phase is Ready.
//
// The PROGRESSING condition's *reason* and *message* are refined separately, from the
// furthest-advanced installation stage, by applyProgressingStageDetail below.
var clusterOrderConditionMappings = map[string]privatev1.ClusterConditionType{
	ckv1alpha1.ConditionProgressing:      privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_PROGRESSING,
	ckv1alpha1.ConditionClusterAvailable: privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_READY,
}

// clusterOrderProvisioningStages are the ClusterOrder conditions that mark individual
// installation steps, ordered from earliest to furthest-advanced. While the cluster is
// still installing, they do not become their own fulfillment conditions; instead they
// refine the single PROGRESSING condition's reason/message to the furthest step reached
// (see applyProgressingStageDetail). Selecting the furthest stage from this fixed order,
// rather than from the order the conditions happen to appear in the CR status, keeps the
// result deterministic (order-independent).
//
// ControlPlaneAvailable also refines PROGRESSING today; in Epic 2 it additionally gains
// its own orthogonal CONTROL_PLANE_AVAILABLE fulfillment condition. ClusterStorageReady
// is written by the storage controller, which uses the typed
// ClusterOrderConditionClusterStorageReady constant, so we key off that same constant to
// avoid reader/writer drift.
var clusterOrderProvisioningStages = []string{
	ckv1alpha1.ConditionAccepted,
	ckv1alpha1.ConditionControlPlaneCreated,
	ckv1alpha1.ConditionControlPlaneAvailable,
	string(ckv1alpha1.ClusterOrderConditionClusterStorageReady),
}

// clusterOrderDegradedConditionSources are conditions that contribute to the
// fulfillment DEGRADED condition without becoming standalone conditions.
var clusterOrderDegradedConditionSources = map[string]struct{}{
	string(ckv1alpha1.ClusterOrderConditionAddOnOperatorsReady): {},
}

// clusterOrderUnsurfacedConditions are ClusterOrder conditions we know about but neither
// copy to the fulfillment API nor use to refine PROGRESSING. They are listed so they are
// not reported as unknown:
//   - NamespaceCreated is internal bookkeeping with no tenant-facing meaning.
//   - Deleting is reported through the DELETING state (see syncClusterOrderPhase and
//     syncClusterOrderDelete), not as a condition.
var clusterOrderUnsurfacedConditions = map[string]struct{}{
	ckv1alpha1.ConditionNamespaceCreated:                               {},
	ckv1alpha1.ConditionDeleting:                                       {},
	string(ckv1alpha1.ClusterOrderConditionFulfillmentTrustReady):      {},
	string(ckv1alpha1.ClusterOrderConditionManagementPublicationReady): {},
	string(ckv1alpha1.ClusterOrderConditionEndpointsReady):             {},
}

func syncClusterOrderConditions(ctx context.Context, clusterOrder *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) {
	log := ctrllog.FromContext(ctx)
	addOnConditionSeen := false

	for i := range clusterOrder.Status.Conditions {
		condition := clusterOrder.Status.Conditions[i]
		if protoType, ok := clusterOrderConditionMappings[condition.Type]; ok {
			if condition.Type == ckv1alpha1.ConditionClusterAvailable &&
				clusterOrder.Status.Phase != ckv1alpha1.ClusterOrderPhaseReady {
				continue
			}
			syncClusterConditionFromCR(remote, protoType, condition)
			continue
		}
		if slices.Contains(clusterOrderProvisioningStages, condition.Type) {
			// An installation-step condition: it refines PROGRESSING's reason/message
			// (handled by applyProgressingStageDetail after this loop), not its own
			// fulfillment condition.
			continue
		}
		if _, ok := clusterOrderDegradedConditionSources[condition.Type]; ok {
			addOnConditionSeen = true
			syncClusterOrderAddOnDegraded(remote, condition)
			continue
		}
		if _, ok := clusterOrderUnsurfacedConditions[condition.Type]; ok {
			continue
		}
		// A condition we do not recognise: log it so a newly added ClusterOrder condition
		// is noticed instead of being silently ignored.
		log.Info("Unmapped ClusterOrder condition, will ignore it", "condition", condition.Type)
	}
	if !addOnConditionSeen {
		// An absent add-on condition means the add-on controller has no current
		// failure to report. Clear only degradation owned by that controller.
		syncClusterOrderAddOnDegraded(remote, metav1.Condition{Status: metav1.ConditionTrue})
	}

	applyProgressingStageDetail(clusterOrder, remote)
}

func syncClusterOrderAddOnDegraded(remote *privatev1.Cluster, condition metav1.Condition) {
	if condition.Status == metav1.ConditionTrue {
		var degraded *privatev1.ClusterCondition
		for _, current := range remote.GetStatus().GetConditions() {
			if current.GetType() == privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_DEGRADED {
				degraded = current
				break
			}
		}
		if degraded == nil || degraded.GetStatus() != privatev1.ConditionStatus_CONDITION_STATUS_TRUE || degraded.GetReason() != addOnOperatorsFailedReason {
			return
		}
		degraded.SetStatus(privatev1.ConditionStatus_CONDITION_STATUS_FALSE)
		degraded.SetReason("AddOnOperatorsRecovered")
		degraded.SetMessage("")
		degraded.SetLastTransitionTime(timestamppb.Now())
		return
	}
	if condition.Status != metav1.ConditionFalse {
		return
	}
	degraded := findClusterCondition(remote, privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_DEGRADED)
	if degraded.GetStatus() == privatev1.ConditionStatus_CONDITION_STATUS_TRUE && degraded.GetReason() != addOnOperatorsFailedReason {
		return
	}
	oldStatus := degraded.GetStatus()
	degraded.SetStatus(privatev1.ConditionStatus_CONDITION_STATUS_TRUE)
	degraded.SetReason(addOnOperatorsFailedReason)
	degraded.SetMessage(sanitizeFeedbackText(condition.Message))
	if oldStatus != degraded.GetStatus() {
		degraded.SetLastTransitionTime(timestamppb.Now())
	}
}

// applyProgressingStageDetail refines the PROGRESSING condition's reason and message to
// the furthest-advanced installation stage that has been reached, while leaving its
// status untouched (the status is single-sourced from "Progressing" in the loop above).
// This only applies while PROGRESSING is True (installation underway). Once the cluster
// is ready or has failed, PROGRESSING is False and keeps the terminal reason/message that
// "Progressing" itself carried, rather than a mid-installation stage.
//
// The reason is read from the CR's Progressing condition (set by the resource controller
// to a sub-stage like PreparingInfrastructure or WorkersJoining). When at least one
// provisioning stage condition is True and the CR's Progressing reason is non-empty, that
// reason and its humanized form are forwarded to the proto PROGRESSING condition.
func applyProgressingStageDetail(clusterOrder *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) {
	var progressing *privatev1.ClusterCondition
	for _, current := range remote.Status.Conditions {
		if current.Type == privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_PROGRESSING {
			progressing = current
			break
		}
	}
	if progressing == nil || progressing.GetStatus() != privatev1.ConditionStatus_CONDITION_STATUS_TRUE {
		return
	}

	trueConditions := trueConditionTypes(clusterOrder)
	hasStage := false
	for _, stage := range clusterOrderProvisioningStages {
		if _, ok := trueConditions[stage]; ok {
			hasStage = true
		}
	}
	if !hasStage {
		return
	}

	crProgressing := apimeta.FindStatusCondition(clusterOrder.Status.Conditions, ckv1alpha1.ConditionProgressing)
	if crProgressing != nil && crProgressing.Reason != "" {
		progressing.SetReason(crProgressing.Reason)
		message := crProgressing.Message
		if message == "" {
			message = humanizeConditionName(crProgressing.Reason)
		}
		progressing.SetMessage(message)
	}
}

// trueConditionTypes returns the set of ClusterOrder condition types whose status is
// True.
func trueConditionTypes(clusterOrder *ckv1alpha1.ClusterOrder) map[string]struct{} {
	trueConditions := map[string]struct{}{}
	for i := range clusterOrder.Status.Conditions {
		condition := clusterOrder.Status.Conditions[i]
		if condition.Status == metav1.ConditionTrue {
			trueConditions[condition.Type] = struct{}{}
		}
	}
	return trueConditions
}

// humanizeConditionName turns a PascalCase condition name into space-separated words for
// a human-readable message, e.g. "ControlPlaneCreated" -> "Control Plane Created".
// Multi-letter acronyms are kept intact, e.g. "CSIDriverReady" -> "CSI Driver Ready" and
// "EnableTLS" -> "Enable TLS", so a space is only inserted at a genuine word boundary.
//
// Version-suffixed acronyms such as "IPv4"/"IPv6" are not handled: the trailing lowercase
// of the suffix is indistinguishable from an acronym-to-word boundary with single-rune
// lookahead, so "IPv4Ready" splits as "I Pv4 Ready". No ClusterOrder condition name uses
// that form today; add explicit handling here if one is ever introduced.
func humanizeConditionName(name string) string {
	runes := []rune(name)
	var builder strings.Builder
	for index, runeValue := range runes {
		if index > 0 && unicode.IsUpper(runeValue) {
			previous := runes[index-1]
			// A new word starts when an uppercase rune follows a lowercase rune or a
			// digit (e.g. the "P" in "ControlPlane"), or when an uppercase rune ends a
			// run of uppercase letters that begins the next word, i.e. the following
			// rune is lowercase (e.g. the "D" in "CSIDriver"). Both checks leave a run
			// of uppercase letters such as "CSI" or "TLS" unsplit.
			startsWord := unicode.IsLower(previous) || unicode.IsDigit(previous)
			endsAcronym := unicode.IsUpper(previous) && index+1 < len(runes) && unicode.IsLower(runes[index+1])
			if startsWord || endsAcronym {
				builder.WriteRune(' ')
			}
		}
		builder.WriteRune(runeValue)
	}
	return builder.String()
}

func syncClusterOrderWorkerConditions(obj *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) {
	workersFailed := apimeta.FindStatusCondition(obj.Status.Conditions, ckv1alpha1.ConditionWorkersFailed)

	failedStatus := privatev1.ConditionStatus_CONDITION_STATUS_FALSE
	failedMessage := ""
	if workersFailed != nil && workersFailed.Status == metav1.ConditionTrue {
		desired := int32(0)
		if obj.Status.DesiredWorkers != nil {
			desired = *obj.Status.DesiredWorkers
		}
		failedStatus = privatev1.ConditionStatus_CONDITION_STATUS_TRUE
		failedMessage = buildWorkerFailedMessage(obj.Status.Workers, desired)
	}
	setWorkerCondition(remote, privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_WORKER_PROVISIONING_FAILED, failedStatus, failedMessage)

	infraEnvReady := apimeta.FindStatusCondition(obj.Status.Conditions, ckv1alpha1.ConditionInfraEnvReady)
	rhcosNotFound := apimeta.FindStatusCondition(obj.Status.Conditions, ckv1alpha1.ConditionRHCOSImageNotFound)

	blocked := (infraEnvReady != nil && infraEnvReady.Status == metav1.ConditionFalse) ||
		(rhcosNotFound != nil && rhcosNotFound.Status == metav1.ConditionTrue)

	blockedStatus := privatev1.ConditionStatus_CONDITION_STATUS_FALSE
	blockedMessage := ""
	if blocked {
		blockedStatus = privatev1.ConditionStatus_CONDITION_STATUS_TRUE
		blockedMessage = "Worker provisioning is blocked — infrastructure issue requires Cloud Infrastructure Admin intervention"
	}
	setWorkerCondition(remote, privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_WORKER_PROVISIONING_BLOCKED, blockedStatus, blockedMessage)
}

// setWorkerCondition reports a worker problem condition on the remote. It creates the
// condition only when reporting a problem (status TRUE); when the computed status is the
// default FALSE it only clears an already-present condition, and does not append a fresh
// "no problem" condition. This keeps the sync idempotent — reconciling with no worker
// problems must not mutate the remote and trigger a needless Save.
func setWorkerCondition(remote *privatev1.Cluster, condType privatev1.ClusterConditionType, status privatev1.ConditionStatus, message string) {
	if status != privatev1.ConditionStatus_CONDITION_STATUS_TRUE && findExistingClusterCondition(remote, condType) == nil {
		return
	}
	setClusterCondition(remote, condType, status, message)
}

func buildWorkerFailedMessage(workers []ckv1alpha1.WorkerStatus, desired int32) string {
	var failed int32
	for i := range workers {
		if workers[i].Phase == "Failed" {
			failed++
		}
	}
	msg := fmt.Sprintf("%d of %d worker nodes failed to provision", failed, desired)
	if failed > 0 {
		msg += "; " + baremetalworker.FormatWorkersFailed(workers)
	}
	return msg
}

func syncClusterConditionFromCR(remote *privatev1.Cluster, condType privatev1.ClusterConditionType, condition metav1.Condition) {
	clusterCondition := findClusterCondition(remote, condType)
	oldStatus := clusterCondition.GetStatus()
	newStatus := mapClusterConditionStatus(condition.Status)
	clusterCondition.SetStatus(newStatus)
	clusterCondition.SetReason(condition.Reason)
	clusterCondition.SetMessage(sanitizeFeedbackText(condition.Message))
	if newStatus != oldStatus {
		clusterCondition.SetLastTransitionTime(timestamppb.Now())
	}
}

func setClusterCondition(remote *privatev1.Cluster, condType privatev1.ClusterConditionType, status privatev1.ConditionStatus, message string) {
	cond := findClusterCondition(remote, condType)
	oldStatus := cond.GetStatus()
	cond.SetStatus(status)
	cond.SetMessage(message)
	if status != oldStatus {
		cond.SetLastTransitionTime(timestamppb.Now())
	}
}

func mapClusterConditionStatus(status metav1.ConditionStatus) privatev1.ConditionStatus {
	switch status {
	case metav1.ConditionFalse:
		return privatev1.ConditionStatus_CONDITION_STATUS_FALSE
	case metav1.ConditionTrue:
		return privatev1.ConditionStatus_CONDITION_STATUS_TRUE
	default:
		return privatev1.ConditionStatus_CONDITION_STATUS_UNSPECIFIED
	}
}

// syncClusterOrderPhase maps the ClusterOrder phase to the fulfillment state. The transition
// time is the CR's recorded phase transition time (PhaseTransitionTime), not the moment
// feedback observed the change: feedback can run long after the phase moved, and those hours
// belong to the state that was actually current. The metering source contract rejects a state
// change whose status.state_transition_time is not later than the stored one. An unchanged
// state keeps the stored time.
func syncClusterOrderPhase(ctx context.Context, clusterOrder *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) {
	log := ctrllog.FromContext(ctx)
	var state privatev1.ClusterState
	knownPhase := true
	switch clusterOrder.Status.Phase {
	case ckv1alpha1.ClusterOrderPhaseProgressing:
		state = privatev1.ClusterState_CLUSTER_STATE_PROGRESSING
	case ckv1alpha1.ClusterOrderPhaseFailed:
		state = privatev1.ClusterState_CLUSTER_STATE_FAILED
	case ckv1alpha1.ClusterOrderPhaseReady:
		state = privatev1.ClusterState_CLUSTER_STATE_READY
	case ckv1alpha1.ClusterOrderPhaseDeleting:
		state = privatev1.ClusterState_CLUSTER_STATE_DELETING
	default:
		knownPhase = false
		log.Info("Unknown phase, will ignore it", "phase", clusterOrder.Status.Phase)
	}
	if knownPhase {
		setClusterStateWithTransitionTime(ctx, clusterOrder, remote, state)
	}
	clearClusterReadyConditionWhenNotReady(clusterOrder, remote)
}

// clearClusterReadyConditionWhenNotReady drops the fulfillment READY condition once the
// ClusterOrder phase is no longer Ready.
func clearClusterReadyConditionWhenNotReady(clusterOrder *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) {
	if clusterOrder.Status.Phase != ckv1alpha1.ClusterOrderPhaseReady {
		clearClusterReadyCondition(remote)
	}
}

// clearClusterReadyCondition drops the fulfillment READY condition, whatever the ClusterOrder
// phase reports: the reported condition must agree with the reported state, and the delete
// path forces DELETING while the CR may still carry Phase=Ready.
func clearClusterReadyCondition(remote *privatev1.Cluster) {
	if ready := findExistingClusterCondition(remote,
		privatev1.ClusterConditionType_CLUSTER_CONDITION_TYPE_READY); ready != nil {
		oldStatus := ready.GetStatus()
		ready.SetStatus(privatev1.ConditionStatus_CONDITION_STATUS_FALSE)
		ready.SetReason("")
		ready.SetMessage("")
		if oldStatus != privatev1.ConditionStatus_CONDITION_STATUS_FALSE {
			ready.SetLastTransitionTime(timestamppb.Now())
		}
	}
}

// syncClusterOrderURLs fetches the HostedCluster and populates API/console URLs
// on the Ready phase. Only called on the update path.
func syncClusterOrderURLs(ctx context.Context, hubClient clnt.Client, clusterOrder *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) error {
	if clusterOrder.Status.Phase != ckv1alpha1.ClusterOrderPhaseReady {
		return nil
	}

	hostedCluster, err := fetchHostedCluster(ctx, hubClient, clusterOrder)
	if err != nil {
		return err
	}
	if hostedCluster == nil {
		return nil
	}

	clusterStatus := remote.GetStatus()
	apiURL := clusterOrder.Status.APIURL
	if apiURL == "" {
		apiURL = calculateAPIURL(hostedCluster)
	}
	if apiURL != "" {
		clusterStatus.SetApiUrl(apiURL)
	}
	consoleURL := clusterOrder.Status.ConsoleURL
	if consoleURL == "" {
		consoleURL = calculateConsoleURL(hostedCluster)
	}
	if consoleURL != "" {
		clusterStatus.SetConsoleUrl(consoleURL)
	}
	return nil
}

func syncClusterOrderNodeRequests(ctx context.Context, clusterOrder *ckv1alpha1.ClusterOrder, remote *privatev1.Cluster) {
	log := ctrllog.FromContext(ctx)
	for i := range len(clusterOrder.Status.NodeRequests) {
		nodeRequest := &clusterOrder.Status.NodeRequests[i]
		if nodeRequest.BareMetal == nil || nodeRequest.BareMetal.InstanceType == "" {
			log.Error(nil, "Node request missing bareMetal.instanceType")
			continue
		}
		instanceTypeName := nodeRequest.BareMetal.InstanceType

		nodeSetID := nodeRequest.NodeSet
		requestedNodeSet, found := remote.GetSpec().GetNodeSets()[nodeSetID]
		if !found || requestedNodeSet == nil {
			log.Error(nil, "Failed to find the requested node set", "node_set", nodeSetID)
			continue
		}
		instanceType := requestedNodeSet.GetBaremetalInstanceType()
		if instanceType.GetName() != instanceTypeName {
			log.Error(nil, "Node set instance type does not match", "node_set", nodeSetID, "instance_type", instanceTypeName)
			continue
		}

		nodeSets := remote.GetStatus().GetNodeSets()
		if nodeSets == nil {
			nodeSets = map[string]*privatev1.ClusterNodeSetStatus{}
			remote.GetStatus().SetNodeSets(nodeSets)
		}
		nodeSet := nodeSets[nodeSetID]
		if nodeSet == nil {
			nodeSet = privatev1.ClusterNodeSetStatus_builder{}.Build()
			nodeSets[nodeSetID] = nodeSet
		}
		nodeSet.SetBaremetalInstanceType(proto.Clone(instanceType).(*privatev1.BareMetalInstanceTypeReference))

		oldValue := nodeSet.GetSize()
		newValue := int32(nodeRequest.NumberOfNodes)
		if newValue != oldValue {
			log.Info("Updating node set size",
				"instance_type", instanceTypeName,
				"old_value", oldValue,
				"new_value", newValue,
			)
			nodeSet.SetSize(newValue)
		}
	}
}

func fetchHostedCluster(ctx context.Context, hubClient clnt.Client, clusterOrder *ckv1alpha1.ClusterOrder) (*hypershiftv1beta1.HostedCluster, error) {
	hostedClusterRef := clusterOrder.Status.ClusterReference
	if hostedClusterRef == nil || hostedClusterRef.Namespace == "" || hostedClusterRef.HostedClusterName == "" {
		return nil, nil
	}
	hostedCluster := &hypershiftv1beta1.HostedCluster{}
	err := hubClient.Get(ctx, clnt.ObjectKey{
		Namespace: hostedClusterRef.Namespace,
		Name:      hostedClusterRef.HostedClusterName,
	}, hostedCluster)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return hostedCluster, nil
}

func calculateAPIURL(hc *hypershiftv1beta1.HostedCluster) string {
	apiEndpoint := hc.Status.ControlPlaneEndpoint
	if apiEndpoint.Host == "" || apiEndpoint.Port == 0 {
		return ""
	}
	return fmt.Sprintf("https://%s:%d", apiEndpoint.Host, apiEndpoint.Port)
}

func calculateConsoleURL(hc *hypershiftv1beta1.HostedCluster) string {
	return fmt.Sprintf(
		"https://console-openshift-console.apps.%s.%s",
		hc.Name, hc.Spec.DNS.BaseDomain,
	)
}

// findExistingClusterCondition returns the condition of the given type if it is already
// present on the remote, or nil otherwise. Unlike findClusterCondition it does not append
// a new condition as a side effect.
func findExistingClusterCondition(remote *privatev1.Cluster, kind privatev1.ClusterConditionType) *privatev1.ClusterCondition {
	for _, current := range remote.Status.Conditions {
		if current.Type == kind {
			return current
		}
	}
	return nil
}

func findClusterCondition(remote *privatev1.Cluster, kind privatev1.ClusterConditionType) *privatev1.ClusterCondition {
	if existing := findExistingClusterCondition(remote, kind); existing != nil {
		return existing
	}
	condition := &privatev1.ClusterCondition{
		Type:   kind,
		Status: privatev1.ConditionStatus_CONDITION_STATUS_FALSE,
	}
	remote.Status.Conditions = append(remote.Status.Conditions, condition)
	return condition
}
