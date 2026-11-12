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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	configv1 "github.com/openshift/api/config/v1"
	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/osac-project/osac/osac-operator/api/v1alpha1"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
)

var _ = Describe("upgrade attempt correlation", func() {
	const image = "quay.io/ocp/release:4.18.0"
	newOrder := func() *v1alpha1.ClusterOrder {
		return &v1alpha1.ClusterOrder{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
				v1alpha1.UpgradeAttemptIDAnnotation: "cluster-id",
			}},
			Spec: v1alpha1.ClusterOrderSpec{ReleaseImage: image,
				NodeRequests: []v1alpha1.NodeRequest{
					{NodeSet: "compute", ReleaseImage: image},
					{NodeSet: "gpu", ReleaseImage: image},
				}},
			Status: v1alpha1.ClusterOrderStatus{NodeRequests: []v1alpha1.NodeRequestStatus{
				{NodeSet: "compute"}, {NodeSet: "gpu"},
			}},
		}
	}
	newPool := func(name, id string) hypershiftv1beta1.NodePool {
		pool := hypershiftv1beta1.NodePool{ObjectMeta: metav1.ObjectMeta{
			Generation:  2,
			Labels:      map[string]string{agentNodeSetLabel: name},
			Annotations: map[string]string{v1alpha1.UpgradeAttemptIDAnnotation: id},
		}}
		pool.Spec.Release.Image = image
		pool.Status.Version = "4.17.0"
		return pool
	}
	newControlPlane := func() *hypershiftv1beta1.HostedCluster {
		hc := &hypershiftv1beta1.HostedCluster{ObjectMeta: metav1.ObjectMeta{
			Generation: 3, Annotations: map[string]string{v1alpha1.UpgradeAttemptIDAnnotation: "cluster-id"},
		}}
		hc.Spec.Release.Image = image
		return hc
	}
	versionStatus := func(version string, state configv1.UpdateState) *hypershiftv1beta1.ClusterVersionStatus {
		return &hypershiftv1beta1.ClusterVersionStatus{
			ObservedGeneration: 1,
			Desired:            configv1.Release{Version: version, Image: "quay.io/ocp/release@sha256:digest"},
			History: []configv1.UpdateHistory{{
				Version: version, Image: "quay.io/ocp/release@sha256:digest", State: state,
			}},
		}
	}
	controlPlaneVersionStatus := func(version string, state configv1.UpdateState) hypershiftv1beta1.ControlPlaneVersionStatus {
		return hypershiftv1beta1.ControlPlaneVersionStatus{
			ObservedGeneration: 1,
			Desired:            configv1.Release{Version: version, Image: "quay.io/ocp/release@sha256:digest"},
			History: []hypershiftv1beta1.ControlPlaneUpdateHistory{{
				Version: version, Image: "quay.io/ocp/release@sha256:digest", State: state,
			}},
		}
	}
	healthyConditions := func() []metav1.Condition {
		return []metav1.Condition{
			{Type: string(hypershiftv1beta1.HostedClusterAvailable), Status: metav1.ConditionTrue, ObservedGeneration: 3},
			{Type: string(hypershiftv1beta1.HostedClusterDegraded), Status: metav1.ConditionFalse, ObservedGeneration: 3},
			{Type: string(hypershiftv1beta1.ClusterVersionSucceeding), Status: metav1.ConditionTrue, ObservedGeneration: 3},
			{Type: string(hypershiftv1beta1.ClusterVersionProgressing), Status: metav1.ConditionFalse, ObservedGeneration: 3},
		}
	}

	It("observes the control plane and all stamped node sets independently", func() {
		order := newOrder()
		hc := newControlPlane()
		hc.Status.Version = versionStatus("4.18.0", configv1.CompletedUpdate)
		hc.Status.ControlPlaneVersion = controlPlaneVersionStatus("4.18.0", configv1.CompletedUpdate)
		hc.Status.Conditions = healthyConditions()
		recordUpgradeObservations(order, hc, []hypershiftv1beta1.NodePool{
			newPool("compute", "cluster-id"), newPool("gpu", "cluster-id"),
		})
		Expect(order.Status.UpgradeObservation.AttemptID).To(Equal("cluster-id"))
		Expect(order.Status.UpgradeObservation.State).To(Equal(v1alpha1.UpgradeObservationSucceeded))
		Expect(order.Status.UpgradeObservation.Version).To(Equal("4.18.0"))
		Expect(order.Status.NodeRequests[0].UpgradeObservation.AttemptID).To(Equal("cluster-id"))
		Expect(order.Status.NodeRequests[1].UpgradeObservation.AttemptID).To(Equal("cluster-id"))
	})

	DescribeTable("reports control-plane progress from either partial history", func(controlPlanePartial bool) {
		order := newOrder()
		hc := newControlPlane()
		hc.Status.Version = versionStatus("4.17.0", configv1.CompletedUpdate)
		hc.Status.ControlPlaneVersion = controlPlaneVersionStatus("4.17.0", configv1.CompletedUpdate)
		if controlPlanePartial {
			hc.Status.ControlPlaneVersion = controlPlaneVersionStatus("4.18.0", configv1.PartialUpdate)
		} else {
			hc.Status.Version = versionStatus("4.18.0", configv1.PartialUpdate)
		}
		recordUpgradeObservations(order, hc, nil)
		Expect(order.Status.UpgradeObservation.AttemptID).To(Equal("cluster-id"))
		Expect(order.Status.UpgradeObservation.Version).To(Equal("4.18.0"))
		Expect(order.Status.UpgradeObservation.State).To(Equal(v1alpha1.UpgradeObservationProgressing))
	},
		Entry("controlPlaneVersion is partial", true),
		Entry("version is partial", false),
	)

	It("reports progress from either history when the two partial histories disagree on version", func() {
		order := newOrder()
		hc := newControlPlane()
		hc.Status.Version = versionStatus("4.18.0", configv1.PartialUpdate)
		hc.Status.ControlPlaneVersion = controlPlaneVersionStatus("4.19.0", configv1.PartialUpdate)
		recordUpgradeObservations(order, hc, nil)
		Expect(order.Status.UpgradeObservation.State).To(Equal(v1alpha1.UpgradeObservationProgressing))
		Expect(order.Status.UpgradeObservation.Version).To(Equal("4.19.0"))
	})

	It("requires both target histories and all current health conditions for control-plane success", func() {
		order := newOrder()
		hc := newControlPlane()
		hc.Status.Version = versionStatus("4.18.0", configv1.CompletedUpdate)
		hc.Status.ControlPlaneVersion = controlPlaneVersionStatus("4.18.0", configv1.CompletedUpdate)
		recordUpgradeObservations(order, hc, nil)
		Expect(order.Status.UpgradeObservation.State).NotTo(Equal(v1alpha1.UpgradeObservationSucceeded))
		hc.Status.Conditions = healthyConditions()
		recordUpgradeObservations(order, hc, nil)
		Expect(order.Status.UpgradeObservation.State).To(Equal(v1alpha1.UpgradeObservationSucceeded))
		Expect(order.Status.UpgradeObservation.Version).To(Equal("4.18.0"))
	})

	DescribeTable("does not report control-plane success with a missing or mismatched signal",
		func(change func(*hypershiftv1beta1.HostedCluster)) {
			order := newOrder()
			hc := newControlPlane()
			hc.Status.Version = versionStatus("4.18.0", configv1.CompletedUpdate)
			hc.Status.Conditions = healthyConditions()
			hc.Status.ControlPlaneVersion = controlPlaneVersionStatus("4.18.0", configv1.CompletedUpdate)
			change(hc)
			recordUpgradeObservations(order, hc, nil)
			Expect(order.Status.UpgradeObservation.State).NotTo(Equal(v1alpha1.UpgradeObservationSucceeded))
		},
		Entry("control-plane desired differs", func(hc *hypershiftv1beta1.HostedCluster) {
			hc.Status.ControlPlaneVersion.Desired.Version = "4.19.0"
		}),
		Entry("control-plane history differs", func(hc *hypershiftv1beta1.HostedCluster) {
			hc.Status.ControlPlaneVersion.History[0].Version = "4.19.0"
		}),
		Entry("control-plane history is partial", func(hc *hypershiftv1beta1.HostedCluster) {
			hc.Status.ControlPlaneVersion.History[0].State = configv1.PartialUpdate
		}),
		Entry("guest desired differs", func(hc *hypershiftv1beta1.HostedCluster) {
			hc.Status.Version.Desired.Version = "4.19.0"
		}),
		Entry("guest history differs", func(hc *hypershiftv1beta1.HostedCluster) {
			hc.Status.Version.History[0].Version = "4.19.0"
		}),
		Entry("guest history is partial", func(hc *hypershiftv1beta1.HostedCluster) {
			hc.Status.Version.History[0].State = configv1.PartialUpdate
		}),
		Entry("available is absent", func(hc *hypershiftv1beta1.HostedCluster) {
			hc.Status.Conditions = hc.Status.Conditions[1:]
		}),
		Entry("degraded is true", func(hc *hypershiftv1beta1.HostedCluster) {
			hc.Status.Conditions[1].Status = metav1.ConditionTrue
		}),
		Entry("succeeding is false", func(hc *hypershiftv1beta1.HostedCluster) {
			hc.Status.Conditions[2].Status = metav1.ConditionFalse
		}),
		Entry("progressing is true", func(hc *hypershiftv1beta1.HostedCluster) {
			hc.Status.Conditions[3].Status = metav1.ConditionTrue
		}),
		Entry("health is stale", func(hc *hypershiftv1beta1.HostedCluster) {
			hc.Status.Conditions[0].ObservedGeneration = 2
		}),
	)

	It("does not report a release-image condition as terminal failure", func() {
		order := newOrder()
		hc := newControlPlane()
		hc.Status.Conditions = []metav1.Condition{{Type: string(hypershiftv1beta1.ValidReleaseImage), Status: metav1.ConditionFalse,
			ObservedGeneration: 3, Reason: "UnsupportedReleaseImage"}}
		recordUpgradeObservations(order, hc, nil)
		Expect(order.Status.UpgradeObservation.State).To(BeEmpty())
	})

	It("does not start a new attempt from an older NodePool transition", func() {
		order := newOrder()
		pool := newPool("compute", "previous-id")
		baseline := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
		pool.Status.Conditions = []hypershiftv1beta1.NodePoolCondition{{
			Type: hypershiftv1beta1.NodePoolUpdatingVersionConditionType, Status: "True",
			LastTransitionTime: metav1.NewTime(baseline),
		}}
		recordUpgradeObservations(order, &hypershiftv1beta1.HostedCluster{}, []hypershiftv1beta1.NodePool{pool})
		Expect(order.Status.NodeRequests[0].UpgradeObservation.BaselineTransitionTime.Time).To(Equal(baseline))
		pool.Annotations[v1alpha1.UpgradeAttemptIDAnnotation] = "cluster-id"
		recordUpgradeObservations(order, &hypershiftv1beta1.HostedCluster{}, []hypershiftv1beta1.NodePool{pool})
		Expect(order.Status.NodeRequests[0].UpgradeObservation.State).To(BeEmpty())
		pool.Status.Conditions[0].LastTransitionTime = metav1.NewTime(baseline.Add(time.Second))
		recordUpgradeObservations(order, &hypershiftv1beta1.HostedCluster{}, []hypershiftv1beta1.NodePool{pool})
		Expect(order.Status.NodeRequests[0].UpgradeObservation.State).To(Equal(v1alpha1.UpgradeObservationProgressing))
	})

	It("reports node-pool progress when the current UpdatingVersion condition is true", func() {
		order := newOrder()
		pool := newPool("compute", "cluster-id")
		pool.Status.Conditions = []hypershiftv1beta1.NodePoolCondition{{
			Type:   hypershiftv1beta1.NodePoolUpdatingVersionConditionType,
			Status: corev1.ConditionTrue, ObservedGeneration: 2,
		}}
		recordUpgradeObservations(order, &hypershiftv1beta1.HostedCluster{}, []hypershiftv1beta1.NodePool{pool})
		Expect(order.Status.NodeRequests[0].UpgradeObservation.State).To(Equal(v1alpha1.UpgradeObservationProgressing))
		Expect(order.Status.NodeRequests[0].UpgradeObservation.Version).To(Equal("4.17.0"))
		pool.Status.Conditions[0].ObservedGeneration = 1
		recordUpgradeObservations(order, &hypershiftv1beta1.HostedCluster{}, []hypershiftv1beta1.NodePool{pool})
		Expect(order.Status.NodeRequests[0].UpgradeObservation.State).To(BeEmpty())
	})

	It("requires the current UpdatingVersion, AllMachinesReady, and Ready conditions for node-pool success", func() {
		order := newOrder()
		pool := newPool("compute", "cluster-id")
		pool.Status.Version = "4.18.0"
		pool.Status.Conditions = []hypershiftv1beta1.NodePoolCondition{
			{Type: hypershiftv1beta1.NodePoolUpdatingVersionConditionType, Status: corev1.ConditionFalse, ObservedGeneration: 2},
			{Type: hypershiftv1beta1.NodePoolReadyConditionType, Status: corev1.ConditionTrue, ObservedGeneration: 2},
			{Type: hypershiftv1beta1.NodePoolAllMachinesReadyConditionType, Status: corev1.ConditionTrue, ObservedGeneration: 2},
		}
		recordUpgradeObservations(order, &hypershiftv1beta1.HostedCluster{}, []hypershiftv1beta1.NodePool{pool})
		Expect(order.Status.NodeRequests[0].UpgradeObservation.State).To(Equal(v1alpha1.UpgradeObservationSucceeded))
		Expect(order.Status.NodeRequests[0].UpgradeObservation.ReadyAtVersion).To(BeTrue())
	})

	DescribeTable("does not report node-pool success without each current signal", func(change func(*hypershiftv1beta1.NodePool)) {
		order := newOrder()
		pool := newPool("compute", "cluster-id")
		pool.Status.Version = "4.18.0"
		pool.Status.Conditions = []hypershiftv1beta1.NodePoolCondition{
			{Type: hypershiftv1beta1.NodePoolUpdatingVersionConditionType, Status: corev1.ConditionFalse, ObservedGeneration: 2},
			{Type: hypershiftv1beta1.NodePoolAllMachinesReadyConditionType, Status: corev1.ConditionTrue, ObservedGeneration: 2},
			{Type: hypershiftv1beta1.NodePoolReadyConditionType, Status: corev1.ConditionTrue, ObservedGeneration: 2},
		}
		change(&pool)
		recordUpgradeObservations(order, &hypershiftv1beta1.HostedCluster{}, []hypershiftv1beta1.NodePool{pool})
		Expect(order.Status.NodeRequests[0].UpgradeObservation.State).NotTo(Equal(v1alpha1.UpgradeObservationSucceeded))
	},
		Entry("status version is absent", func(pool *hypershiftv1beta1.NodePool) { pool.Status.Version = "" }),
		Entry("UpdatingVersion is absent", func(pool *hypershiftv1beta1.NodePool) {
			pool.Status.Conditions = pool.Status.Conditions[1:]
		}),
		Entry("UpdatingVersion is true", func(pool *hypershiftv1beta1.NodePool) {
			pool.Status.Conditions[0].Status = corev1.ConditionTrue
		}),
		Entry("AllMachinesReady is false", func(pool *hypershiftv1beta1.NodePool) {
			pool.Status.Conditions[1].Status = corev1.ConditionFalse
		}),
		Entry("Ready is false", func(pool *hypershiftv1beta1.NodePool) {
			pool.Status.Conditions[2].Status = corev1.ConditionFalse
		}),
		Entry("Ready is stale", func(pool *hypershiftv1beta1.NodePool) {
			pool.Status.Conditions[2].ObservedGeneration = 1
		}),
	)

	It("does not report a NodePool release condition as terminal failure", func() {
		order := newOrder()
		pool := newPool("compute", "cluster-id")
		pool.Status.Conditions = []hypershiftv1beta1.NodePoolCondition{{
			Type:   hypershiftv1beta1.NodePoolValidReleaseImageConditionType,
			Status: corev1.ConditionFalse, ObservedGeneration: 2, Reason: "ValidationFailed",
		}}
		recordUpgradeObservations(order, &hypershiftv1beta1.HostedCluster{}, []hypershiftv1beta1.NodePool{pool})
		Expect(order.Status.NodeRequests[0].UpgradeObservation.State).To(BeEmpty())
	})

	newControlPlaneState := func(state privatev1.ClusterUpgradeState, accepted, transition time.Time,
	) *privatev1.ClusterUpgradeControlPlaneState {
		return privatev1.ClusterUpgradeControlPlaneState_builder{
			State:               state,
			AcceptedTime:        timestamppb.New(accepted),
			StateTransitionTime: timestamppb.New(transition),
		}.Build()
	}
	newNodeSetState := func(name string, state privatev1.ClusterUpgradeState,
		accepted, transition time.Time, nodes int32,
	) *privatev1.ClusterUpgradeNodeSetState {
		return privatev1.ClusterUpgradeNodeSetState_builder{
			NodeSetId: name, Scheduled: true, State: state,
			AcceptedTime:        timestamppb.New(accepted),
			StateTransitionTime: timestamppb.New(transition),
			TimeoutNodeCount:    nodes,
		}.Build()
	}
	newHistoryEntry := func(attemptID, targetVersion string,
		state privatev1.ClusterUpgradeState, accepted, transition time.Time,
		controlPlane *privatev1.ClusterUpgradeControlPlaneState,
		nodeSets ...*privatev1.ClusterUpgradeNodeSetState,
	) *privatev1.ClusterUpgradeHistoryEntry {
		return privatev1.ClusterUpgradeHistoryEntry_builder{
			AttemptId:           attemptID,
			TargetVersion:       targetVersion,
			State:               state,
			AcceptedTime:        timestamppb.New(accepted),
			StateTransitionTime: timestamppb.New(transition),
			ControlPlane:        controlPlane,
			NodeSets:            nodeSets,
		}.Build()
	}
	It("does not promote failed feedback with a stale readiness bit", func() {
		now := time.Now()
		componentState := newControlPlaneState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now)
		entry := newHistoryEntry("cluster-id", "4.18.0",
			privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now, componentState)
		remote := privatev1.Cluster_builder{
			Status: privatev1.ClusterStatus_builder{
				UpgradeAttemptId: "cluster-id",
				UpgradeHistory:   []*privatev1.ClusterUpgradeHistoryEntry{entry},
			}.Build(),
		}.Build()
		observation := &v1alpha1.UpgradeObservation{
			AttemptID: "cluster-id", State: v1alpha1.UpgradeObservationFailed, Version: "4.18.0",
			ReadyAtVersion: true, Message: "Release image was rejected",
		}
		syncControlPlaneUpgradeState(entry, componentState, observation, now)
		aggregateClusterUpgradeEntry(remote, entry, now)
		Expect(entry.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_FAILED))
		Expect(entry.GetMessage()).To(Equal(observation.Message))
	})

	It("requires the target version for control-plane progress and success attribution", func() {
		now := time.Now()
		componentState := newControlPlaneState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now)
		entry := newHistoryEntry("cluster-id", "4.18.0",
			privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now, componentState)
		remote := privatev1.Cluster_builder{
			Status: privatev1.ClusterStatus_builder{
				UpgradeAttemptId: "cluster-id",
				UpgradeHistory:   []*privatev1.ClusterUpgradeHistoryEntry{entry},
			}.Build(),
		}.Build()
		observation := &v1alpha1.UpgradeObservation{
			AttemptID: "cluster-id", State: v1alpha1.UpgradeObservationProgressing, Version: "4.19.0",
		}
		syncControlPlaneUpgradeState(entry, componentState, observation, now)
		Expect(componentState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING))
		observation.Version = "4.18.0"
		syncControlPlaneUpgradeState(entry, componentState, observation, now)
		Expect(componentState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING))
		observation.State = v1alpha1.UpgradeObservationSucceeded
		observation.Version = "4.19.0"
		syncControlPlaneUpgradeState(entry, componentState, observation, now)
		Expect(componentState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING))
		_ = remote
	})

	It("does not infer node-pool progress from a target status version alone", func() {
		now := time.Now()
		controlPlane := newControlPlaneState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED, now, now)
		componentState := newNodeSetState("compute", privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now, 2)
		entry := newHistoryEntry("cluster-id", "4.18.0",
			privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING, now, now, controlPlane, componentState)
		observation := &v1alpha1.UpgradeObservation{AttemptID: "cluster-id", Version: "4.18.0"}
		syncNodeSetUpgradeState(entry, componentState, observation, now)
		Expect(componentState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING))
		observation.State = v1alpha1.UpgradeObservationProgressing
		observation.Version = "4.17.0"
		syncNodeSetUpgradeState(entry, componentState, observation, now)
		Expect(componentState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING))
	})

	It("updates every component independently and aggregates the whole attempt", func() {
		order := newOrder()
		now := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
		order.Status.UpgradeObservation = &v1alpha1.UpgradeObservation{
			AttemptID: "shared-id", State: v1alpha1.UpgradeObservationSucceeded, Version: "4.18.0"}
		order.Status.NodeRequests[0].UpgradeObservation = &v1alpha1.UpgradeObservation{
			AttemptID: "shared-id", State: v1alpha1.UpgradeObservationFailed, Message: "Compute upgrade failed"}
		order.Status.NodeRequests[1].UpgradeObservation = &v1alpha1.UpgradeObservation{
			AttemptID: "shared-id", State: v1alpha1.UpgradeObservationProgressing}
		cpState := newControlPlaneState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now)
		computeState := newNodeSetState("compute", privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now, 2)
		gpuState := newNodeSetState("gpu", privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now, 3)
		entry := newHistoryEntry("shared-id", "4.18.0",
			privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now,
			cpState, computeState, gpuState)
		remote := privatev1.Cluster_builder{
			Spec: privatev1.ClusterSpec_builder{NodeSets: map[string]*privatev1.ClusterNodeSet{
				"compute": {}, "gpu": {},
			}}.Build(),
			Status: privatev1.ClusterStatus_builder{
				UpgradeAttemptId: "shared-id",
				UpgradeHistory:   []*privatev1.ClusterUpgradeHistoryEntry{entry},
			}.Build(),
		}.Build()

		syncClusterOrderUpgradeObservationsAt(order, remote, now)
		Expect(cpState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED))
		Expect(computeState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_FAILED))
		Expect(computeState.GetMessage()).To(Equal("Compute upgrade failed"))
		Expect(gpuState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING))
		Expect(entry.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_FAILED))
	})

	It("keeps the whole attempt open until every participating node set is scheduled", func() {
		now := time.Now()
		cpState := newControlPlaneState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED, now, now)
		computeState := newNodeSetState("compute", privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED, now, now, 2)
		gpuState := newNodeSetState("gpu", privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED, now, now, 3)
		computeState.SetScheduled(false)
		gpuState.SetScheduled(false)
		entry := newHistoryEntry("cluster-id", "4.18.0",
			privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING, now, now, cpState,
			computeState, gpuState)
		remote := privatev1.Cluster_builder{
			Spec: privatev1.ClusterSpec_builder{NodeSets: map[string]*privatev1.ClusterNodeSet{
				"compute": {}, "gpu": {},
			}}.Build(),
			Status: privatev1.ClusterStatus_builder{
				UpgradeAttemptId: "cluster-id",
				UpgradeHistory:   []*privatev1.ClusterUpgradeHistoryEntry{entry},
			}.Build(),
		}.Build()

		aggregateClusterUpgradeEntry(remote, entry, now)
		Expect(entry.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING))

		computeState.SetScheduled(true)
		gpuState.SetScheduled(true)
		aggregateClusterUpgradeEntry(remote, entry, now)
		Expect(entry.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED))
	})
	It("times out the control plane forty minutes after Progressing", func() {
		order := newOrder()
		now := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
		cpState := newControlPlaneState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now)
		entry := newHistoryEntry("cluster-id", "4.18.0",
			privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now, cpState)
		remote := privatev1.Cluster_builder{
			Status: privatev1.ClusterStatus_builder{
				UpgradeAttemptId: "cluster-id",
				ObservedVersion:  "4.17.0",
				UpgradeHistory:   []*privatev1.ClusterUpgradeHistoryEntry{entry},
			}.Build(),
		}.Build()
		order.Status.UpgradeObservation = &v1alpha1.UpgradeObservation{
			AttemptID: "cluster-id", State: v1alpha1.UpgradeObservationProgressing, Version: "4.18.0",
		}
		syncClusterOrderUpgradeObservationsAt(order, remote, now.Add(5*time.Minute))
		Expect(cpState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING))
		Expect(entry.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING))
		syncClusterOrderUpgradeObservationsAt(order, remote, now.Add(44*time.Minute))
		Expect(cpState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING))
		syncClusterOrderUpgradeObservationsAt(order, remote, now.Add(45*time.Minute))
		Expect(cpState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT))
		Expect(entry.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT))
		Expect(entry.GetMessage()).To(ContainSubstring("Control plane"))
		Expect(remote.GetStatus().GetObservedVersion()).To(Equal("4.17.0"))
		order.Status.UpgradeObservation.State = v1alpha1.UpgradeObservationSucceeded
		order.Status.UpgradeObservation.Version = "4.18.0"
		syncClusterOrderUpgradeObservationsAt(order, remote, now.Add(46*time.Minute))
		Expect(cpState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED))
		Expect(entry.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED))
		Expect(remote.GetStatus().GetObservedVersion()).To(Equal("4.17.0"))
	})

	It("times out a pending node pool ten minutes after acceptance", func() {
		order := newOrder()
		now := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
		cpState := newControlPlaneState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED, now, now)
		nodeState := newNodeSetState("compute", privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now, 2)
		entry := newHistoryEntry("cluster-id", "4.18.0",
			privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING, now, now, cpState, nodeState)
		remote := privatev1.Cluster_builder{
			Spec: privatev1.ClusterSpec_builder{
				NodeSets: map[string]*privatev1.ClusterNodeSet{"compute": {}},
			}.Build(),
			Status: privatev1.ClusterStatus_builder{
				UpgradeAttemptId: "cluster-id",
				NodeSets:         map[string]*privatev1.ClusterNodeSetStatus{"compute": {}},
				UpgradeHistory:   []*privatev1.ClusterUpgradeHistoryEntry{entry},
			}.Build(),
		}.Build()
		syncClusterOrderUpgradeObservationsAt(order, remote, now.Add(9*time.Minute))
		Expect(nodeState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING))
		syncClusterOrderUpgradeObservationsAt(order, remote, now.Add(10*time.Minute))
		Expect(nodeState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT))
		Expect(entry.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT))
		Expect(entry.GetMessage()).To(ContainSubstring("Node set"))
		order.Status.NodeRequests[0].UpgradeObservation = &v1alpha1.UpgradeObservation{AttemptID: "cluster-id",
			State: v1alpha1.UpgradeObservationSucceeded, Version: "4.18.0"}
		syncClusterOrderUpgradeObservationsAt(order, remote, now.Add(11*time.Minute))
		Expect(nodeState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED))
		Expect(entry.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED))
		Expect(remote.GetStatus().GetNodeSets()["compute"].GetObservedVersion()).To(BeEmpty())
	})

	It("uses the configured per-node completion timeout", func() {
		now := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
		cpState := newControlPlaneState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED, now, now)
		nodeState := newNodeSetState("compute", privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING,
			now.Add(-time.Minute), now, 2)
		entry := newHistoryEntry("cluster-id", "4.18.0",
			privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING, now.Add(-time.Minute), now,
			cpState, nodeState)
		remote := privatev1.Cluster_builder{
			Spec: privatev1.ClusterSpec_builder{
				NodeSets: map[string]*privatev1.ClusterNodeSet{"compute": {}},
			}.Build(),
			Status: privatev1.ClusterStatus_builder{
				UpgradeAttemptId: "cluster-id",
				UpgradeHistory:   []*privatev1.ClusterUpgradeHistoryEntry{entry},
			}.Build(),
		}.Build()
		config := ClusterUpgradeTimeouts{Pending: 2 * time.Minute, ControlPlane: 5 * time.Minute, NodePoolPerNode: 3 * time.Minute}
		applyUpgradeTimeouts(remote, entry, now.Add(5*time.Minute), config)
		Expect(nodeState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING))
		applyUpgradeTimeouts(remote, entry, now.Add(6*time.Minute), config)
		Expect(nodeState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT))
		Expect(nodeState.GetMessage()).To(ContainSubstring("completion"))
	})

	It("uses the configured pending and control-plane completion timeouts", func() {
		now := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
		cpState := newControlPlaneState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now)
		entry := newHistoryEntry("cluster-id", "4.18.0",
			privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now, cpState)
		remote := privatev1.Cluster_builder{
			Status: privatev1.ClusterStatus_builder{
				UpgradeAttemptId: "cluster-id",
				UpgradeHistory:   []*privatev1.ClusterUpgradeHistoryEntry{entry},
			}.Build(),
		}.Build()
		config := ClusterUpgradeTimeouts{Pending: 2 * time.Minute, ControlPlane: 5 * time.Minute, NodePoolPerNode: 3 * time.Minute}
		applyUpgradeTimeouts(remote, entry, now.Add(2*time.Minute), config)
		Expect(cpState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT))
		Expect(cpState.GetMessage()).To(ContainSubstring("progress"))
		cpState.SetState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING)
		cpState.SetStateTransitionTime(timestamppb.New(now.Add(3 * time.Minute)))
		applyUpgradeTimeouts(remote, entry, now.Add(8*time.Minute), config)
		Expect(cpState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT))
		Expect(cpState.GetMessage()).To(ContainSubstring("completion"))
	})

	It("requeues while an open component is monitored even after a whole-attempt timeout", func() {
		now := time.Now()
		cpState := newControlPlaneState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now)
		entry := newHistoryEntry("cluster-id", "4.18.0",
			privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_TIMED_OUT, now, now, cpState)
		remote := privatev1.Cluster_builder{
			Spec: privatev1.ClusterSpec_builder{NodeSets: map[string]*privatev1.ClusterNodeSet{
				"compute": {},
			}}.Build(),
			Status: privatev1.ClusterStatus_builder{
				UpgradeAttemptId: "cluster-id",
				UpgradeHistory:   []*privatev1.ClusterUpgradeHistoryEntry{entry},
			}.Build(),
		}.Build()
		Expect(hasUpgradeAwaitingTimeout(remote)).To(BeTrue())
		cpState.SetState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED)
		Expect(hasUpgradeAwaitingTimeout(remote)).To(BeFalse())

		nodeState := newNodeSetState("compute", privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now, 2)
		entry.SetNodeSets([]*privatev1.ClusterUpgradeNodeSetState{nodeState})
		Expect(hasUpgradeAwaitingTimeout(remote)).To(BeTrue())
		nodeState.SetScheduled(true)
		Expect(hasUpgradeAwaitingTimeout(remote)).To(BeTrue())
		nodeState.SetState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED)
		Expect(hasUpgradeAwaitingTimeout(remote)).To(BeFalse())
	})

	It("ignores a removed NodeSet's live NodePool progress", func() {
		order := newOrder()
		order.Spec.NodeRequests = order.Spec.NodeRequests[:1]
		order.Annotations[v1alpha1.UpgradeAttemptIDAnnotation] = "new-cluster-id"
		pool := newPool("gpu", "cluster-id")
		pool.Status.Conditions = []hypershiftv1beta1.NodePoolCondition{{
			Type:   hypershiftv1beta1.NodePoolUpdatingVersionConditionType,
			Status: corev1.ConditionTrue, ObservedGeneration: pool.Generation,
		}}
		recordUpgradeObservations(order, &hypershiftv1beta1.HostedCluster{}, []hypershiftv1beta1.NodePool{pool})
		Expect(order.Status.NodeRequests[1].UpgradeObservation).To(BeNil())
	})

	It("keeps removed NodeSet history while it stops participating", func() {
		order := newOrder()
		order.Name = "order"
		order.Spec.NodeRequests = order.Spec.NodeRequests[:1]
		order.Status.ClusterReference = &v1alpha1.ClusterOrderClusterReferenceType{Namespace: "cluster-namespace"}
		pool := newPool("gpu", "cluster-id")
		pool.Name = "nodepool-order-gpu"
		pool.Namespace = "cluster-namespace"
		pool.Labels[osacClusterOrderNameLabel] = order.Name
		pool.Labels[agentInstanceTypeLabel] = "bm.gpu"
		pool.Status.Replicas = 1
		scheme := runtime.NewScheme()
		Expect(hypershiftv1beta1.AddToScheme(scheme)).To(Succeed())
		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&pool).Build()
		now := time.Now()
		cpState := newControlPlaneState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED, now, now)
		gpuState := newNodeSetState("gpu", privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING, now, now, 2)
		entry := newHistoryEntry("cluster-id", "4.18.0",
			privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PROGRESSING, now, now, cpState, gpuState)
		remote := privatev1.Cluster_builder{
			Spec: privatev1.ClusterSpec_builder{NodeSets: map[string]*privatev1.ClusterNodeSet{"compute": {}}}.Build(),
			Status: privatev1.ClusterStatus_builder{
				UpgradeAttemptId: "cluster-id",
				NodeSets:         map[string]*privatev1.ClusterNodeSetStatus{"compute": {}, "gpu": {}},
				UpgradeHistory:   []*privatev1.ClusterUpgradeHistoryEntry{entry},
			}.Build(),
		}.Build()
		aggregateClusterUpgradeEntry(remote, entry, time.Now())
		Expect(entry.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED))

		Expect(syncRemovedNodeSets(context.Background(), client, order, remote)).To(Succeed())
		Expect(remote.GetStatus().GetNodeSets()).To(HaveKey("gpu"))
		Expect(entry.GetNodeSets()).To(ContainElement(gpuState))
		Expect(gpuState.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING))

		Expect(client.Delete(context.Background(), &pool)).To(Succeed())
		Expect(syncRemovedNodeSets(context.Background(), client, order, remote)).To(Succeed())
		Expect(remote.GetStatus().GetNodeSets()).NotTo(HaveKey("gpu"))
		Expect(entry.GetNodeSets()).To(ContainElement(gpuState))
		Expect(entry.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED))
	})
})
