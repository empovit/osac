/*
Copyright (c) 2026 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package it

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2/dsl/core"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/osac-project/osac/fulfillment-service/internal/kubernetes/labels"
	"github.com/osac-project/osac/fulfillment-service/internal/uuid"
	osacv1alpha1 "github.com/osac-project/osac/osac-operator/api/v1alpha1"
	privatev1 "github.com/osac-project/osac/proto/gen/osac/private/v1"
	publicv1 "github.com/osac-project/osac/proto/gen/osac/public/v1"
)

var _ = Describe("Cluster upgrade", func() {
	var (
		ctx                  context.Context
		bmitName             string
		clustersClient       publicv1.ClustersClient
		privateClusters      privatev1.ClustersClient
		clusterVersions      privatev1.ClusterVersionsClient
		templatesClient      privatev1.ClusterTemplatesClient
		templateID           string
		targetVersionName    string
		targetSemver         string
		targetImage          string
		defaultImage         string
		defaultVersionName   string
		defaultVersionSemver string
	)

	BeforeEach(func() {
		ctx = context.Background()
		ensureTenantDefaultNetworkingFixture(ctx, usersGroup, "")
		clustersClient = publicv1.NewClustersClient(tool.ExternalView().UserConn())
		privateClusters = privatev1.NewClustersClient(tool.InternalView().AdminConn())
		clusterVersions = privatev1.NewClusterVersionsClient(tool.InternalView().AdminConn())
		templatesClient = privatev1.NewClusterTemplatesClient(tool.InternalView().AdminConn())

		bmitName = fmt.Sprintf("test-bmit-%s", uuid.New()[24:32])
		instanceTypes := privatev1.NewBareMetalInstanceTypesClient(tool.InternalView().AdminConn())
		_, err := instanceTypes.Create(ctx, privatev1.BareMetalInstanceTypesCreateRequest_builder{
			Object: privatev1.BareMetalInstanceType_builder{
				Metadata: privatev1.Metadata_builder{Name: bmitName}.Build(),
				Spec: privatev1.BareMetalInstanceTypeSpec_builder{
					HostLabelSelector: privatev1.BareMetalLabelSelector_builder{
						MatchLabels: map[string]string{"hardware.profile": "compute"},
					}.Build(),
					Hardware: privatev1.BareMetalHardwareSpec_builder{
						NetworkPorts: []*privatev1.BareMetalNetworkPortSpec{
							privatev1.BareMetalNetworkPortSpec_builder{
								Name: "eth0", Role: "fabric", Type: "Ethernet", Speed: "10Gbps",
							}.Build(),
						},
					}.Build(),
				}.Build(),
			}.Build(),
		}.Build())
		Expect(err).NotTo(HaveOccurred())

		templateID = newTemplateID("upgrade_template")
		_, err = templatesClient.Create(ctx, privatev1.ClusterTemplatesCreateRequest_builder{
			Object: privatev1.ClusterTemplate_builder{
				Id:          templateID,
				Title:       "Upgrade template",
				Description: "Upgrade template.",
				Metadata:    privatev1.Metadata_builder{Name: fmt.Sprintf("upgrade-tmpl-%s", uuid.New()[24:32])}.Build(),
			}.Build(),
		}.Build())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_, err := templatesClient.Delete(ctx, privatev1.ClusterTemplatesDeleteRequest_builder{Id: templateID}.Build())
			Expect(err).NotTo(HaveOccurred())
		})

		defaults, err := clusterVersions.List(ctx, privatev1.ClusterVersionsListRequest_builder{
			Filter: new(`this.spec.is_default == true`),
		}.Build())
		Expect(err).NotTo(HaveOccurred())
		Expect(defaults.GetItems()).To(HaveLen(1))
		defaultVersion := defaults.GetItems()[0]
		defaultVersionName = defaultVersion.GetMetadata().GetName()
		defaultVersionSemver = defaultVersion.GetSpec().GetVersion()
		defaultImage = defaultVersion.GetSpec().GetImage()

		targetSemver = nextCVVersion()
		targetVersionName = fmt.Sprintf("upgrade-cv-%s", uuid.New()[24:32])
		targetImage = fmt.Sprintf("quay.io/openshift-release-dev/ocp-release:%s-multi", targetSemver)
		diskImage := createCatalogItemDiskImageFixture(ctx, "shared", catalogItemFixtureName())
		targetVersion, err := clusterVersions.Create(ctx, privatev1.ClusterVersionsCreateRequest_builder{
			Object: privatev1.ClusterVersion_builder{
				Metadata: privatev1.Metadata_builder{Name: targetVersionName}.Build(),
				Spec: privatev1.ClusterVersionSpec_builder{
					Version:   targetSemver,
					Image:     targetImage,
					DiskImage: privatev1.DiskImageReference_builder{Id: diskImage.GetId()}.Build(),
				}.Build(),
			}.Build(),
		}.Build())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_, err := clusterVersions.Delete(ctx, privatev1.ClusterVersionsDeleteRequest_builder{
				Id: targetVersion.GetObject().GetId(),
			}.Build())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	It("projects node-set images only after control-plane success", func() {
		created, err := clustersClient.Create(ctx, publicv1.ClustersCreateRequest_builder{
			Object: publicv1.Cluster_builder{
				Metadata: publicv1.Metadata_builder{
					Name: fmt.Sprintf("upgrade-cluster-%s", uuid.New()[24:32]),
				}.Build(),
				Spec: publicv1.ClusterSpec_builder{
					Template: publicv1.ClusterTemplateReference_builder{Id: templateID}.Build(),
					NodeSets: testClusterNodeSets(bmitName, 1),
				}.Build(),
			}.Build(),
		}.Build())
		Expect(err).NotTo(HaveOccurred())
		cluster := created.GetObject()
		DeferCleanup(func() {
			_, err := clustersClient.Delete(ctx, publicv1.ClustersDeleteRequest_builder{Id: cluster.GetId()}.Build())
			Expect(err).NotTo(HaveOccurred())
		})

		kubeClient := tool.KubeClient()
		orders := &osacv1alpha1.ClusterOrderList{}
		var order *osacv1alpha1.ClusterOrder
		Eventually(func(g Gomega) {
			err := kubeClient.List(ctx, orders, crclient.MatchingLabels{labels.ClusterOrderUuid: cluster.GetId()})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(orders.Items).To(HaveLen(1))
			order = &orders.Items[0]
		}, time.Minute, time.Second).Should(Succeed())
		Expect(order.Spec.NodeRequests[0].ReleaseImage).To(Equal(defaultImage))

		_, err = clustersClient.Update(ctx, publicv1.ClustersUpdateRequest_builder{
			Object: publicv1.Cluster_builder{
				Id: cluster.GetId(),
				Spec: publicv1.ClusterSpec_builder{
					Version: publicv1.ClusterVersionReference_builder{Name: targetVersionName}.Build(),
				}.Build(),
			}.Build(),
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.version"}},
		}.Build())
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			updated := &osacv1alpha1.ClusterOrder{}
			g.Expect(kubeClient.Get(ctx, crclient.ObjectKeyFromObject(order), updated)).To(Succeed())
			g.Expect(updated.Spec.ReleaseImage).To(Equal(targetImage))
			g.Expect(updated.Spec.NodeRequests[0].ReleaseImage).To(Equal(defaultImage))
		}, time.Minute, time.Second).Should(Succeed())

		privateCluster, err := privateClusters.Get(ctx, privatev1.ClustersGetRequest_builder{Id: cluster.GetId()}.Build())
		Expect(err).NotTo(HaveOccurred())
		stored := privateCluster.GetObject()
		Expect(stored.GetSpec().GetUpgradeAttemptId()).NotTo(BeEmpty())
		Expect(stored.GetStatus().GetUpgradeHistory()).To(HaveLen(1))
		controlPlane := stored.GetStatus().GetUpgradeHistory()[0]
		Expect(controlPlane.GetComponent().GetControlPlane()).NotTo(BeNil())
		Expect(controlPlane.GetState()).To(Equal(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_PENDING))
		Expect(stored.GetSpec().GetNodeSets()["my-node-set"].GetVersion().GetName()).To(Equal(defaultVersionName))

		controlPlane.SetState(privatev1.ClusterUpgradeState_CLUSTER_UPGRADE_STATE_SUCCEEDED)
		controlPlane.SetStateTransitionTime(timestamppb.New(time.Now()))
		_, err = privateClusters.Update(ctx, privatev1.ClustersUpdateRequest_builder{
			Object:     stored,
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"status.upgrade_history"}},
		}.Build())
		Expect(err).NotTo(HaveOccurred())
		_, err = privateClusters.Signal(ctx, privatev1.ClustersSignalRequest_builder{Id: cluster.GetId()}.Build())
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			reconciled, err := privateClusters.Get(ctx, privatev1.ClustersGetRequest_builder{Id: cluster.GetId()}.Build())
			g.Expect(err).NotTo(HaveOccurred())
			object := reconciled.GetObject()
			g.Expect(object.GetSpec().GetNodeSets()["my-node-set"].GetVersion().GetName()).To(Equal(targetVersionName))
			g.Expect(object.GetStatus().GetUpgradeHistory()).To(HaveLen(2))
			g.Expect(object.GetStatus().GetUpgradeHistory()[0].GetComponent().GetNodeSetId()).To(Equal("my-node-set"))
			g.Expect(object.GetStatus().GetUpgradeHistory()[0].GetAttemptId()).To(Equal(object.GetSpec().GetUpgradeAttemptId()))

			updated := &osacv1alpha1.ClusterOrder{}
			g.Expect(kubeClient.Get(ctx, crclient.ObjectKeyFromObject(order), updated)).To(Succeed())
			g.Expect(updated.Spec.NodeRequests[0].ReleaseImage).To(Equal(targetImage))
		}, time.Minute, time.Second).Should(Succeed())
		Expect(defaultVersionSemver).NotTo(Equal(targetSemver))
	})
})
