package v1alpha1_test

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/osac-project/osac/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac/osac-operator/pkg/provisioning"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

var _ = Describe("ClusterOrder upgrade contract", func() {
	It("validates control-plane and node-set release images identically", func() {
		data, err := os.ReadFile("../../config/crd/bases/osac.openshift.io_clusterorders.yaml")
		Expect(err).NotTo(HaveOccurred())
		var crd apiextensionsv1.CustomResourceDefinition
		Expect(yaml.Unmarshal(data, &crd)).To(Succeed())
		Expect(crd.Spec.Versions).NotTo(BeEmpty())

		spec := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
		controlPlane := spec.Properties["releaseImage"]
		nodeSet := spec.Properties["nodeRequests"].Items.Schema.Properties["releaseImage"]
		Expect(nodeSet.Pattern).To(Equal(controlPlane.Pattern))
		Expect(nodeSet.MaxLength).To(Equal(controlPlane.MaxLength))
		pattern := regexp.MustCompile(controlPlane.Pattern)

		for _, image := range []string{
			"quay.io/openshift-release-dev/ocp-release:4.22.0-multi",
			"registry--mirror.example/ocp/release:stable",
			"[2001:db8::1]:5000/ocp/release:stable",
			"registry.example:5000/ocp/release@sha256:" + strings.Repeat("a", 64),
			"quay.io/ocp/release:stable@sha256:" + strings.Repeat("a", 64),
		} {
			Expect(pattern.MatchString(image)).To(BeTrue(), image)
		}
		for _, image := range []string{
			"quay.io/ocp/release:bad tag",
			"quay.io/ocp/release:bad\ttag",
			"quay.io/ocp/release:tag;rm",
			"quay.io/ocp/release:",
			"quay.io/ocp/release@sha256:abc",
		} {
			Expect(pattern.MatchString(image)).To(BeFalse(), image)
		}
	})

	It("keeps independent images across serialization and copying", func() {
		order := &v1alpha1.ClusterOrder{
			Spec: v1alpha1.ClusterOrderSpec{
				ReleaseImage: "registry.example/ocp:4.22.0",
				NodeRequests: []v1alpha1.NodeRequest{{
					NodeSet:      "workers",
					ReleaseImage: "registry.example/ocp:4.21.0",
				}},
			},
			Status: v1alpha1.ClusterOrderStatus{
				ObservedVersion: "4.22.0",
				NodeRequests: []v1alpha1.NodeRequestStatus{{
					NodeSet: "workers", ObservedVersion: "4.21.0",
				}},
			},
		}

		data, err := json.Marshal(order)
		Expect(err).NotTo(HaveOccurred())
		var decoded v1alpha1.ClusterOrder
		Expect(json.Unmarshal(data, &decoded)).To(Succeed())
		Expect(decoded.Spec.ReleaseImage).To(Equal("registry.example/ocp:4.22.0"))
		Expect(decoded.Spec.NodeRequests[0].ReleaseImage).To(Equal("registry.example/ocp:4.21.0"))
		Expect(decoded.Status.ObservedVersion).To(Equal("4.22.0"))
		Expect(decoded.Status.NodeRequests[0].ObservedVersion).To(Equal("4.21.0"))
		copy := order.DeepCopy()
		copy.Spec.NodeRequests[0].ReleaseImage = "registry.example/ocp:4.23.0"
		Expect(order.Spec.NodeRequests[0].ReleaseImage).To(Equal("registry.example/ocp:4.21.0"))
	})

	It("preserves the config hash of an order with an empty node image", func() {
		spec := v1alpha1.ClusterOrderSpec{NodeRequests: []v1alpha1.NodeRequest{{
			NodeSet: "workers", NumberOfNodes: 3,
		}}}
		encoded, err := json.Marshal(spec)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(encoded)).To(Equal(`{"nodeRequests":[{"nodeSet":"workers","numberOfNodes":3}]}`))

		hash, err := provisioning.ComputeDesiredConfigVersion(spec)
		Expect(err).NotTo(HaveOccurred())
		Expect(hash).To(Equal("398d688e45d7741c"))

		spec.NodeRequests[0].ReleaseImage = "registry.example/ocp:4.21.0"
		withImage, err := provisioning.ComputeDesiredConfigVersion(spec)
		Expect(err).NotTo(HaveOccurred())
		Expect(withImage).NotTo(Equal(hash))
	})
})
