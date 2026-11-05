/*
Copyright (c) 2026 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package migrations

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2/dsl/core"
	. "github.com/onsi/gomega"
)

var _ = DescribeMigration("Protect status node-set ClusterVersions", func() {
	insertVersion := func(ctx context.Context, id, name string) {
		_, err := conn.Exec(ctx, `insert into cluster_versions (id, name, tenant, data)
			values ($1, $2, 'system', $3::jsonb)`, id, name,
			fmt.Sprintf(`{"spec":{"version":%q,"image":"quay.io/example/release:4.17"}}`, strings.ReplaceAll(name, "-", ".")))
		Expect(err).ToNot(HaveOccurred())
	}

	insertCluster := func(ctx context.Context, id, data string) {
		_, err := conn.Exec(ctx, `insert into clusters (id, name, tenant, data)
			values ($1, $1, 'system', $2::jsonb)`, id, data)
		Expect(err).ToNot(HaveOccurred())
	}

	deleteVersion := func(ctx context.Context, id string) error {
		_, err := conn.Exec(ctx, `update cluster_versions set deletion_timestamp = now() where id = $1`, id)
		return err
	}

	expectInUse := func(err error) {
		var pgErr *pgconn.PgError
		ExpectWithOffset(1, errors.As(err, &pgErr)).To(BeTrue())
		ExpectWithOffset(1, pgErr.Code).To(Equal("Z0003"))
		ExpectWithOffset(1, pgErr.Message).To(ContainSubstring("in use by at least one cluster"))
	}

	It("backfills legacy node-set versions without overwriting status", func(ctx context.Context) {
		insertVersion(ctx, "node-version", "4-17-0")
		insertVersion(ctx, "control-plane-version", "4-16-0")
		insertVersion(ctx, "existing-status-version", "4-15-0")
		insertCluster(ctx, "legacy-cluster", `{
			"spec": {
				"version": {"id": "control-plane-version"},
				"node_sets": {
					"compute": {"version": {"id": "node-version"}},
					"gpu": {},
					"storage": {}
				}
			},
			"status": {
				"node_sets": {
					"gpu": {},
					"storage": {"version": {"id": "existing-status-version"}}
				}
			}
		}`)

		Expect(tool.Migrate(ctx, 149)).To(Succeed())

		expected := map[string]string{
			"compute": "node-version",
			"gpu":     "control-plane-version",
			"storage": "existing-status-version",
		}
		for name, versionID := range expected {
			var actual string
			err := conn.QueryRow(ctx,
				`select data -> 'status' -> 'node_sets' -> $1 -> 'version' ->> 'id' from clusters where id = $2`,
				name, "legacy-cluster",
			).Scan(&actual)
			Expect(err).ToNot(HaveOccurred(), name)
			Expect(actual).To(Equal(versionID), name)
		}
	})

	It("creates the status object when backfilling a legacy node set", func(ctx context.Context) {
		insertVersion(ctx, "node-version", "4-17-0")
		insertCluster(ctx, "missing-status-cluster", `{
			"spec": {
				"node_sets": {
					"compute": {"version": {"id": "node-version"}}
				}
			}
		}`)

		Expect(tool.Migrate(ctx, 149)).To(Succeed())

		var actual string
		err := conn.QueryRow(ctx,
			`select data -> 'status' -> 'node_sets' -> 'compute' -> 'version' ->> 'id'
				from clusters where id = $1`, "missing-status-cluster",
		).Scan(&actual)
		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal("node-version"))
	})

	It("protects a ClusterVersion selected in node-set status", func(ctx context.Context) {
		insertVersion(ctx, "node-version", "4-17-0")
		insertCluster(ctx, "cluster", `{
			"status":{"node_sets":{"workers":{"version":{"id":"node-version"}}}}
		}`)
		Expect(tool.Migrate(ctx, 149)).To(Succeed())

		expectInUse(deleteVersion(ctx, "node-version"))
	})

	It("protects references added after migration", func(ctx context.Context) {
		insertVersion(ctx, "node-version", "4-17-0")
		insertCluster(ctx, "cluster", `{}`)
		Expect(tool.Migrate(ctx, 149)).To(Succeed())

		_, err := conn.Exec(ctx, `update clusters set data = jsonb_set(data,
			'{status}', '{"node_sets":{"workers":{"version":{"id":"node-version"}}}}'::jsonb) where id = 'cluster'`)
		Expect(err).ToNot(HaveOccurred())
		expectInUse(deleteVersion(ctx, "node-version"))
	})

	It("retains control-plane protection", func(ctx context.Context) {
		insertVersion(ctx, "control-plane-version", "4-17-0")
		insertCluster(ctx, "cluster", `{"spec":{"version":{"id":"control-plane-version"}}}`)
		Expect(tool.Migrate(ctx, 149)).To(Succeed())

		expectInUse(deleteVersion(ctx, "control-plane-version"))
	})

	It("ignores references held only by soft-deleted clusters", func(ctx context.Context) {
		insertVersion(ctx, "node-version", "4-17-0")
		insertCluster(ctx, "cluster", `{
			"status":{"node_sets":{"workers":{"version":{"id":"node-version"}}}}
		}`)
		Expect(tool.Migrate(ctx, 149)).To(Succeed())

		_, err := conn.Exec(ctx, `update clusters set deletion_timestamp = now() where id = 'cluster'`)
		Expect(err).ToNot(HaveOccurred())
		Expect(deleteVersion(ctx, "node-version")).To(Succeed())
	})
})
