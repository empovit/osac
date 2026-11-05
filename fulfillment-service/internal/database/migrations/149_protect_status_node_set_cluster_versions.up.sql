--
-- Copyright (c) 2026 Red Hat, Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-- Node-set selected ClusterVersions moved from Cluster spec to server-owned
-- Cluster status. Backfill legacy rows so every spec node set retains a
-- selected version, then keep delete protection keyed by the canonical
-- reference ID.
create function backfill_cluster_node_set_version(node_set jsonb, status jsonb, cluster_version jsonb)
returns jsonb as $$
  select case
    when jsonb_typeof(status) = 'object' and status ? 'version' then status
    when jsonb_typeof(node_set) = 'object' and node_set -> 'version' is not null then
      jsonb_set(
        case when jsonb_typeof(status) = 'object' then status else '{}'::jsonb end,
        '{version}',
        node_set -> 'version'
      )
    when cluster_version is not null then
      jsonb_set(
        case when jsonb_typeof(status) = 'object' then status else '{}'::jsonb end,
        '{version}',
        cluster_version
      )
    else case when jsonb_typeof(status) = 'object' then status else '{}'::jsonb end
  end;
$$ language sql immutable;

create function backfill_cluster_node_set_versions(
  node_sets jsonb, statuses jsonb, cluster_version jsonb
) returns jsonb as $$
  select
    case when jsonb_typeof(statuses) = 'object' then statuses else '{}'::jsonb end
    || coalesce((
      select jsonb_object_agg(
        node.name,
        backfill_cluster_node_set_version(
          node.value, statuses -> node.name, cluster_version
        )
      )
      from jsonb_each(node_sets) as node(name, value)
    ), '{}'::jsonb);
$$ language sql immutable;

update clusters
set data = jsonb_set(
  jsonb_set(
    data,
    '{status}',
    case when jsonb_typeof(data -> 'status') = 'object'
      then data -> 'status'
      else '{}'::jsonb
    end
  ),
  '{status,node_sets}',
  backfill_cluster_node_set_versions(
    data -> 'spec' -> 'node_sets',
    data -> 'status' -> 'node_sets',
    data -> 'spec' -> 'version'
  )
)
where jsonb_typeof(data -> 'spec' -> 'node_sets') = 'object';

drop function backfill_cluster_node_set_versions(jsonb, jsonb, jsonb);
drop function backfill_cluster_node_set_version(jsonb, jsonb, jsonb);

create or replace function check_cluster_version_not_in_use() returns trigger as $$
begin
  if exists (
    select 1
    from clusters c
    where c.deletion_timestamp = 'epoch'
      and (
        c.data #>> '{spec,version,id}' = old.id
        or exists (
          select 1
          from jsonb_each(
            case when jsonb_typeof(c.data #> '{status,node_sets}') = 'object'
              then c.data #> '{status,node_sets}'
              else '{}'::jsonb
            end
          ) as ns(name, node_set)
          where ns.node_set #>> '{version,id}' = old.id
        )
      )
  ) then
    raise exception using
      errcode = 'Z0003',
      message = format('cannot delete cluster version ''%s'': it is in use by at least one cluster', old.name);
  end if;

  if exists (
    select 1
    from cluster_templates t
    where t.deletion_timestamp = 'epoch'
      and t.data #>> '{spec_defaults,version,id}' = old.id
  ) then
    raise exception using
      errcode = 'Z0003',
      message = format('cannot delete cluster version ''%s'': it is in use by at least one cluster template', old.name);
  end if;

  if exists (
    select 1 from cluster_catalog_items c
    where c.deletion_timestamp = 'epoch'
      and (
        c.data #>> '{fields,version,locked,id}' = old.id
        or c.data #>> '{fields,version,editable,default_value,id}' = old.id
      )
  ) then
    raise exception using
      errcode = 'Z0003',
      message = format('cannot delete cluster version ''%s'': it is in use by at least one cluster catalog item', old.name);
  end if;

  return new;
end;
$$ language plpgsql;
