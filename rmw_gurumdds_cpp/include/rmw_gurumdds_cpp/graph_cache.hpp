// Copyright 2022 GurumNetworks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RMW_GURUMDDS_CPP__GRAPH_CACHE_HPP_
#define RMW_GURUMDDS_CPP__GRAPH_CACHE_HPP_

#include "rmw_gurumdds_cpp/rmw_context_impl.hpp"
#include "rmw_gurumdds_cpp/types.hpp"

rmw_ret_t
graph_cache_initialize(rmw_context_impl_t * const ctx);

rmw_ret_t
graph_cache_finalize(rmw_context_impl_t * const ctx);

rmw_ret_t
graph_enable(rmw_context_t * const ctx);

rmw_ret_t
graph_publish_update(
  rmw_context_impl_t * const ctx,
  void * const msg);

rmw_ret_t
graph_on_node_created(
  rmw_context_impl_t * const ctx,
  const rmw_node_t * const node);

rmw_ret_t
graph_on_node_deleted(
  rmw_context_impl_t * const ctx,
  const rmw_node_t * const node);

rmw_ret_t
graph_on_publisher_created(
  rmw_context_impl_t * const ctx,
  const rmw_node_t * const node,
  GurumddsPublisherInfo * const pub);

rmw_ret_t
graph_on_publisher_deleted(
  rmw_context_impl_t * const ctx,
  const rmw_node_t * const node,
  GurumddsPublisherInfo * const pub);

rmw_ret_t
graph_on_subscriber_created(
  rmw_context_impl_t * const ctx,
  const rmw_node_t * const node,
  GurumddsSubscriberInfo * const sub);

rmw_ret_t
graph_on_subscriber_deleted(
  rmw_context_impl_t * const ctx,
  const rmw_node_t * const node,
  GurumddsSubscriberInfo * const sub);

rmw_ret_t
graph_on_service_created(
  rmw_context_impl_t * const ctx,
  const rmw_node_t * const node,
  GurumddsServiceInfo * const svc);

rmw_ret_t
graph_on_service_deleted(
  rmw_context_impl_t * const ctx,
  const rmw_node_t * const node,
  GurumddsServiceInfo * const svc);

rmw_ret_t
graph_on_client_created(
  rmw_context_impl_t * const ctx,
  const rmw_node_t * const node,
  GurumddsClientInfo * const client);

rmw_ret_t
graph_on_client_deleted(
  rmw_context_impl_t * const ctx,
  const rmw_node_t * const node,
  GurumddsClientInfo * const client);

rmw_ret_t
graph_on_participant_info(rmw_context_impl_t * ctx);

rmw_ret_t
graph_add_participant(
  rmw_context_impl_t * const ctx,
  const rmw_gid_t gid,
  const char * const enclave);

rmw_ret_t
graph_remove_participant(
  rmw_context_impl_t * const ctx,
  const rmw_gid_t gid);

rmw_ret_t
graph_add_remote_entity(
  rmw_context_impl_t * ctx,
  const dds_GUID_t * const endp_guid,
  const dds_GUID_t * const dp_guid,
  const char * const topic_name,
  const char * const type_name,
  const dds_ReliabilityQosPolicy * const reliability,
  const dds_DurabilityQosPolicy * const durability,
  const dds_DeadlineQosPolicy * const deadline,
  const dds_LivelinessQosPolicy * const liveliness,
  const dds_LifespanQosPolicy * const lifespan,
  const bool is_reader);

rmw_ret_t
graph_remove_entity(
  rmw_context_impl_t * const ctx,
  const rmw_gid_t gid,
  const bool is_reader);

#endif  // RMW_GURUMDDS_CPP__GRAPH_CACHE_HPP_
