// Copyright 2019 GurumNetworks, Inc.
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

#include "rmw/publisher_options.h"
#include "rmw/subscription_options.h"
#include "rmw/qos_profiles.h"

#include "rmw_gurumdds_cpp/context_listener_thread.hpp"
#include "rmw_gurumdds_cpp/graph_cache.hpp"
#include "rmw_gurumdds_cpp/gid.hpp"
#include "rmw_gurumdds_cpp/rmw_publisher.hpp"
#include "rmw_gurumdds_cpp/rmw_subscription.hpp"

#include "rosidl_typesupport_cpp/message_type_support.hpp"

rmw_ret_t
graph_cache_initialize(rmw_context_impl_t * const ctx)
{
  rmw_qos_profile_t qos = rmw_qos_profile_default;
  qos.avoid_ros_namespace_conventions = true;
  qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
  qos.depth = 1;
  qos.durability = RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL;
  qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;

  rmw_publisher_options_t publisher_options = rmw_get_default_publisher_options();
  rmw_subscription_options_t subscription_options = rmw_get_default_subscription_options();
  subscription_options.ignore_local_publications = true;

  const rosidl_message_type_support_t * const type_supports_partinfo =
    rosidl_typesupport_cpp::get_message_type_support_handle<
    rmw_dds_common::msg::ParticipantEntitiesInfo>();

  const char * const topic_name_partinfo = "ros_discovery_info";

  ctx->common_ctx.pub =
    __rmw_create_publisher(
    ctx,
    ctx->participant,
    ctx->publisher,
    type_supports_partinfo,
    topic_name_partinfo,
    &qos,
    &publisher_options,
    true);

  if (ctx->common_ctx.pub == nullptr) {
    RMW_SET_ERROR_MSG("failed to create publisher for ParticipantEntityInfo");
    return RMW_RET_ERROR;
  }

  qos.history = RMW_QOS_POLICY_HISTORY_KEEP_ALL;

  ctx->common_ctx.sub =
    __rmw_create_subscription(
    ctx,
    ctx->participant,
    ctx->subscriber,
    type_supports_partinfo,
    topic_name_partinfo,
    &qos,
    &subscription_options,
    true);

  if (ctx->common_ctx.sub == nullptr) {
    RMW_SET_ERROR_MSG("failed to create subscription for ParticipantEntityInfo");
    return RMW_RET_ERROR;
  }

  ctx->common_ctx.graph_guard_condition =
    rmw_create_guard_condition(ctx->base);
  if (ctx->common_ctx.graph_guard_condition == nullptr) {
    RMW_SET_ERROR_MSG("failed to create graph guard condition");
    return RMW_RET_BAD_ALLOC;
  }

  ctx->common_ctx.graph_cache.set_on_change_callback(
    [gcond = ctx->common_ctx.graph_guard_condition]()
    {
      rmw_ret_t ret = rmw_trigger_guard_condition(gcond);
      if (ret != RMW_RET_OK) {
        RMW_SET_ERROR_MSG("failed to trigger graph cache on_change_callback");
      }
    });

  entity_get_gid<dds_DomainParticipant>(ctx->participant, ctx->common_ctx.gid);
  std::string dp_enclave = ctx->base->options.enclave;
  ctx->common_ctx.graph_cache.add_participant(ctx->common_ctx.gid, dp_enclave);

  dds_Subscriber * builtin_subscriber =
    dds_DomainParticipant_get_builtin_subscriber(ctx->participant);
  if (builtin_subscriber == nullptr) {
    RMW_SET_ERROR_MSG("failed to get builtin subscriber");
    return RMW_RET_ERROR;
  }

  ctx->builtin_participant_datareader =
    dds_Subscriber_lookup_datareader(builtin_subscriber, "BuiltinParticipant");
  if (ctx->builtin_participant_datareader == nullptr) {
    RMW_SET_ERROR_MSG("builtin participant datareader handle is null");
    return RMW_RET_ERROR;
  }

  ctx->builtin_publication_datareader =
    dds_Subscriber_lookup_datareader(builtin_subscriber, "BuiltinPublications");
  if (ctx->builtin_publication_datareader == nullptr) {
    RMW_SET_ERROR_MSG("builtin publication datareader handle is null");
    return RMW_RET_ERROR;
  }

  ctx->builtin_subscription_datareader =
    dds_Subscriber_lookup_datareader(builtin_subscriber, "BuiltinSubscriptions");
  if (ctx->builtin_subscription_datareader == nullptr) {
    RMW_SET_ERROR_MSG("builtin subscription datareader handle is null");
    return RMW_RET_ERROR;
  }

  return RMW_RET_OK;
}

rmw_ret_t
graph_cache_finalize(rmw_context_impl_t * const ctx)
{
  if (stop_listener_thread(ctx->base)) {
    RMW_SET_ERROR_MSG("failed to stop listener thread");
    return RMW_RET_ERROR;
  }

  ctx->common_ctx.graph_cache.clear_on_change_callback();

  if (ctx->common_ctx.graph_guard_condition) {
    if (RMW_RET_OK !=
      rmw_destroy_guard_condition(ctx->common_ctx.graph_guard_condition))
    {
      RMW_SET_ERROR_MSG("failed to destory graph guard condition");
      return RMW_RET_ERROR;
    }
    ctx->common_ctx.graph_guard_condition = nullptr;
  }

  return RMW_RET_OK;
}
