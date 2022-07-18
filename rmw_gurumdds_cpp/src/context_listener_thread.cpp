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

#include <atomic>
#include <cassert>
#include <cstring>
#include <thread>

#include "rcutils/macros.h"

#include "rmw/allocators.h"
#include "rmw/error_handling.h"
#include "rmw/init.h"
#include "rmw/ret_types.h"
#include "rmw/rmw.h"
#include "rmw/types.h"
#include "rmw/impl/cpp/macros.hpp"

#include "rmw_dds_common/context.hpp"
#include "rmw_dds_common/gid_utils.hpp"
#include "rmw_dds_common/msg/participant_entities_info.hpp"

#include "rmw_gurumdds_cpp/context_listener_thread.hpp"
#include "rmw_gurumdds_cpp/identifier.hpp"

static
void rmw_gurumdds_discovery_thread(rmw_context_t * context)
{
  assert(nullptr != context);
  assert(nullptr != context->impl);

  rmw_dds_common::Context * const common_ctx = &context->impl->common_ctx;
  while (common_ctx->thread_is_running.load()) {
    assert(nullptr != common_ctx->sub);
    assert(nullptr != common_ctx->sub->data);
    void * subscriptions_buffer[] = {common_ctx->sub->data};
    void * guard_conditions_buffer[] = {common_ctx->listener_thread_gc->data};
    rmw_subscriptions_t subscriptions;
    rmw_guard_conditions_t guard_conditions;
    subscriptions.subscriber_count = 1;
    subscriptions.subscribers = subscriptions_buffer;
    guard_conditions.guard_condition_count = 1;
    guard_conditions.guard_conditions = guard_conditions_buffer;

    rmw_wait_set_t * wait_set = rmw_create_wait_set(context, 2);
    if (nullptr == wait_set) {
      RMW_SET_ERROR_MSG("Failed to create wait set for discovery");
    }
    if (RMW_RET_OK != rmw_wait(
        &subscriptions,
        &guard_conditions,
        nullptr,
        nullptr,
        nullptr,
        wait_set,
        nullptr))
    {
      RMW_SET_ERROR_MSG("rmw_wait failed");
    }
    if (RMW_RET_OK != rmw_destroy_wait_set(wait_set)) {
      RMW_SET_ERROR_MSG("Failed to destory wait set");
    }

    if (subscriptions_buffer[0]) {
      rmw_dds_common::msg::ParticipantEntitiesInfo msg;
      bool taken;
      if (RMW_RET_OK != rmw_take(
          common_ctx->sub,
          static_cast<void *>(&msg),
          &taken,
          nullptr))
      {
        RMW_SET_ERROR_MSG("rmw_take failed");
      }
      if (taken) {
        if (std::memcmp(
            reinterpret_cast<char *>(common_ctx->gid.data),
            reinterpret_cast<char *>(&msg.gid.data),
            RMW_GID_STORAGE_SIZE) == 0)
        {
          // ignore local messages
          continue;
        }
        common_ctx->graph_cache.update_participant_entities(msg);
      }
    }
  }
}

rmw_ret_t
run_listener_thread(rmw_context_t * context)
{
  RCUTILS_LOG_DEBUG_NAMED(RMW_GURUMDDS_ID, "Starting discovery thread");

  rmw_dds_common::Context * const common_ctx = &context->impl->common_ctx;
  common_ctx->listener_thread_gc = rmw_create_guard_condition(context);

  if (common_ctx->listener_thread_gc == nullptr) {
    RMW_SET_ERROR_MSG("Failed to create context listener thread condition");
    return RMW_RET_ERROR;
  }

  common_ctx->thread_is_running.store(true);

  try {
    common_ctx->listener_thread = std::thread(rmw_gurumdds_discovery_thread, context);
    RCUTILS_LOG_DEBUG_NAMED(RMW_GURUMDDS_ID, "discovery thread started");
    return RMW_RET_OK;
  } catch (const std::exception & exc) {
    RMW_SET_ERROR_MSG_WITH_FORMAT_STRING("Failed to create std::thread: %s", exc.what());
  } catch (...) {
    RMW_SET_ERROR_MSG("Failed to create std::thread");
  }

  // Handling when thread creation fails
  common_ctx->thread_is_running.store(false);
  if (rmw_destroy_guard_condition(common_ctx->listener_thread_gc) != RMW_RET_OK) {
    RCUTILS_LOG_ERROR_NAMED(RMW_GURUMDDS_ID, "Failed to destroy discovery thread guard condition");
  }

  return RMW_RET_ERROR;
}


rmw_ret_t
stop_listener_thread(rmw_context_t * context)
{
  RCUTILS_LOG_DEBUG_NAMED(RMW_GURUMDDS_ID, "Stopping discovery thread");

  rmw_dds_common::Context * const common_ctx = &context->impl->common_ctx;
  common_ctx->thread_is_running.exchange(false);

  rmw_ret_t ret = rmw_trigger_guard_condition(common_ctx->listener_thread_gc);
  if (RMW_RET_OK != ret) {
    return ret;
  }

  try {
    common_ctx->listener_thread.join();
  } catch (const std::exception & exc) {
    RMW_SET_ERROR_MSG_WITH_FORMAT_STRING("Failed to join std::thread: %s", exc.what());
    return RMW_RET_ERROR;
  } catch (...) {
    RMW_SET_ERROR_MSG("Failed to join std::thread");
    return RMW_RET_ERROR;
  }

  ret = rmw_destroy_guard_condition(common_ctx->listener_thread_gc);
  if (RMW_RET_OK != ret) {
    return ret;
  }

  RCUTILS_LOG_DEBUG_NAMED(RMW_GURUMDDS_ID, "discovery thread stopped");
  return RMW_RET_OK;
}
