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

#include "rmw/rmw.h"
#include "rmw_gurumdds_shared_cpp/rmw_common.hpp"
#include "rmw_gurumdds_cpp/identifier.hpp"
#include "rmw_gurumdds_cpp/types.hpp"

template<typename T>
static void event_set_callback(
  T event_info,
  dds_StatusId status_id,
  rmw_event_callback_t callback,
  const void * user_data)
{
  GurumddsUserCallback * data = &(event_info->user_callback_data);

  std::lock_guard<std::mutex> guard(data->mutex);

  data->event_callback[status_id] = callback;
  data->event_data[status_id] = user_data;

  if (callback && data->event_unread_count[status_id]) {
    callback(user_data, data->event_unread_count[status_id]);
    data->event_unread_count[status_id] = 0;
  }
}

extern "C"
{
rmw_ret_t
rmw_publisher_event_init(
  rmw_event_t * rmw_event,
  const rmw_publisher_t * publisher,
  rmw_event_type_t event_type)
{
  return shared__rmw_init_event(
    gurum_gurumdds_identifier,
    rmw_event,
    publisher->implementation_identifier,
    publisher->data,
    event_type);
}

rmw_ret_t
rmw_subscription_event_init(
  rmw_event_t * rmw_event,
  const rmw_subscription_t * subscription,
  rmw_event_type_t event_type)
{
  return shared__rmw_init_event(
    gurum_gurumdds_identifier,
    rmw_event,
    subscription->implementation_identifier,
    subscription->data,
    event_type);
}

rmw_ret_t
rmw_take_event(
  const rmw_event_t * event_handle,
  void * event_info,
  bool * taken)
{
  return shared__rmw_take_event(gurum_gurumdds_identifier, event_handle, event_info, taken);
}

rmw_ret_t
rmw_event_set_callback(
  rmw_event_t * rmw_event,
  rmw_event_callback_t callback,
  const void * user_data)
{
  switch (rmw_event->event_type) {
    case RMW_EVENT_LIVELINESS_CHANGED:
      {
        auto sub_info = static_cast<GurumddsSubscriberInfo *>(rmw_event->data);
        event_set_callback(
          sub_info, dds_LivelinessChangedStatusId,
          callback, user_data);
        break;
      }

    case RMW_EVENT_REQUESTED_DEADLINE_MISSED:
      {
        auto sub_info = static_cast<GurumddsSubscriberInfo *>(rmw_event->data);
        event_set_callback(
          sub_info, dds_RequestedDeadlineMissedStatusId,
          callback, user_data);
        break;
      }

    case RMW_EVENT_REQUESTED_QOS_INCOMPATIBLE:
      {
        auto sub_info = static_cast<GurumddsSubscriberInfo *>(rmw_event->data);
        event_set_callback(
          sub_info, dds_RequestedIncompatibleQosStatusId,
          callback, user_data);
        break;
      }

    case RMW_EVENT_MESSAGE_LOST:
      {
        auto sub_info = static_cast<GurumddsSubscriberInfo *>(rmw_event->data);
        event_set_callback(
          sub_info, dds_SampleLostStatusId,
          callback, user_data);
        break;
      }

    case RMW_EVENT_LIVELINESS_LOST:
      {
        auto pub_info = static_cast<GurumddsPublisherInfo *>(rmw_event->data);
        event_set_callback(
          pub_info, dds_LivelinessLostStatusId,
          callback, user_data);
        break;
      }

    case RMW_EVENT_OFFERED_DEADLINE_MISSED:
      {
        auto pub_info = static_cast<GurumddsPublisherInfo *>(rmw_event->data);
        event_set_callback(
          pub_info, dds_OfferedDeadlineMissedStatusId,
          callback, user_data);
        break;
      }

    case RMW_EVENT_OFFERED_QOS_INCOMPATIBLE:
      {
        auto pub_info = static_cast<GurumddsPublisherInfo *>(rmw_event->data);
        event_set_callback(
          pub_info, dds_OfferedIncompatibleQosStatusId,
          callback, user_data);
        break;
      }

    case RMW_EVENT_INVALID:
      {
        return RMW_RET_INVALID_ARGUMENT;
      }
  }

  return RMW_RET_OK;
}
}  // extern "C"
