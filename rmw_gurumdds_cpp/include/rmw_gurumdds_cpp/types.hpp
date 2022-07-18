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

#ifndef RMW_GURUMDDS_CPP__TYPES_HPP_
#define RMW_GURUMDDS_CPP__TYPES_HPP_

#include <atomic>
#include <cassert>
#include <exception>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include "rmw/rmw.h"
#include "rmw/ret_types.h"

#include "rmw_gurumdds_cpp/dds_include.hpp"
#include "rmw_gurumdds_cpp/visibility_control.h"

rmw_ret_t
part_on_data_available(rmw_context_impl_t * const ctx);

rmw_ret_t
pub_on_data_available(rmw_context_impl_t * const ctx);

rmw_ret_t
sub_on_data_available(rmw_context_impl_t * const ctx);

typedef struct _GurumddsNodeInfo
{
  dds_DomainParticipant * participant;
  rmw_guard_condition_t * graph_guard_condition;
  std::list<dds_Publisher *> pub_list;
  std::list<dds_Subscriber *> sub_list;
} GurumddsNodeInfo;

typedef struct _GurumddsWaitSetInfo
{
  dds_WaitSet * wait_set;
  dds_ConditionSeq * active_conditions;
  dds_ConditionSeq * attached_conditions;
} GurumddsWaitSetInfo;

typedef struct _GurumddsEventInfo
{
  virtual ~_GurumddsEventInfo() = default;
  virtual rmw_ret_t get_status(const dds_StatusMask mask, void * event) = 0;
  virtual dds_StatusCondition * get_statuscondition() = 0;
  virtual dds_StatusMask get_status_changes() = 0;
} GurumddsEventInfo;

typedef struct _GurumddsPublisherInfo : GurumddsEventInfo
{
  rmw_gid_t ros_gid;
  dds_DataWriter * dds_writer;
  const rosidl_message_type_support_t * rosidl_message_typesupport;
  const char * implementation_identifier;
  int64_t sequence_number;
  rmw_context_impl_t * ctx;

  rmw_ret_t get_status(dds_StatusMask mask, void * event) override;
  dds_StatusCondition * get_statuscondition() override;
  dds_StatusMask get_status_changes() override;
} GurumddsPublisherInfo;

typedef struct _GurumddsPublisherGID
{
  uint8_t publication_handle[16];
} GurumddsPublisherGID;

typedef struct _GurumddsSubscriberInfo : GurumddsEventInfo
{
  rmw_gid_t ros_gid;
  dds_DataReader * dds_reader;
  dds_ReadCondition * read_condition;
  const rosidl_message_type_support_t * rosidl_message_typesupport;
  const char * implementation_identifier;
  rmw_context_impl_t * ctx;

  rmw_ret_t get_status(dds_StatusMask mask, void * event) override;
  dds_StatusCondition * get_statuscondition() override;
  dds_StatusMask get_status_changes() override;
} GurumddsSubscriberInfo;

typedef struct _GurumddsServiceInfo
{
  GurumddsPublisherInfo * reply_pub;
  GurumddsSubscriberInfo * request_sub;
  rmw_context_impl_t * ctx;
} GurumddsServiceInfo;

typedef struct _GurumddsClientInfo
{
  GurumddsPublisherInfo * request_pub;
  GurumddsSubscriberInfo * reply_sub;
  std::atomic_uint next_request_id;
  rmw_context_impl_t * ctx;
} GurumddsClientInfo;

#endif  // RMW_GURUMDDS_CPP__TYPES_HPP_
