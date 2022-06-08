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

#include "rmw/rmw.h"
#include "rmw/event_callback_type.h"
#include "rmw_gurumdds_shared_cpp/types.hpp"

typedef struct _GurumddsUserCallback
{
  std::mutex mutex;
  rmw_event_callback_t callback {nullptr};
  const void * user_data {nullptr};
  size_t unread_count {0};
  rmw_event_callback_t event_callback[DDS_STATUS_ID_MAX + 1] {nullptr};
  const void * event_data[DDS_STATUS_ID_MAX + 1] {nullptr};
  size_t event_unread_count[DDS_STATUS_ID_MAX + 1] {0};
  rmw_subscription_t * subscription {nullptr};
  rmw_client_t * client {nullptr};
  rmw_service_t * service {nullptr};
} GurumddsUserCallback;

typedef struct _GurumddsPublisherInfo : GurumddsEventInfo
{
  dds_Publisher * publisher;
  rmw_gid_t publisher_gid;
  dds_DataWriter * topic_writer;
  const rosidl_message_type_support_t * rosidl_message_typesupport;
  const char * implementation_identifier;
  GurumddsUserCallback user_callback_data;
  dds_DataWriterListener listener {};

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
  dds_Subscriber * subscriber;
  dds_DataReader * topic_reader;
  dds_ReadCondition * read_condition;
  const rosidl_message_type_support_t * rosidl_message_typesupport;
  const char * implementation_identifier;
  GurumddsUserCallback user_callback_data;
  dds_DataReaderListener listener {};

  rmw_ret_t get_status(dds_StatusMask mask, void * event) override;
  dds_StatusCondition * get_statuscondition() override;
  dds_StatusMask get_status_changes() override;
} GurumddsSubscriberInfo;

typedef struct _GurumddsServiceInfo
{
  const rosidl_service_type_support_t * service_typesupport;

  dds_Subscriber * dds_subscriber;
  dds_DataReader * request_reader;

  dds_Publisher * dds_publisher;
  dds_DataWriter * response_writer;

  dds_ReadCondition * read_condition;
  dds_DomainParticipant * participant;
  const char * implementation_identifier;
  GurumddsUserCallback user_callback_data;
  dds_DataReaderListener listener {};
} GurumddsServiceInfo;

typedef struct _GurumddsClientInfo
{
  const rosidl_service_type_support_t * service_typesupport;

  dds_Publisher * dds_publisher;
  dds_DataWriter * request_writer;

  dds_Subscriber * dds_subscriber;
  dds_DataReader * response_reader;

  dds_ReadCondition * read_condition;
  dds_DomainParticipant * participant;
  const char * implementation_identifier;
  GurumddsUserCallback user_callback_data;
  dds_DataReaderListener listener {};

  int64_t sequence_number;
  int8_t writer_guid[16];
} GurumddsClientInfo;

#endif  // RMW_GURUMDDS_CPP__TYPES_HPP_
