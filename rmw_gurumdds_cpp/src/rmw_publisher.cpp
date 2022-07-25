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

#include <string>
#include <sstream>
#include <limits>
#include <thread>
#include <chrono>

#include "rcutils/error_handling.h"
#include "rcutils/types.h"

#include "rmw/allocators.h"
#include "rmw/error_handling.h"
#include "rmw/impl/cpp/macros.hpp"
#include "rmw/types.h"

#include "rmw_gurumdds_cpp/identifier.hpp"
#include "rmw_gurumdds_cpp/namespace_prefix.hpp"
#include "rmw_gurumdds_cpp/qos.hpp"
#include "rmw_gurumdds_cpp/rmw_context_impl.hpp"
#include "rmw_gurumdds_cpp/rmw_publisher.hpp"
#include "rmw_gurumdds_cpp/types.hpp"

rmw_publisher_t *
__rmw_create_publisher(
  rmw_context_impl_t * const ctx,
  dds_DomainParticipant * const participant,
  dds_Publisher * const pub,
  const rosidl_message_type_support_t * type_supports,
  const char * topic_name,
  const rmw_qos_profile_t * qos_policies,
  const rmw_publisher_options_t * publisher_options,
  const bool internal)
{
  std::lock_guard<std::mutex> guard(ctx->endpoint_mutex);
  
  const rosidl_message_type_support_t * type_support =
    get_message_typesupport_handle(type_supports, rosidl_typesupport_introspection_c__identifier);
  if (type_support == nullptr) {
    rcutils_reset_error();
    type_support = get_message_typesupport_handle(
      type_supports, rosidl_typesupport_introspection_cpp::typesupport_identifier);
    if (type_support == nullptr) {
      rcutils_reset_error();
      RMW_SET_ERROR_MSG("type support not from this implementation");
      return nullptr;
    }
  }

  rmw_publisher_t * rmw_publisher = nullptr;
  GurumddsPublisherInfo * publisher_info = nullptr;
  dds_PublisherQos publisher_qos;
  dds_DataWriter * topic_writer = nullptr;
  dds_DataWriterQos datawriter_qos;
  dds_Topic * topic = nullptr;
  dds_TopicDescription * topic_desc = nullptr;
  dds_TypeSupport * dds_typesupport = nullptr;
  dds_ReturnCode_t ret;
  rmw_ret_t rmw_ret;

  std::string type_name =
    create_type_name(type_support->data, type_support->typesupport_identifier);
  if (type_name.empty()) {
    // Error message is already set
    return nullptr;
  }

  std::string processed_topic_name = create_topic_name(
    ros_topic_prefix, topic_name, "", qos_policies);

  std::string metastring =
    create_metastring(type_support->data, type_support->typesupport_identifier);
  if (metastring.empty()) {
    // Error message is already set
    return nullptr;
  }

  dds_typesupport = dds_TypeSupport_create(metastring.c_str());
  if (dds_typesupport == nullptr) {
    RMW_SET_ERROR_MSG("failed to create typesupport");
    goto fail;
  }

  ret = dds_TypeSupport_register_type(dds_typesupport, participant, type_name.c_str());
  if (ret != dds_RETCODE_OK) {
    RMW_SET_ERROR_MSG("failed to register type to domain participant");
    goto fail;
  }

  topic_desc = dds_DomainParticipant_lookup_topicdescription(
    participant, processed_topic_name.c_str());
  if (topic_desc == nullptr) {
    dds_TopicQos topic_qos;
    ret = dds_DomainParticipant_get_default_topic_qos(participant, &topic_qos);
    if (ret != dds_RETCODE_OK) {
      RMW_SET_ERROR_MSG("failed to get default topic qos");
      goto fail;
    }

    topic = dds_DomainParticipant_create_topic(
      participant, processed_topic_name.c_str(), type_name.c_str(), &topic_qos, nullptr, 0);
    if (topic == nullptr) {
      RMW_SET_ERROR_MSG("failed to create topic");
      dds_TopicQos_finalize(&topic_qos);
      goto fail;
    }

    ret = dds_TopicQos_finalize(&topic_qos);
    if (ret != dds_RETCODE_OK) {
      RMW_SET_ERROR_MSG("failed to finalize topic qos");
      goto fail;
    }
  } else {
    dds_Duration_t timeout;
    timeout.sec = 0;
    timeout.nanosec = 1;
    topic = dds_DomainParticipant_find_topic(participant, processed_topic_name.c_str(), &timeout);
    if (topic == nullptr) {
      RMW_SET_ERROR_MSG("failed to find topic");
      goto fail;
    }
  }

  if (!get_datawriter_qos(dds_publisher, qos_policies, &datawriter_qos)) {
    // Error message already set
    goto fail;
  }

  topic_writer = dds_Publisher_create_datawriter(dds_publisher, topic, &datawriter_qos, nullptr, 0);
  if (topic_writer == nullptr) {
    RMW_SET_ERROR_MSG("failed to create datawriter");
    dds_DataWriterQos_finalize(&datawriter_qos);
    goto fail;
  }

  ret = dds_DataWriterQos_finalize(&datawriter_qos);
  if (ret != dds_RETCODE_OK) {
    RMW_SET_ERROR_MSG("failed to finalize datawriter qos");
    goto fail;
  }

  publisher_info = new(std::nothrow) GurumddsPublisherInfo();
  if (publisher_info == nullptr) {
    RMW_SET_ERROR_MSG("failed to allocate GurumddsPublisherInfo");
    goto fail;
  }

  publisher_info->dds_writer = topic_writer;
  
  return nullptr;
}

rmw_ret_t
__rmw_destroy_publisher(
  rmw_context_impl_t * const ctx,
  rmw_publisher_t * const publisher)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(ctx, RMW_RET_INVALID_ARGUMENT);
  return RMW_RET_OK;
}

extern "C"
{
rmw_ret_t
rmw_init_publisher_allocation(
  const rosidl_message_type_support_t * type_support,
  const rosidl_runtime_c__Sequence__bound * message_bounds,
  rmw_publisher_allocation_t * allocation)
{
  (void)type_support;
  (void)message_bounds;
  (void)allocation;

  RMW_SET_ERROR_MSG("rmw_init_publisher_allocation is not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t
rmw_fini_publisher_allocation(rmw_publisher_allocation_t * allocation)
{
  (void)allocation;

  RMW_SET_ERROR_MSG("rmw_fini_publisher_allocation is not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_publisher_t *
rmw_create_publisher(
  const rmw_node_t * node,
  const rosidl_message_type_support_t * type_supports,
  const char * topic_name,
  const rmw_qos_profile_t * qos_policies,
  const rmw_publisher_options_t * publisher_options)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, nullptr);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node,
    node->implementation_identifier,
    RMW_GURUMDDS_ID,
    return nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(type_supports, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(topic_name, nullptr);
  if (strlen(topic_name) == 0) {
    RMW_SET_ERROR_MSG("topic_name argument is empty");
    return nullptr;
  }
  RMW_CHECK_ARGUMENT_FOR_NULL(qos_policies, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher_options, nullptr); 

  if (!qos_policies->avoid_ros_namespace_conventions) {
    int validation_result = RMW_TOPIC_VALID;
    rmw_ret_t ret = rmw_validate_full_topic_name(topic_name, &validation_result, nullptr);
    if (ret != RMW_RET_OK) {
      return nullptr;
    }
    if (validation_result != RMW_TOPIC_VALID) {
      const char * reason = rmw_full_topic_name_validation_result_string(validation_result);
      RMW_SET_ERROR_MSG_WITH_FORMAT_STRING(
        "topic name is invalid: %s", reason);
      return nullptr;
    }
  }

  if (publisher_options->require_unique_network_flow_endpoints ==
    RMW_UNIQUE_NETWORK_FLOW_ENDPOINTS_STRICTLY_REQUIRED)
  {
    RMW_SET_ERROR_MSG("Unique network flow endpoints not supported on publishers");
    return nullptr;
  }

  rmw_context_impl_t * ctx = node->context->impl;

  rmw_publisher_t * const rmw_pub =
    __rmw_create_publisher(
      ctx,
      ctx->participant,
      ctx->publisher,
      type_supports,
      topic_name,
      qos_policies,
      publisher_options,
      ctx->localhost_only);

  if (rmw_pub == nullptr) {
    RMW_SET_ERROR_MSG("failed to create RMW publisher");
    return nullptr;
  }

  return rmw_pub;
}

rmw_ret_t
rmw_publisher_count_matched_subscriptions(
  const rmw_publisher_t * publisher,
  size_t * subscription_count)
{
  if (publisher == nullptr) {
    RMW_SET_ERROR_MSG("publisher handle is null");
    return RMW_RET_INVALID_ARGUMENT;
  }

  if (subscription_count == nullptr) {
    RMW_SET_ERROR_MSG("subscription_count is null");
    return RMW_RET_INVALID_ARGUMENT;
  }

  auto info = static_cast<GurumddsPublisherInfo *>(publisher->data);
  if (info == nullptr) {
    RMW_SET_ERROR_MSG("publisher internal data is invalid");
    return RMW_RET_ERROR;
  }

  dds_Publisher * dds_publisher = info->publisher;
  if (dds_publisher == nullptr) {
    RMW_SET_ERROR_MSG("dds publisher is null");
    return RMW_RET_ERROR;
  }

  dds_DataWriter * topic_writer = info->topic_writer;
  if (topic_writer == nullptr) {
    RMW_SET_ERROR_MSG("topic writer is null");
    return RMW_RET_ERROR;
  }

  dds_InstanceHandleSeq * seq = dds_InstanceHandleSeq_create(4);
  if (dds_DataWriter_get_matched_subscriptions(topic_writer, seq) != dds_RETCODE_OK) {
    RMW_SET_ERROR_MSG("failed to get matched subscriptions");
    dds_InstanceHandleSeq_delete(seq);
    return RMW_RET_ERROR;
  }
  *subscription_count = static_cast<size_t>(dds_InstanceHandleSeq_length(seq));

  dds_InstanceHandleSeq_delete(seq);

  return RMW_RET_OK;
}

rmw_ret_t
rmw_publisher_assert_liveliness(const rmw_publisher_t * publisher)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);

  auto * info = static_cast<GurumddsPublisherInfo *>(publisher->data);
  if (info == nullptr) {
    RMW_SET_ERROR_MSG("publisher internal data is invalid");
    return RMW_RET_ERROR;
  }

  if (info->topic_writer == nullptr) {
    RMW_SET_ERROR_MSG("publisher internal datawriter is invalid");
    return RMW_RET_ERROR;
  }

  if (dds_DataWriter_assert_liveliness(info->topic_writer) != dds_RETCODE_OK) {
    RMW_SET_ERROR_MSG("failed to assert liveliness of datawriter");
    return RMW_RET_ERROR;
  }

  return RMW_RET_OK;
}

rmw_ret_t
rmw_publisher_wait_for_all_acked(const rmw_publisher_t * publisher, rmw_time_t wait_timeout)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    publisher,
    publisher->implementation_identifier, RMW_GURUMDDS_ID,
    return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  GurumddsPublisherInfo * publisher_info = static_cast<GurumddsPublisherInfo *>(publisher->data);
  if (publisher_info == nullptr) {
    RMW_SET_ERROR_MSG("publisher internal data is invalid");
    return RMW_RET_ERROR;
  }

  dds_Duration_t timeout = rmw_time_to_dds(wait_timeout);
  dds_ReturnCode_t ret = dds_DataWriter_wait_for_acknowledgments(
    publisher_info->topic_writer, &timeout);

  if (ret == dds_RETCODE_OK) {
    return RMW_RET_OK;
  } else if (ret == dds_RETCODE_TIMEOUT) {
    return RMW_RET_TIMEOUT;
  } else {
    return RMW_RET_ERROR;
  }
}

rmw_ret_t
rmw_destroy_publisher(rmw_node_t * node, rmw_publisher_t * publisher)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node handle,
    node->implementation_identifier, RMW_GURUMDDS_ID,
    return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    publisher handle,
    publisher->implementation_identifier, RMW_GURUMDDS_ID,
    return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  rmw_context_impl_t * ctx = node->context->impl;

  return __rmw_destroy_publisher(ctx, publisher);
}

rmw_ret_t
rmw_get_gid_for_publisher(const rmw_publisher_t * publisher, rmw_gid_t * gid)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(gid, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    publisher handle,
    publisher->implementation_identifier,
    RMW_GURUMDDS_ID,
    return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  const GurumddsPublisherInfo * info =
    static_cast<const GurumddsPublisherInfo *>(publisher->data);
  if (info == nullptr) {
    RMW_SET_ERROR_MSG("publisher info handle is null");
    return RMW_RET_ERROR;
  }

  *gid = info->publisher_gid;
  return RMW_RET_OK;
}

rmw_ret_t
rmw_publisher_get_actual_qos(
  const rmw_publisher_t * publisher,
  rmw_qos_profile_t * qos)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(qos, RMW_RET_INVALID_ARGUMENT);

  GurumddsPublisherInfo * info = static_cast<GurumddsPublisherInfo *>(publisher->data);
  if (info == nullptr) {
    RMW_SET_ERROR_MSG("publisher internal data is invalid");
    return RMW_RET_ERROR;
  }

  dds_DataWriter * data_writer = info->topic_writer;
  if (data_writer == nullptr) {
    RMW_SET_ERROR_MSG("publisher internal data writer is invalid");
    return RMW_RET_ERROR;
  }

  dds_DataWriterQos dds_qos;
  dds_ReturnCode_t ret = dds_DataWriter_get_qos(data_writer, &dds_qos);
  if (ret != dds_RETCODE_OK) {
    RMW_SET_ERROR_MSG("publisher can't get data writer qos policies");
    return RMW_RET_ERROR;
  }

  qos->reliability = convert_reliability(dds_qos.reliability);
  qos->durability = convert_durability(dds_qos.durability);
  qos->deadline = convert_deadline(dds_qos.deadline);
  qos->lifespan = convert_lifespan(dds_qos.lifespan);
  qos->liveliness = convert_liveliness(dds_qos.liveliness);
  qos->liveliness_lease_duration = convert_liveliness_lease_duration(dds_qos.liveliness);
  qos->history = convert_history(dds_qos.history);
  qos->depth = static_cast<size_t>(dds_qos.history.depth);

  ret = dds_DataWriterQos_finalize(&dds_qos);
  if (ret != dds_RETCODE_OK) {
    RMW_SET_ERROR_MSG("failed to finalize datawriter qos");
    return RMW_RET_ERROR;
  }

  return RMW_RET_OK;
}

rmw_ret_t
rmw_publish(
  const rmw_publisher_t * publisher,
  const void * ros_message,
  rmw_publisher_allocation_t * allocation)
{
  (void)allocation;
  RCUTILS_CHECK_FOR_NULL_WITH_MSG(
    publisher, "publisher pointer is null",
    return RMW_RET_INVALID_ARGUMENT);
  RCUTILS_CHECK_FOR_NULL_WITH_MSG(
    ros_message, "ros_message pointer is null",
    return RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    publisher,
    publisher->implementation_identifier,
    RMW_GURUMDDS_ID,
    return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto info = static_cast<GurumddsPublisherInfo *>(publisher->data);
  RCUTILS_CHECK_FOR_NULL_WITH_MSG(info, "publisher info pointer is null", return RMW_RET_ERROR);

  dds_DataWriter * topic_writer = info->topic_writer;
  RCUTILS_CHECK_FOR_NULL_WITH_MSG(info, "topic writer is null", return RMW_RET_ERROR);

  const rosidl_message_type_support_t * rosidl_typesupport = info->rosidl_message_typesupport;
  if (rosidl_typesupport == nullptr) {
    RMW_SET_ERROR_MSG("rosidl typesupport handle is null");
    return RMW_RET_ERROR;
  }

  size_t size = 0;
  void * dds_message = allocate_message(
    rosidl_typesupport->data,
    rosidl_typesupport->typesupport_identifier,
    ros_message,
    &size,
    false
  );
  if (dds_message == nullptr) {
    // Error message already set
    return RMW_RET_ERROR;
  }

  bool result = serialize_ros_to_cdr(
    rosidl_typesupport->data,
    rosidl_typesupport->typesupport_identifier,
    ros_message,
    dds_message,
    size
  );
  if (!result) {
    RMW_SET_ERROR_MSG("failed to serialize message");
    free(dds_message);
    return RMW_RET_ERROR;
  }

  dds_SampleInfoEx sampleinfo_ex;
  memset(&sampleinfo_ex, 0, sizeof(dds_SampleInfoEx));
  ros_sn_to_dds_sn(++info->sequence_number, &sampleinfo_ex.seq);
  auto publisher_gid =
    reinterpret_cast<GurumddsPublisherGID *>(info->publisher_gid.data);
  ros_guid_to_dds_guid(
    reinterpret_cast<int8_t *>(publisher_gid->publication_handle),
    reinterpret_cast<int8_t *>(&sampleinfo_ex.src_guid));

  dds_ReturnCode_t ret = dds_DataWriter_raw_write_w_sampleinfoex(
    topic_writer, dds_message, size,
    &sampleinfo_ex);
  const char * errstr;
  if (ret == dds_RETCODE_OK) {
    errstr = "dds_RETCODE_OK";
  } else if (ret == dds_RETCODE_TIMEOUT) {
    errstr = "dds_RETCODE_TIMEOUT";
  } else if (ret == dds_RETCODE_OUT_OF_RESOURCES) {
    errstr = "dds_RETCODE_OUT_OF_RESOURCES";
  } else {
    errstr = "dds_RETCODE_ERROR";
  }

  if (ret != dds_RETCODE_OK) {
    std::stringstream errmsg;
    errmsg << "failed to publish data: " << errstr << ", " << ret;
    RMW_SET_ERROR_MSG(errmsg.str().c_str());
    free(dds_message);
    return RMW_RET_ERROR;
  }
  const char * topic_name = dds_Topic_get_name(dds_DataWriter_get_topic(topic_writer));
  RCUTILS_LOG_DEBUG_NAMED(RMW_GURUMDDS_ID, "Published data on topic %s", topic_name);

  free(dds_message);

  return RMW_RET_OK;
}

rmw_ret_t
rmw_publish_serialized_message(
  const rmw_publisher_t * publisher,
  const rmw_serialized_message_t * serialized_message,
  rmw_publisher_allocation_t * allocation)
{
  (void)allocation;
  RCUTILS_CHECK_FOR_NULL_WITH_MSG(
    publisher, "publisher pointer is null",
    return RMW_RET_INVALID_ARGUMENT);
  RCUTILS_CHECK_FOR_NULL_WITH_MSG(
    serialized_message, "serialized_message pointer is null",
    return RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    publisher,
    publisher->implementation_identifier,
    RMW_GURUMDDS_ID,
    return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto info = static_cast<GurumddsPublisherInfo *>(publisher->data);
  RCUTILS_CHECK_FOR_NULL_WITH_MSG(info, "publisher info pointer is null", return RMW_RET_ERROR);

  dds_DataWriter * topic_writer = info->topic_writer;
  RCUTILS_CHECK_FOR_NULL_WITH_MSG(info, "topic writer is null", return RMW_RET_ERROR);

  dds_SampleInfoEx sampleinfo_ex;
  memset(&sampleinfo_ex, 0, sizeof(dds_SampleInfoEx));
  ros_sn_to_dds_sn(++info->sequence_number, &sampleinfo_ex.seq);
  auto publisher_gid =
    reinterpret_cast<GurumddsPublisherGID *>(info->publisher_gid.data);
  ros_guid_to_dds_guid(
    reinterpret_cast<int8_t *>(publisher_gid->publication_handle),
    reinterpret_cast<int8_t *>(&sampleinfo_ex.src_guid));

  dds_ReturnCode_t ret = dds_DataWriter_raw_write_w_sampleinfoex(
    topic_writer,
    serialized_message->buffer,
    static_cast<uint32_t>(serialized_message->buffer_length),
    &sampleinfo_ex
  );
  const char * errstr;
  if (ret == dds_RETCODE_OK) {
    errstr = "dds_RETCODE_OK";
  } else if (ret == dds_RETCODE_TIMEOUT) {
    errstr = "dds_RETCODE_TIMEOUT";
  } else if (ret == dds_RETCODE_OUT_OF_RESOURCES) {
    errstr = "dds_RETCODE_OUT_OF_RESOURCES";
  } else {
    errstr = "dds_RETCODE_ERROR";
  }

  if (ret != dds_RETCODE_OK) {
    std::stringstream errmsg;
    errmsg << "failed to publish data: " << errstr << ", " << ret;
    RMW_SET_ERROR_MSG(errmsg.str().c_str());
    return RMW_RET_ERROR;
  }
  const char * topic_name = dds_Topic_get_name(dds_DataWriter_get_topic(topic_writer));
  RCUTILS_LOG_DEBUG_NAMED(RMW_GURUMDDS_ID, "Published data on topic %s", topic_name);

  return RMW_RET_OK;
}

rmw_ret_t
rmw_publish_loaned_message(
  const rmw_publisher_t * publisher,
  void * ros_message,
  rmw_publisher_allocation_t * allocation)
{
  (void)publisher;
  (void)ros_message;
  (void)allocation;

  RMW_SET_ERROR_MSG("rmw_publish_loaned_message is not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t
rmw_borrow_loaned_message(
  const rmw_publisher_t * publisher,
  const rosidl_message_type_support_t * type_support,
  void ** ros_message)
{
  (void)publisher;
  (void)type_support;
  (void)ros_message;

  RMW_SET_ERROR_MSG("rmw_borrow_loaned_message is not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t
rmw_return_loaned_message_from_publisher(
  const rmw_publisher_t * publisher,
  void * loaned_message)
{
  (void)publisher;
  (void)loaned_message;

  RMW_SET_ERROR_MSG("rmw_return_loaned_message_from_publisher is not supported");
  return RMW_RET_UNSUPPORTED;
}
}  // extern "C"
