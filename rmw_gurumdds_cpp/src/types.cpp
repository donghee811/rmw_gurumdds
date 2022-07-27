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

#include "rmw/impl/cpp/key_value.hpp"

#include "rmw_gurumdds_cpp/event_converter.hpp"
#include "rmw_gurumdds_cpp/gid.hpp"
#include "rmw_gurumdds_cpp/graph_cache.hpp"
#include "rmw_gurumdds_cpp/guid.hpp"
#include "rmw_gurumdds_cpp/qos.hpp"
#include "rmw_gurumdds_cpp/rmw_context_impl.hpp"
#include "rmw_gurumdds_cpp/types.hpp"

rmw_ret_t GurumddsPublisherInfo::get_status(
  dds_StatusMask mask,
  void * event)
{
  if (mask == dds_LIVELINESS_LOST_STATUS) {
    dds_LivelinessLostStatus status;
    dds_ReturnCode_t dds_ret =
      dds_DataWriter_get_liveliness_lost_status(this->dds_writer, &status);
    rmw_ret_t rmw_ret = check_dds_ret_code(dds_ret);
    if (rmw_ret != RMW_RET_OK) {
      return rmw_ret;
    }

    auto rmw_status = static_cast<rmw_liveliness_lost_status_t *>(event);
    rmw_status->total_count = status.total_count;
    rmw_status->total_count_change = status.total_count_change;
  } else if (mask == dds_OFFERED_DEADLINE_MISSED_STATUS) {
    dds_OfferedDeadlineMissedStatus status;
    dds_ReturnCode_t dds_ret =
      dds_DataWriter_get_offered_deadline_missed_status(this->dds_writer, &status);
    rmw_ret_t rmw_ret = check_dds_ret_code(dds_ret);
    if (rmw_ret != RMW_RET_OK) {
      return rmw_ret;
    }

    auto rmw_status = static_cast<rmw_offered_deadline_missed_status_t *>(event);
    rmw_status->total_count = status.total_count;
    rmw_status->total_count_change = status.total_count_change;
  } else if (mask == dds_OFFERED_INCOMPATIBLE_QOS_STATUS) {
    dds_OfferedIncompatibleQosStatus status;
    dds_ReturnCode_t dds_ret =
      dds_DataWriter_get_offered_incompatible_qos_status(this->dds_writer, &status);
    rmw_ret_t rmw_ret = check_dds_ret_code(dds_ret);
    if (rmw_ret != RMW_RET_OK) {
      return rmw_ret;
    }

    auto rmw_status = static_cast<rmw_offered_qos_incompatible_event_status_t *>(event);
    rmw_status->total_count = status.total_count;
    rmw_status->total_count_change = status.total_count_change;
    rmw_status->last_policy_kind = convert_qos_policy(status.last_policy_id);
  } else {
    return RMW_RET_UNSUPPORTED;
  }
  return RMW_RET_OK;
}

dds_StatusCondition * GurumddsPublisherInfo::get_statuscondition()
{
  return dds_DataWriter_get_statuscondition(this->dds_writer);
}

dds_StatusMask GurumddsPublisherInfo::get_status_changes()
{
  return dds_DataWriter_get_status_changes(this->dds_writer);
}

rmw_ret_t GurumddsSubscriberInfo::get_status(
  dds_StatusMask mask,
  void * event)
{
  if (mask == dds_LIVELINESS_CHANGED_STATUS) {
    dds_LivelinessChangedStatus status;
    dds_ReturnCode_t dds_ret =
      dds_DataReader_get_liveliness_changed_status(this->dds_reader, &status);
    rmw_ret_t rmw_ret = check_dds_ret_code(dds_ret);
    if (rmw_ret != RMW_RET_OK) {
      return rmw_ret;
    }

    auto rmw_status = static_cast<rmw_liveliness_changed_status_t *>(event);
    rmw_status->alive_count = status.alive_count;
    rmw_status->not_alive_count = status.not_alive_count;
    rmw_status->alive_count_change = status.alive_count_change;
    rmw_status->not_alive_count_change =
      status.not_alive_count_change;
  } else if (mask == dds_REQUESTED_DEADLINE_MISSED_STATUS) {
    dds_RequestedDeadlineMissedStatus status;
    dds_ReturnCode_t dds_ret =
      dds_DataReader_get_requested_deadline_missed_status(this->dds_reader, &status);
    rmw_ret_t rmw_ret = check_dds_ret_code(dds_ret);
    if (rmw_ret != RMW_RET_OK) {
      return rmw_ret;
    }

    auto rmw_status =
      static_cast<rmw_requested_deadline_missed_status_t *>(event);
    rmw_status->total_count = status.total_count;
    rmw_status->total_count_change = status.total_count_change;
  } else if (mask == dds_REQUESTED_INCOMPATIBLE_QOS_STATUS) {
    dds_RequestedIncompatibleQosStatus status;
    dds_ReturnCode_t dds_ret =
      dds_DataReader_get_requested_incompatible_qos_status(this->dds_reader, &status);
    rmw_ret_t rmw_ret = check_dds_ret_code(dds_ret);
    if (rmw_ret != RMW_RET_OK) {
      return rmw_ret;
    }

    auto rmw_status = static_cast<rmw_requested_qos_incompatible_event_status_t *>(event);
    rmw_status->total_count = status.total_count;
    rmw_status->total_count_change = status.total_count_change;
    rmw_status->last_policy_kind = convert_qos_policy(status.last_policy_id);
  } else if (mask == dds_SAMPLE_LOST_STATUS) {
    dds_SampleLostStatus status;
    dds_ReturnCode_t dds_ret =
      dds_DataReader_get_sample_lost_status(this->dds_reader, &status);
    rmw_ret_t rmw_ret = check_dds_ret_code(dds_ret);
    if (rmw_ret != RMW_RET_OK) {
      return rmw_ret;
    }

    auto rmw_status = static_cast<rmw_message_lost_status_t *>(event);
    rmw_status->total_count = status.total_count;
    rmw_status->total_count_change = status.total_count_change;
  } else {
    return RMW_RET_UNSUPPORTED;
  }
  return RMW_RET_OK;
}

dds_StatusCondition * GurumddsSubscriberInfo::get_statuscondition()
{
  return dds_DataReader_get_statuscondition(this->dds_reader);
}

dds_StatusMask GurumddsSubscriberInfo::get_status_changes()
{
  return dds_DataReader_get_status_changes(this->dds_reader);
}

static std::map<std::string, std::vector<uint8_t>>
__parse_map(uint8_t * const data, const uint32_t data_len)
{
  std::vector<uint8_t> data_vec(data, data + data_len);
  std::map<std::string, std::vector<uint8_t>> map =
    rmw::impl::cpp::parse_key_value(data_vec);

  return map;
}

static rmw_ret_t
__get_user_data_key(
  dds_ParticipantBuiltinTopicData * data,
  const std::string key,
  std::string & value,
  bool & found)
{
  found = false;
  uint8_t * user_data =
    static_cast<uint8_t *>(data->user_data.value);
  const uint32_t user_data_len = data->user_data.size;
  if (nullptr == user_data || user_data_len == 0) {
    return RMW_RET_OK;
  }

  auto map = __parse_map(user_data, user_data_len);
  auto name_found = map.find(key);
  if (name_found != map.end()) {
    value = std::string(name_found->second.begin(), name_found->second.end());
    found = true;
  }

  return RMW_RET_OK;
}

rmw_ret_t
part_on_data_available(rmw_context_impl_t * const ctx)
{
  dds_DataSeq * samples = dds_DataSeq_create(8);
  if (samples == nullptr) {
    RMW_SET_ERROR_MSG("failed to create data sample sequence");
    return RMW_RET_ERROR;
  }
  dds_SampleInfoSeq * infos = dds_SampleInfoSeq_create(8);
  if (infos == nullptr) {
    dds_DataSeq_delete(samples);
    RMW_SET_ERROR_MSG("failed to create sample info sequence");
    return RMW_RET_ERROR;
  }

  dds_ReturnCode_t rc = dds_RETCODE_OK;

  do {
    rc = dds_DataReader_take(
      ctx->builtin_participant_datareader,
      samples,
      infos,
      dds_LENGTH_UNLIMITED,
      dds_ANY_SAMPLE_STATE,
      dds_ANY_VIEW_STATE,
      dds_ANY_INSTANCE_STATE);
  
    if (rc != dds_RETCODE_OK) {
      continue;
    }

    const uint32_t data_len = dds_DataSeq_length(samples);
    for (uint32_t i = 0; i < data_len; i++) {
      GuidPrefix_t dp_guid_prefix;
      dds_GUID_t dp_guid;
      rmw_gid_t dp_gid;
      memset(&dp_guid, 0, sizeof(dp_guid));
      dds_ParticipantBuiltinTopicData * pbtd =
        reinterpret_cast<dds_ParticipantBuiltinTopicData *>(dds_DataSeq_get(samples, i));
      dds_SampleInfo * info = dds_SampleInfoSeq_get(infos, i);
      if (reinterpret_cast<void *>(info->instance_handle) == NULL) {
        continue;
      }
      if (!info->valid_data) {
        if (info->instance_state == dds_NOT_ALIVE_DISPOSED_INSTANCE_STATE ||
          info->instance_state == dds_NOT_ALIVE_NO_WRITERS_INSTANCE_STATE)
        {
          continue;
        } else {
          RMW_SET_ERROR_MSG("[discovery thread] ignored participant invalid data");
        }
        continue;
      }
      dds_BuiltinTopicKey_to_GUID(&dp_guid_prefix, pbtd->key);
      memcpy(dp_guid.prefix, dp_guid_prefix.value, sizeof(dp_guid_prefix.value));
      guid_to_gid(dp_guid, dp_gid);

      std::string enclave_str;
      bool enclave_found;

      rc = __get_user_data_key(pbtd, "enclave", enclave_str, enclave_found);
      if (RMW_RET_OK != rc) {
        RMW_SET_ERROR_MSG("failed to parse user data for enclave");
        continue;
      }

      const char * enclave = nullptr;
      if (enclave_found) {
        enclave = enclave_str.c_str();
      }

      if (RMW_RET_OK !=
        graph_add_participant(ctx, dp_gid, enclave))
      {
        RMW_SET_ERROR_MSG("failed to asser remote participant in graph");
        continue;
      }
    }
  } while (dds_RETCODE_OK == rc);

  if (dds_RETCODE_OK !=
    dds_DataReader_return_loan(ctx->builtin_participant_datareader, samples, infos))
  {
    RMW_SET_ERROR_MSG("failed to return loan to dds reader");
    return RMW_RET_ERROR;
  }
  dds_DataSeq_delete(samples);
  dds_SampleInfoSeq_delete(infos);

  return RMW_RET_OK;
}

rmw_ret_t
pub_on_data_available(rmw_context_impl_t * const ctx)
{
  dds_DataSeq * samples = dds_DataSeq_create(8);
  if (samples == nullptr) {
    RMW_SET_ERROR_MSG("failed to create data sample sequence");
    return RMW_RET_ERROR;
  }
  dds_SampleInfoSeq * infos = dds_SampleInfoSeq_create(8);
  if (infos == nullptr) {
    dds_DataSeq_delete(samples);
    RMW_SET_ERROR_MSG("failed to create sample info sequence");
    return RMW_RET_ERROR;
  }

  dds_ReturnCode_t rc = dds_RETCODE_OK;

  do {
    rc = dds_DataReader_take(
      ctx->builtin_publication_datareader,
      samples,
      infos,
      dds_LENGTH_UNLIMITED,
      dds_ANY_SAMPLE_STATE,
      dds_ANY_VIEW_STATE,
      dds_ANY_INSTANCE_STATE);
  
    if (rc != dds_RETCODE_OK) {
      continue;
    }

    const uint32_t data_len = dds_DataSeq_length(samples);
    for (uint32_t i = 0; i < data_len; i++) {
      GuidPrefix_t dp_guid_prefix;
      dds_GUID_t endp_guid;
      dds_GUID_t dp_guid;
      memset(&endp_guid, 0, sizeof(endp_guid));
      memset(&dp_guid, 0, sizeof(dp_guid));
      dds_PublicationBuiltinTopicData * pbtd =
        reinterpret_cast<dds_PublicationBuiltinTopicData *>(dds_DataSeq_get(samples, i));
      dds_SampleInfo * info = dds_SampleInfoSeq_get(infos, i);
      if (reinterpret_cast<void *>(info->instance_handle) == NULL) {
        continue;
      }
      if (!info->valid_data) {
        if (info->instance_state == dds_NOT_ALIVE_DISPOSED_INSTANCE_STATE ||
          info->instance_state == dds_NOT_ALIVE_NO_WRITERS_INSTANCE_STATE)
        {
          continue;
        } else {
          RMW_SET_ERROR_MSG("[discovery thread] ignored publication invalid data");
        }
        continue;
      }
      dds_BuiltinTopicKey_to_GUID(&dp_guid_prefix, pbtd->participant_key);
      memcpy(dp_guid.prefix, dp_guid_prefix.value, sizeof(dp_guid_prefix.value));
      memcpy(endp_guid.prefix, reinterpret_cast<void *>(info->instance_handle), 16);

      graph_add_remote_entity(
        ctx,
        &endp_guid,
        &dp_guid,
        pbtd->topic_name,
        pbtd->type_name,
        &pbtd->reliability,
        &pbtd->durability,
        &pbtd->deadline,
        &pbtd->liveliness,
        &pbtd->lifespan,
        false);
    }
  } while (dds_RETCODE_OK == rc);

  if (dds_RETCODE_OK !=
    dds_DataReader_return_loan(ctx->builtin_publication_datareader, samples, infos))
  {
    RMW_SET_ERROR_MSG("failed to return loan to dds reader");
    return RMW_RET_ERROR;
  }
  dds_DataSeq_delete(samples);
  dds_SampleInfoSeq_delete(infos);

  return RMW_RET_OK;
}

rmw_ret_t
sub_on_data_available(rmw_context_impl_t * const ctx)
{
  dds_DataSeq * samples = dds_DataSeq_create(8);
  if (samples == nullptr) {
    RMW_SET_ERROR_MSG("failed to create data sample sequence");
    return RMW_RET_ERROR;
  }
  dds_SampleInfoSeq * infos = dds_SampleInfoSeq_create(8);
  if (infos == nullptr) {
    dds_DataSeq_delete(samples);
    RMW_SET_ERROR_MSG("failed to create sample info sequence");
    return RMW_RET_ERROR;
  }

  dds_ReturnCode_t rc = dds_RETCODE_OK;

  do {
    rc = dds_DataReader_take(
      ctx->builtin_subscription_datareader,
      samples,
      infos,
      dds_LENGTH_UNLIMITED,
      dds_ANY_SAMPLE_STATE,
      dds_ANY_VIEW_STATE,
      dds_ANY_INSTANCE_STATE);
  
    if (rc != dds_RETCODE_OK) {
      continue;
    }

    const uint32_t data_len = dds_DataSeq_length(samples);
    for (uint32_t i = 0; i < data_len; i++) {
      GuidPrefix_t dp_guid_prefix;
      dds_GUID_t endp_guid;
      dds_GUID_t dp_guid;
      memset(&endp_guid, 0, sizeof(endp_guid));
      memset(&dp_guid, 0, sizeof(dp_guid));
      dds_SubscriptionBuiltinTopicData * sbtd =
        reinterpret_cast<dds_SubscriptionBuiltinTopicData *>(dds_DataSeq_get(samples, i));
      dds_SampleInfo * info = dds_SampleInfoSeq_get(infos, i);
      if (reinterpret_cast<void *>(info->instance_handle) == NULL) {
        continue;
      }
      if (!info->valid_data) {
        if (info->instance_state == dds_NOT_ALIVE_DISPOSED_INSTANCE_STATE ||
          info->instance_state == dds_NOT_ALIVE_NO_WRITERS_INSTANCE_STATE)
        {
          continue;
        } else {
          RMW_SET_ERROR_MSG("[discovery thread] ignored subscription invalid data");
        }
        continue;
      }
      dds_BuiltinTopicKey_to_GUID(&dp_guid_prefix, sbtd->participant_key);
      memcpy(dp_guid.prefix, dp_guid_prefix.value, sizeof(dp_guid_prefix.value));
      memcpy(endp_guid.prefix, reinterpret_cast<void *>(info->instance_handle), 16);

      graph_add_remote_entity(
        ctx,
        &endp_guid,
        &dp_guid,
        sbtd->topic_name,
        sbtd->type_name,
        &sbtd->reliability,
        &sbtd->durability,
        &sbtd->deadline,
        &sbtd->liveliness,
        nullptr,
        true);
    }
  } while (dds_RETCODE_OK == rc);

  if (dds_RETCODE_OK !=
    dds_DataReader_return_loan(ctx->builtin_subscription_datareader, samples, infos))
  {
    RMW_SET_ERROR_MSG("failed to return loan to dds reader");
    return RMW_RET_ERROR;
  }
  dds_DataSeq_delete(samples);
  dds_SampleInfoSeq_delete(infos);

  return RMW_RET_OK;
}
