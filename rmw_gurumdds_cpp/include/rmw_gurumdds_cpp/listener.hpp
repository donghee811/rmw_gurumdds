#define DDS_EVENT_CALLBACK_FN(event_type, EVENT_TYPE, entity_type) \
  static void on_ ## event_type ## _fn( \
    const dds_ ## entity_type * entity, \
    const dds_ ## EVENT_TYPE ## Status * status) \
  { \
    (void)status; \
    dds_ ## entity_type * _entity = const_cast<dds_ ## entity_type *>(entity); \
    auto data = static_cast<GurumddsUserCallback *>(dds_## entity_type ## _get_listener_context(_entity)); \
    std::lock_guard<std::mutex> guard(data->mutex); \
    auto callback = data->event_callback[dds_ ## EVENT_TYPE ## StatusId]; \
    if (callback) { \
      callback(data->event_data[dds_ ## EVENT_TYPE ## StatusId], 1); \
    } else { \
      data->event_unread_count[dds_ ## EVENT_TYPE ## StatusId]++; \
    } \
  }

// Define event callback functions
DDS_EVENT_CALLBACK_FN(requested_deadline_missed, RequestedDeadlineMissed, DataReader)
DDS_EVENT_CALLBACK_FN(requested_incompatible_qos, RequestedIncompatibleQos, DataReader)
DDS_EVENT_CALLBACK_FN(sample_lost, SampleLost, DataReader)
DDS_EVENT_CALLBACK_FN(liveliness_changed, LivelinessChanged, DataReader)
DDS_EVENT_CALLBACK_FN(liveliness_lost, LivelinessLost, DataWriter)
DDS_EVENT_CALLBACK_FN(offered_deadline_missed, OfferedDeadlineMissed, DataWriter)
DDS_EVENT_CALLBACK_FN(offered_incompatible_qos, OfferedIncompatibleQos, DataWriter)

static void
listener_set_event_callbacks(dds_DataReaderListener * rl, dds_DataWriterListener * wl) {
  if (rl != nullptr) {
    rl->on_requested_deadline_missed = on_requested_deadline_missed_fn;
    rl->on_requested_incompatible_qos = on_requested_incompatible_qos_fn;
    rl->on_sample_lost = on_sample_lost_fn;
    rl->on_liveliness_changed = on_liveliness_changed_fn;
  }

  if (wl != nullptr) {
    wl->on_liveliness_lost = on_liveliness_lost_fn;
    wl->on_offered_deadline_missed = on_offered_deadline_missed_fn;
    wl->on_offered_incompatible_qos = on_offered_incompatible_qos_fn;
  }
}
