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

rmw_ret_t
graph_cache_initialize(rmw_context_impl_t * const ctx);

rmw_ret_t
graph_cache_finalize(rmw_context_impl_t * const ctx);

#endif  // RMW_GURUMDDS_CPP__GRAPH_CACHE_HPP_
