/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

namespace paimon::parquet {

class ParquetMetrics {
 public:
    static inline const char READ_ROW_GROUPS_TOTAL[] = "parquet.read.row_groups.total";
    static inline const char READ_ROW_GROUPS_FILTERED[] = "parquet.read.row_groups.filtered";
    static inline const char READ_ROWS[] = "parquet.read.rows";
    static inline const char READ_BATCH_COUNT[] = "parquet.read.batch.count";
    static inline const char READ_NEXT_BATCH_LATENCY_MS[] = "parquet.read.next_batch.latency.ms";
};

}  // namespace paimon::parquet
