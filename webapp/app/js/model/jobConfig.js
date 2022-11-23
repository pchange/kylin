/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

KylinApp.constant('jobConfig', {
  allStatus: [
    {name: '新', value: 0, count: ''},
    {name: '等待中', value: 1, count: ''},
    {name: '运行中', value: 2, count: ''},
    {name: '已停止', value: 32, count: ''},
    {name: '已完成', value: 4, count: ''},
    {name: '错误', value: 8, count: ''},
    {name: '已取消', value: 16, count: ''}
  ],
  timeFilter: [
    {name: '今天', value: 0},
    {name: '昨天', value: 1},
    {name: '上周', value: 2},
    {name: '上个月', value: 3},
    {name: '上一年', value: 4},
    {name: '全部', value: 5},
  ],
  theaditems: [
    {attr: 'name', name: '任务名称'},
    {attr: 'related_object', name: 'Object'},
    {attr: 'progress', name: '进度'},
    {attr: 'last_modified', name: '上一次修改时间'},
    {attr: 'duration', name: '时长'}
  ],
  searchMode: [
    {name: 'CUBING', value: 'CUBING_ONLY'},
    {name: 'CHECK POINT', value: 'CHECKPOINT_ONLY'},
    {name: 'CARDINALITY', value: 'CARDINALITY_ONLY'},
    {name: 'SNAPSHOT', value: 'SNAPSHOT_ONLY'},
    {name: 'ALL', value: 'ALL'}
  ],
  queryitems: [
  {attr: 'server', name: 'Server'},
  {attr: 'user', name: '创建人'},
  {attr: 'cube', name: 'Hit Cube'},
  {attr: 'sql', name: 'Sql'},
  {attr: 'adj', name: '描述'},
  {attr: 'running_seconds', name: '运行时长'},
  {attr: 'start_time', name: '开始时间'},
  {attr: 'last_modified', name: '最后修改'},
  {attr: 'thread', name: 'Thread'}
]

});
