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

'use strict';

KylinApp.controller('AdminStreamingCtrl', function ($scope, $timeout, $modal, AdminStreamingService, MessageService, loadingRequest, UserService, ProjectModel, SweetAlert, $filter) {
  //TODO move config to model dir
  var config = liquidFillGaugeDefaultSettings();
  config.waveCount = 3;
  config.waveAnimate = false;
  config.waveRise = false;
  config.waveHeightScaling = false;
  config.waveOffset = 0.25;
  config.textSize = 0.9;
  config.textVertPosition = 0.7;
  config.waveHeight = 0;
  config.displayPercent = false;
  config.circleColor = '#00a65a';
  config.waveColor = '#00a65a';
  config.textColor = '#000000';
  config.waveTextColor = '#ffffff';

  config.maxValue = 20000 // TODO change it to configurable

  var inactiveConfig = angular.copy(config);
  inactiveConfig.circleColor = '#dd4b39';
  inactiveConfig.waveColor = '#dd4b39';

  var unreachableConfig = angular.copy(config);
  unreachableConfig.circleColor = '#d2d6de';
  unreachableConfig.waveColor = '#d2d6de';

  var warningConfig = angular.copy(config);
  warningConfig.circleColor = '#f39c12';
  warningConfig.waveColor = '#f39c12';

  var availableConfig = angular.copy(config);
  availableConfig.circleColor = '#3c8dbc';
  availableConfig.waveColor = '#3c8dbc';

  $scope.listReplicaSet = function(callback) {
    AdminStreamingService.getClusterState({}, function(data) {
      $scope.replicaSetStates = data.rs_states;
      $scope.availableReceiver = data.available_receivers;
      $timeout(function() {
        angular.forEach($scope.replicaSetStates, function(replicaSetState, rsIndex) {
          var rsId = 'rs-' + replicaSetState.rs_id;
          angular.forEach(replicaSetState.receiver_states, function(receiverState, reIndex) {
            var reId = rsId + '-re-' + $filter('formatId')(receiverState.receiver.host) + '_' + receiverState.receiver.port;
            if (!$scope[reId]) {
              drawLiquidChart(reId, receiverState.rate, receiverState.state);
            } else {
              delete $scope[reId];
              drawLiquidChart(reId, receiverState.rate, receiverState.state);
            }
          });
        });
        angular.forEach($scope.availableReceiver, function(receiverState, reIndex) {
          var reId = 're-' + $filter('formatId')(receiverState.receiver.host) + '_' + receiverState.receiver.port;
          if (!$scope[reId]) {
            drawLiquidChart(reId, receiverState.rate, receiverState.state, 'isAvailable');
          } else {
            delete $scope[reId];
            drawLiquidChart(reId, receiverState.rate, receiverState.state, 'isAvailable');
          }
        });
        callback && callback();
      }, 100);
    }, function(e) {
      if (e.data && e.data.exception) {
        var message = e.data.exception;
        var msg = !!(message) ? message : '获取副本集失败';
        SweetAlert.swal('提示...', msg, 'error');
      } else {
        SweetAlert.swal('提示...', '获取副本集失败', 'error');
      }
      callback && callback();
    });
  };

  function drawLiquidChart(reId, rate, state, isAvailable) {
    var value = Number(rate).toFixed(0);
    if ('DOWN' === state) {
      $scope[reId] = loadLiquidFillGauge(reId, value, angular.copy(inactiveConfig));
    } else if ('UNREACHABLE' === state) {
      $scope[reId] = loadLiquidFillGauge(reId, value, angular.copy(unreachableConfig));
    } else if ('WARN' === state) {
      $scope[reId] = loadLiquidFillGauge(reId, value, angular.copy(warningConfig));
    } else {
      if (isAvailable === 'isAvailable') {
        $scope[reId] = loadLiquidFillGauge(reId, value, angular.copy(availableConfig));
      } else {
        $scope[reId] = loadLiquidFillGauge(reId, value, angular.copy(config));
      }
    }
  };

  $scope.editReplicaSet = function(replicaSet){
    $modal.open({
      templateUrl: 'editReplicaSet.html',
      controller: function ($scope, scope, $modalInstance, replicaSet, AdminStreamingService, availableReceiver) {
        $scope.replicaSet = replicaSet;
        $scope.availableNodes = availableReceiver;

        $scope.node = {
          selected : ''
        };

        $scope.cancel = function () {
          $modalInstance.dismiss('cancel');
        };

        $scope.removeReceiver = function(receiver) {
          var nodeId = receiver.receiver.host + '_' + receiver.receiver.port;
          if (nodeId !== '_') {
            loadingRequest.show();
            AdminStreamingService.removeNodeToReplicaSet({replicaSetId: $scope.replicaSet.rs_id, nodeId: nodeId}, {}, function(data) {
              AdminStreamingService.getClusterState({},function(data) {
                var newReplicaSet = _.find(data.rs_states, function(item){
                  return item.rs_id == $scope.replicaSet.rs_id;
                });
                $scope.replicaSet = newReplicaSet;
                scope.listReplicaSet(function() {
                  $scope.availableNodes = scope.availableReceiver;
                  loadingRequest.hide();
                });
              }, function(e) {
                scope.listReplicaSet(function() {
                  $scope.availableNodes = scope.availableReceiver;
                  loadingRequest.hide();
                });
                errorMessage(e, '获取副本集失败');
              });
            }, function(e) {
              scope.listReplicaSet(function() {
                loadingRequest.hide();
              });
              errorMessage(e, 'receiver 删除失败');
            });
          }
        };

        $scope.addNodeToReplicaSet = function() {
          if ($scope.node.selected) {
            loadingRequest.show();
            AdminStreamingService.addNodeToReplicaSet({replicaSetId: $scope.replicaSet.rs_id, nodeId: $scope.node.selected}, {}, function(data) {
              AdminStreamingService.getClusterState({},function(data) {
                var newReplicaSet = _.find(data.rs_states, function(item){
                  return item.rs_id == $scope.replicaSet.rs_id;
                });
                $scope.replicaSet = newReplicaSet;
                $scope.node.selected = '';
                scope.listReplicaSet(function() {
                  $scope.availableNodes = scope.availableReceiver;
                  loadingRequest.hide();
                });
              }, function(e) {
                scope.listReplicaSet(function() {
                  $scope.availableNodes = scope.availableReceiver;
                  loadingRequest.hide();
                });
                errorMessage(e, '获取副本集失败');
              });
            }, function(e) {
              scope.listReplicaSet(function() {
                loadingRequest.hide();
              });
              errorMessage(e, '添加节点失败');
            });
          } else {
             errorMessage(e, '添加节点失败');
          }
        };

        function errorMessage(e, errMsg) {
          $modalInstance.dismiss('cancel');
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : errMsg;
            SweetAlert.swal('提示...', msg, 'error');
          } else {
            SweetAlert.swal('提示...', errMsg, 'error');
          }
        };
      },
      windowClass: 'receiver-stats-modal',
      resolve: {
        replicaSet: function () {
          return replicaSet;
        },
        scope: function() {
          return $scope;
        },
        availableReceiver : function() {
          return $scope.availableReceiver;
        }
      }
    });
  };

  $scope.createReplicaSet = function() {
    $modal.open({
      templateUrl: 'createReplicaSet.html',
      controller: function ($scope, scope, $modalInstance, nodes, AdminStreamingService) {
        $scope.availableNodes = nodes;
        $scope.node = {
          selected: []
        };

        $scope.saveReplicaSet = function() {
          if ($scope.node.selected.length) {
            AdminStreamingService.createReplicaSet({}, {
              nodes: $scope.node.selected
            }, function(data) {
              scope.listReplicaSet();
              $modalInstance.close();
              SweetAlert.swal('成功!', '添加节点成功', 'success');
            }, function(e) {
              if (e.data && e.data.exception) {
                var message = e.data.exception;
                var msg = !!(message) ? message : '添加节点失败';
                SweetAlert.swal('提示...', msg, 'error');
              } else {
                SweetAlert.swal('提示...', '添加节点失败', 'error');
              }
              scope.listReplicaSet();
            });
          } else {
            SweetAlert.swal('提示...', "请选择节点", 'info');
          }
        };

        $scope.cancel = function () {
          $modalInstance.dismiss('cancel');
        };

      },
      windowClass: 'receiver-stats-modal',
      resolve: {
        scope: function() {
          return $scope;
        },
        nodes: function() {
          var availableReceiver = [];
          angular.forEach($scope.availableReceiver, function(item) {
            availableReceiver.push(item.receiver);
          });
          return availableReceiver;
        }
      }
    });
  };

  $scope.removeReplicaSet = function(replicaSet) {
    SweetAlert.swal({
      title: '',
      text: '确定删除副本集['+replicaSet.rs_id+']? ',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "确定",
      cancelButtonText: "取消",
      closeOnConfirm: true
    }, function(isConfirm) {
      if(isConfirm){
        AdminStreamingService.removeReplicaSet({replicaSetId: replicaSet.rs_id}, {}, function (result) {
          $scope.listReplicaSet();
          SweetAlert.swal('成功!', '副本集删除成功', 'success');
        }, function(e){
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : '副本集删除失败';
            SweetAlert.swal('提示...', msg, 'error');
          } else {
            SweetAlert.swal('提示...', '副本集删除失败', 'error');
          }
          $scope.listReplicaSet();
        });
      }
    });
  };

  $scope.removeReceiver = function(receiverID) {
    SweetAlert.swal({
      title: '',
      text: '确定要删除receiver \'' + receiverID + '\'?',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "确定",
      cancelButtonText: "取消",
      closeOnConfirm: true
    }, function(isConfirm) {
      if(isConfirm){
        AdminStreamingService.removeReceiver({receiverID: receiverID}, {}, function (result) {
          SweetAlert.swal({title: '成功!', text:'Receiver 删除成功'}, function (isConfirm) {
            if (isConfirm) {
              $timeout(function() {}, 2000);
              $scope.listReplicaSet();
            }
          });
        }, function(e){
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : 'Receiver 删除失败';
            SweetAlert.swal('提示...', msg, 'error');
          } else {
            SweetAlert.swal('提示...', 'Receiver 删除失败', 'error');
          }
        });
      }
    });
  };

}).filter('formatId', function () {
  return function (item) {
    return item.split('.').join('_');
  }
});


KylinApp.controller('StreamingReceiverCtrl', function($scope, $routeParams, $modal, AdminStreamingService, MessageService, loadingRequest, UserService, ProjectModel){
  $scope.receiverId = $routeParams.receiverId;
  $scope.receiverServer = $scope.receiverId.split('_')[0];

  $scope.getReceiverStats = function() {
    AdminStreamingService.getReceiverStats({nodeId: $scope.receiverId}, function(data){
      $scope.receiverStats = data;
    }, function(e) {
      if (e.data && e.data.exception) {
        var message = e.data.exception;
        var msg = !!(message) ? message : '获取 receiver 统计数据失败';
        SweetAlert.swal('提示...', msg, 'error');
      } else {
        SweetAlert.swal('提示...', '获取 receiver 统计数据失败', 'error');
      }
    });
  }

  $scope.moreDetails = function(receiverCubeStats, cubeName) {
    $modal.open({
      templateUrl: 'receiverCubeDetails.html',
      controller: function ($scope, scope, $modalInstance, receiverCubeStats, cubeName, AdminStreamingService) {
        $scope.receiverCubeStats = receiverCubeStats;
        $scope.cubeName = cubeName;

        $scope.cancel = function () {
          $modalInstance.dismiss('cancel');
        };

      },
      resolve: {
        scope: function() {
          return $scope;
        },
        receiverCubeStats: function() {
          return receiverCubeStats;
        },
        cubeName: function() {
          return cubeName;
        }
      }
    });
  };

  $scope.getReceiverStats();

});
