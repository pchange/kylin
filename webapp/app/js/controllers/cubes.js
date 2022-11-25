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

KylinApp.controller('CubesCtrl', function ($scope, $q, $routeParams, $location, $modal, MessageService, CubeDescService, CubeService, JobService, UserService, ProjectService, SweetAlert, loadingRequest, $log, cubeConfig, ProjectModel, ModelService, MetaModel, CubeList,modelsManager,TableService, kylinConfig, MessageBox, AdminStreamingService, tableConfig) {

    $scope.cubeConfig = cubeConfig;
    $scope.cubeList = CubeList;

    $scope.modelsManager = modelsManager;
    $scope.listParams = {
      cubeName: $routeParams.cubeName,
      projectName: $routeParams.projectName
    };
    if ($routeParams.projectName) {
      $scope.projectModel.setSelectedProject($routeParams.projectName);
    }
    CubeList.removeAll();
    $scope.loading = false;
    $scope.action = {};

    $scope.state = {
      filterAttr: 'create_time_utc', filterReverse: true, reverseColumn: 'create_time_utc',
      dimensionFilter: '', measureFilter: ''
    };

    $scope.refreshCube = function(cube){
      var queryParam = {
        cubeId: cube.name
      };
      var defer = $q.defer();
      CubeService.getCube(queryParam, function(newCube){
        var segmentsLen = newCube.segments && newCube.segments.length || 0
        newCube.input_records_count = 0;
        newCube.size_kb = 0;
        for(var i = segmentsLen - 1;i >= 0;i--){
          var curSeg = newCube.segments[i]
          if(curSeg.status === "READY"){
            newCube.input_records_count += curSeg.input_records
            newCube.size_kb += curSeg.size_kb
            if(newCube.last_build_time === undefined || newCube.last_build_time < curSeg.last_build_time) {
              newCube.last_build_time = curSeg.last_build_time;
            }
          }
        }
        newCube.project = cube.project;
        defer.resolve(newCube);
      },function(e){
        defer.resolve([]);
      })
      return defer.promise;
    }

    $scope.list = function (offset, limit) {
      var defer = $q.defer();
      if (!$scope.projectModel.projects.length) {
        defer.resolve([]);
        return defer.promise;
      }
      offset = (!!offset) ? offset : 0;
      limit = (!!limit) ? limit : 20;

      var queryParam = {offset: offset, limit: limit};
      if ($scope.listParams.cubeName) {
        queryParam.cubeName = $scope.listParams.cubeName;
      }
      queryParam.projectName = $scope.projectModel.selectedProject;

      $scope.loading = true;

      return CubeList.list(queryParam).then(function (resp) {
        angular.forEach($scope.cubeList.cubes,function(cube,index){
        })

        $scope.loading = false;
        defer.resolve(resp);
        return defer.promise;

      },function(resp){
        $scope.loading = false;
        defer.resolve([]);
        SweetAlert.swal('提示...', resp, 'error');
        return defer.promise;
      });
    };

    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
      if (newValue != oldValue || newValue == null) {
        CubeList.removeAll();
        $scope.reload();
      }

    });
    $scope.reload = function () {
      // trigger reload action in pagination directive
      $scope.action.reload = !$scope.action.reload;
    };

    $scope.loadDetail = function (cube) {
      var defer = $q.defer();
      if (cube.detail) {
        defer.resolve(cube);
      } else {
        CubeDescService.query({cube_name: cube.name}, {}, function (detail) {
          if (detail.length > 0 && detail[0].hasOwnProperty("name")) {
            cube.detail = detail[0];
            cube.model = modelsManager.getModel(cube.detail.model_name);
              defer.resolve(cube);

          } else {
            SweetAlert.swal('提示...', "加载cube详细信息失败.", 'error');
          }
        }, function (e) {
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : '操作失败.';
            SweetAlert.swal('提示...', msg, 'error');
          } else {
            SweetAlert.swal('提示...', "操作失败.", 'error');
          }
        });
      }

      return defer.promise;
    };

    $scope.getStreamingInfo = function(cube) {
      var defer = $q.defer();
      if (cube.streamingV2) {
        defer.resolve(cube);
      } else {
        var cubeModel = modelsManager.getModel(cube.model.name);
        var cubeTable = cubeModel.fact_table;
        var cubeProject = $scope.projectModel.selectedProject || cubeModel.project;

        TableService.get({tableName: cubeTable, pro: cubeProject},function(table){
          if (table && table.source_type == 1) {
            cube.streaming = true;
          } else if (table && _.values(tableConfig.streamingSourceType).indexOf(table.source_type) > -1){
            cube.streamingV2 = true;
            if (table.source_type == tableConfig.streamingSourceType.kafka_hive) {
              cube.lambda = true;
            }
          }
          defer.resolve(cube);
        });
      }
      return defer.promise;
    };

    $scope.loadDetailWithStreamingV2Info = function(cube){
      $scope.loadDetail(cube).then(function(cube) {
        return $scope.getStreamingInfo(cube);
      });
    };

    $scope.getTotalSize = function (cubes) {
      var size = 0;
      if (!cubes) {
        return 0;
      }
      else {
        for (var i = 0; i < cubes.length; i++) {
          size += cubes[i].size_kb;
        }
        return $scope.dataSize(size * 1024);
      }
    };

    $scope.getStreamingV2State = function(cube) {
      var defer = $q.defer();
      if (cube.consumeState) {
        defer.resolve(cube);
      } else {
        if (cube.streamingV2) {
          AdminStreamingService.getConsumeState({cubeName:cube.name}, function(state) {
            cube.consumeState = state;
          });
        }
        defer.resolve(cube);
      }
      return defer.promise;
    };

    // List Cube Action
    $scope.listCubeAction = function(cube) {
      $scope.actionLoaded = false;
      $scope.loadDetail(cube).then(function(cube) {
        return $scope.getStreamingInfo(cube);
      }, function(reason) {
        console.error(reason);
        $scope.actionLoaded = true;
      }).then(function(cube) {
        return $scope.getStreamingV2State(cube);
      }, function(reason) {
        console.error(reason);
        $scope.actionLoaded = true;
      }).then(function(cube) {
        $scope.actionLoaded = true;
      });
    };

//    Cube Action
    $scope.enable = function (cube) {
      SweetAlert.swal({
        title: '',
        text: '确定要启用该cube吗？请注意:如果cube结构在停用期间变更，cube所有区段都会因为数据和结构不符而被舍弃.',
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          CubeService.enable({cubeId: cube.name}, {}, function (result) {

            loadingRequest.hide();
            $scope.refreshCube(cube).then(function(_cube){
              if(_cube && _cube.name){
                $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
              }
            });
            MessageBox.successNotify('启用任务已成功提交');
          },function(e){

            loadingRequest.hide();
            if(e.data&& e.data.exception){
              var message =e.data.exception;
              var msg = !!(message) ? message : '操作失败.';
              SweetAlert.swal('提示...', msg, 'error');
            }else{
              SweetAlert.swal('提示...', "操作失败.", 'error');
            }
          });
        }
      });
    };

    $scope.purge = function (cube) {
      SweetAlert.swal({
        title: '',
        text: '确定要清除cube吗? ',
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "确定",
        cancelButtonText: "取消",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          CubeService.purge({cubeId: cube.name}, {}, function (result) {

            loadingRequest.hide();
            $scope.refreshCube(cube).then(function(_cube){
             if(_cube && _cube.name){
                $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
             }
            });
            MessageBox.successNotify('清除任务已成功提交');
          },function(e){
            loadingRequest.hide();
            if(e.data&& e.data.exception){
              var message =e.data.exception;
              var msg = !!(message) ? message : '操作失败.';
              SweetAlert.swal('提示...', msg, 'error');
            }else{
              SweetAlert.swal('提示...', "操作失败.", 'error');
            }
          });
        }
      });
    }

    $scope.disable = function (cube) {

      SweetAlert.swal({
        title: '',
        text: '您确定要禁用该cube吗? ',
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "确定",
        cancelButtonText: "取消",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          CubeService.disable({cubeId: cube.name}, {}, function (result) {
            loadingRequest.hide();
            $scope.refreshCube(cube).then(function(_cube){
              if(_cube && _cube.name){
                $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
              }
            });
            MessageBox.successNotify('禁用任务已成功提交');
          },function(e){

            loadingRequest.hide();
            if(e.data&& e.data.exception){
              var message =e.data.exception;
              var msg = !!(message) ? message : '操作失败.';
              SweetAlert.swal('提示...', msg, 'error');
            }else{
              SweetAlert.swal('提示...', "操作失败.", 'error');
            }
          });
        }

      });
    };

    $scope.dropCube = function (cube) {

      SweetAlert.swal({
        title: '',
        text: " 一旦删除，cube的元数据和数据将被清除，并且无法恢复. ",
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "确定",
        cancelButtonText: "取消",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          CubeService.drop({cubeId: cube.name}, {}, function (result) {
            loadingRequest.hide();
            MessageBox.successNotify('Cube删除成功完成');
            $scope.cubeList.cubes.splice($scope.cubeList.cubes.indexOf(cube),1);
          },function(e){

            loadingRequest.hide();
            if(e.data&& e.data.exception){
              var message =e.data.exception;
              var msg = !!(message) ? message : '操作失败.';
              SweetAlert.swal('提示...', msg, 'error');
            }else{
              SweetAlert.swal('提示...', "操作失败.", 'error');
            }
          });
        }

      });
    };

    $scope.isAutoMigrateCubeEnabled = function(){
      return kylinConfig.isAutoMigrateCubeEnabled();
    };

    $scope.migrateCube = function (cube) {
      SweetAlert.swal({
        title: '',
        text: "该cube将覆盖生产环境中的同一cube" +
        "\n迁移cube需要几分钟时间." +
        "\n请等待.",
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "确定",
        cancelButtonText: "取消",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){
          loadingRequest.show();
          CubeService.autoMigrate({cubeId: cube.name, propName: $scope.projectModel.selectedProject}, {}, function (result) {
            loadingRequest.hide();
            MessageBox.successNotify(cube.name + ' 迁移成功!');
          },function(e){
            loadingRequest.hide();
            SweetAlert.swal('迁移失败!', "请联系管理员.", 'error');
          });
        }
      });
    };

    $scope.startJobSubmit = function (cube) {

      $scope.metaModel={
        model:cube.model
      };

      if(cube.streaming){
        SweetAlert.swal({
          title: '',
          text: "确定要开始构建吗?",
          type: '',
          showCancelButton: true,
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          closeOnConfirm: true
        }, function(isConfirm) {
          if(isConfirm){
            loadingRequest.show();
            CubeService.rebuildStreamingCube(
              {
                cubeId: cube.name
              },
              {
                sourceOffsetStart:0,
                sourceOffsetEnd:'9223372036854775807',
                buildType:'BUILD'
              }, function (job) {
                loadingRequest.hide();
                MessageBox.successNotify('重建任务已成功提交');
              },function(e){

                loadingRequest.hide();
                if(e.data&& e.data.exception){
                  var message =e.data.exception;
                  var msg = !!(message) ? message : '操作失败.';
                  SweetAlert.swal('提示...', msg, 'error');
                }else{
                  SweetAlert.swal('提示...', "操作失败.", 'error');
                }
              });
          }
        })
        return;
      }

      //for batch cube build tip
      if ($scope.metaModel.model.name) {

        //for partition cube build tip
        if ($scope.metaModel.model.partition_desc.partition_date_column) {
          $modal.open({
            templateUrl: 'jobSubmit.html',
            controller: jobSubmitCtrl,
            resolve: {
              cube: function () {
                return cube;
              },
              metaModel:function(){
                return $scope.metaModel;
              },
              buildType: function () {
                return 'BUILD';
              },
              scope:function(){
                return $scope;
              }
            }
          });
        }

        //for not partition cube build tip
        else {
          SweetAlert.swal({
            title: '',
            text: "您确定要开始构建吗?",
            type: '',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "确定",
            cancelButtonText: "取消",
            closeOnConfirm: true
          }, function(isConfirm) {
            if(isConfirm){

              loadingRequest.show();
              CubeService.rebuildCube(
                {
                  cubeId: cube.name
                },
                {
                  buildType: 'BUILD',
                  startTime: 0,
                  endTime: 0
                }, function (job) {

                  loadingRequest.hide();
                  MessageBox.successNotify('重建任务已成功提交');
                },function(e){

                  loadingRequest.hide();
                  if(e.data&& e.data.exception){
                    var message =e.data.exception;
                    var msg = !!(message) ? message : '操作失败.';
                    SweetAlert.swal('提示...', msg, 'error');
                  }else{
                    SweetAlert.swal('提示...', "操作失败.", 'error');
                  }
                });
            }

          });
        }
      }

    };

    $scope.startRefresh = function (cube) {

      $scope.metaModel={
        model:cube.model
      };
      $modal.open({
        templateUrl: 'jobRefresh.html',
        controller: jobSubmitCtrl,
        resolve: {
          cube: function () {
            return cube;
          },
          metaModel:function(){
            return $scope.metaModel;
          },
          buildType: function () {
            return 'REFRESH';
          },
          scope:function(){
            return $scope;
          }
        }
      });
    };

    $scope.cloneCube = function(cube){
      if(!$scope.projectModel.selectedProject){
        SweetAlert.swal('提示...', "克隆前请先关闭项目.", 'info');
        return;
      }

      $modal.open({
        templateUrl: 'cubeClone.html',
        controller: cubeCloneCtrl,
        windowClass:"clone-cube-window",
        resolve: {
          cube: function () {
            return cube;
          }
        }
      });
    }
    $scope.cubeEdit = function (cube) {
      $location.path("cubes/edit/" + cube.name);
    }
    $scope.startMerge = function (cube) {

      $scope.metaModel={
        model:cube.model
      };
      $modal.open({
        templateUrl: 'jobMerge.html',
        controller: jobSubmitCtrl,
        resolve: {
          cube: function () {
            return cube;
          },
          metaModel:function(){
            return $scope.metaModel;
          },
          buildType: function () {
            return 'MERGE';
          },
          scope:function(){
            return $scope;
          }
        }
      });

    };

     $scope.startDeleteSegment = function (cube) {
       $scope.metaModel={
         model:modelsManager.getModelByCube(cube.name)
       };
       $modal.open({
         templateUrl: 'deleteSegment.html',
         controller: deleteSegmentCtrl,
         resolve: {
           cube: function () {
             return cube;
           },
           scope: function() {
             return $scope;
           }
         }
       });
     };

    $scope.startLookupRefresh = function(cube) {
      $scope.metaModel={
        model:cube.model
      };
      $modal.open({
        templateUrl: 'lookupRefresh.html',
        controller: lookupRefreshCtrl,
        resolve: {
          cube: function () {
            return cube;
          },
          scope:function(){
            return $scope;
          }
        }
      });
    };

    $scope.listCubeAccess = function (cube) {
      //check project auth for user
      $scope.cubeProjectEntity = _.find($scope.projectModel.projects, function(project) {return project.name == $scope.projectModel.selectedProject;});

      if (!!cube.uuid) {
        $scope.listAccess(cube, 'CubeInstance');
      }
    };

    // streaming cube action
    $scope.startCube = function(cube) {
      AdminStreamingService.assignCube({cubeName:cube.name}, {}, function(data){
        SweetAlert.swal({
          title: '成功!',
          text: 'Cube 开启成功',
          type: 'success',
          confirmButtonText: '确定',
          confirmButtonClass: 'btn-primary',
          closeOnConfirm: true
        }, function () {
          location.reload();
        });
      }, function(e){
        if(e.data&& e.data.exception){
          var message =e.data.exception;
          var msg = !!(message) ? message : '开启cube成功';
          SweetAlert.swal('提示...', msg, 'error');
        } else{
          SweetAlert.swal('提示...', '开启cube失败', 'error');
        }
      });
    };

    $scope.pauseCube = function(cube) {
      AdminStreamingService.suspendCubeConsume({cubeName:cube.name}, {}, function(data){
        SweetAlert.swal({
          title: '成功!',
          text: 'Cube 暂停成功',
          type: 'success',
          confirmButtonText: 'OK',
          confirmButtonClass: 'btn-primary',
          closeOnConfirm: true
        }, function () {
          location.reload();
        });
      }, function(e){
        if(e.data&& e.data.exception){
          var message =e.data.exception;
          var msg = !!(message) ? message : '暂停 cube 成功';
          SweetAlert.swal('提示...', msg, 'error');
        } else{
          SweetAlert.swal('提示...', '暂停 cube 失败', 'error');
        }
      });
    };

    $scope.resumeCube = function(cube) {
      AdminStreamingService.resumeCubeConsume({cubeName:cube.name}, {}, function(data){
        SweetAlert.swal({
          title: '成功!',
          text: 'Cube 重启成功',
          type: 'success',
          confirmButtonText: 'OK',
          confirmButtonClass: 'btn-primary',
          closeOnConfirm: true
        }, function () {
          location.reload();
        });
      }, function(e){
        if(e.data&& e.data.exception){
          var message =e.data.exception;
          var msg = !!(message) ? message : 'cube重启成功';
          SweetAlert.swal('提示...', msg, 'error');
        } else{
          SweetAlert.swal('提示...', 'cube重启失败', 'error');
        }
      });
    };

    $scope.viewAssignment = function(cube) {
      AdminStreamingService.getCubeAssignment({cube: cube.name}, function(data) {
        $modal.open({
          templateUrl: 'cubeAssignment.html',
          controller: function($scope, assignment, cube, $modalInstance) {
            $scope.status = 'view';

            $scope.assignmentGridOptions = {
              paginationPageSize: 20,
              columnDefs: [
                { name: '副本集ID', field: 'rs_id', width:'20%'},
                { name: '分区', field: 'partitions', width:'*', cellTemplate: '<div class="ui-grid-cell-contents"><span class="label label-primary" style="margin-right:5px;" ng-repeat="partition in row.entity.partitions">{{partition.partition_id}}</span></div>' }
              ]
            };
            $scope.assignmentGridOptions.data = assignment[cube];

            $scope.cancel = function() {
              $modalInstance.dismiss('cancel');
            };

            $scope.editAssignment = function() {
              $scope.cubePartitions = [];
              angular.forEach(assignment[cube], function(replicaSet, index) {
                $scope.cubePartitions = $scope.cubePartitions.concat(replicaSet.partitions);
              });
              $scope.replicaSetIds = [];

              AdminStreamingService.getReplicaSets({}, function (data) {
                angular.forEach(data, function(rs, index) {
                  $scope.replicaSetIds.push(rs.rs_id);
                });
                $scope.status = 'edit';
                $scope.currentAssignment = assignment[cube];
              },function(e){
                if (e.data && e.data.exception) {
                  var message = e.data.exception;
                  var msg = !!(message) ? message : '无法获取副本集信息';
                  SweetAlert.swal('提示...', msg, 'error');
                } else {
                  SweetAlert.swal('提示...', '无法获取副本集信息', 'error');
                }
              });
            };

            $scope.removeReplicaSet = function(index) {
              console.log('remove element:', $scope.currentAssignment[index]);
              $scope.currentAssignment.splice(index, 1);
            };

            $scope.addReplicaSet = function() {
              $scope.currentAssignment.push({rs_id:"", partitions:[]});
            };

            $scope.reAssignCube = function() {
              AdminStreamingService.reAssignCube({cubeName: cube}, {cube_name: cube, assignments: $scope.currentAssignment}, function(data){
                $scope.cancel();
              }, function(e){
                if(e.data&& e.data.exception){
                  var message =e.data.exception;
                  var msg = !!(message) ? message : '重新分配cube失败';
                  SweetAlert.swal('提示...', msg, 'error');
                } else{
                  SweetAlert.swal('提示...', '重新分配cube失败', 'error');
                }
              });
            };
          },
          resolve: {
            assignment: function() {
              return data;
            },
            cube: function() {
              return cube.name;
            }
          }
        });
      }, function(e) {
        if(e.data&& e.data.exception){
          var message =e.data.exception;
          var msg = !!(message) ? message : '获取cube分配信息失败';
          SweetAlert.swal('提示...', msg, 'error');
        } else{
          SweetAlert.swal('提示...', '获取cube分配信息失败', 'error');
        }
      });
    };

  });


var cubeCloneCtrl = function ($scope, $modalInstance, CubeService, MessageService, $location, cube, MetaModel, SweetAlert,ProjectModel, loadingRequest, MessageBox) {
  $scope.projectModel = ProjectModel;

  $scope.targetObj={
    cubeName:cube.descriptor+"_clone",
    targetProject:$scope.projectModel.selectedProject
  }

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.cloneCube = function(){

    if(!$scope.targetObj.targetProject){
      SweetAlert.swal('提示...', "请选择目标项目.", 'info');
      return;
    }

    $scope.cubeRequest = {
      cubeName:$scope.targetObj.cubeName,
      project:$scope.targetObj.targetProject
    }

    SweetAlert.swal({
      title: '',
      text: '确定克隆该cube? ',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "确定",
      cancelButtonText: "取消",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {

        loadingRequest.show();
        CubeService.clone({cubeId: cube.name}, $scope.cubeRequest, function (result) {
          loadingRequest.hide();
          MessageBox.successNotify('克隆cube成功');
          location.reload();
        }, function (e) {
          loadingRequest.hide();
          if (e.data && e.data.exception) {
            var message = e.data.exception;
            var msg = !!(message) ? message : '操作失败.';
            SweetAlert.swal('提示...', msg, 'error');
          } else {
            SweetAlert.swal('提示...', "操作失败.", 'error');
          }
        });
      }
    });
  }

}


var jobSubmitCtrl = function ($scope, $modalInstance, CubeService, MessageService, $location, cube, metaModel, buildType, SweetAlert, loadingRequest, scope, CubeList,$filter, MessageBox) {
  $scope.cubeList = CubeList;
  $scope.cube = cube;
  $scope.metaModel = metaModel;
  $scope.jobBuildRequest = {
    buildType: buildType,
    startTime: 0,
    endTime: 0
  };
  $scope.message = "";
  $scope.refreshType = 'normal';
  var startTime;
  if(cube.segments.length == 0){
    startTime = (!!cube.detail.partition_date_start)?cube.detail.partition_date_start:0;
  }else{
    startTime = cube.segments[cube.segments.length-1].date_range_end;
  }
  $scope.jobBuildRequest.startTime=startTime;
  $scope.rebuild = function (isForce) {
    $scope.message = null;
    if ($scope.jobBuildRequest.startTime >= $scope.jobBuildRequest.endTime) {
      $scope.message = "WARNING: End time should be later than the start time.";
      return;
    }
    $scope.jobBuildRequest.forceMergeEmptySegment = !!isForce;
    loadingRequest.show();
    CubeService.rebuildCube({cubeId: cube.name}, $scope.jobBuildRequest, function (job) {
      loadingRequest.hide();
      $modalInstance.dismiss('cancel');
      MessageBox.successNotify('重建任务已成功提交');
      scope.refreshCube(cube).then(function(_cube){
          $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
        });
    }, function (e) {
      loadingRequest.hide();
      if (e.data && e.data.exception) {
        var message = e.data.exception;

        if(message.indexOf("Empty cube segment found")!=-1){
          var _segment = message.substring(message.indexOf(":")+1,message.length-1);
          SweetAlert.swal({
            title:'',
            type:'info',
            text: '找到空的cube段'+':'+_segment+', 强制合并段吗 ?',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            closeOnConfirm: true
          }, function (isConfirm) {
            if (isConfirm) {
              $scope.rebuild(true);
            }
          });
          return;
        }
        if(message.indexOf("Merging segments must not have gaps between")!=-1){
          SweetAlert.swal({
            title:'',
            type:'info',
            text: '段之间有间隙，是否要强制合并段 ?',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            closeOnConfirm: true
          }, function (isConfirm) {
            if (isConfirm) {
              $scope.rebuild(true);
            }
          });
          return;
        }
        var msg = !!(message) ? message : '操作失败.';
        SweetAlert.swal('提示...', msg, 'error');
      } else {
        SweetAlert.swal('提示...', "操作失败.", 'error');
      }
    });
  };

  // used by cube segment refresh
  $scope.segmentSelected = function (selectedSegment) {
    $scope.jobBuildRequest.startTime = 0;
    $scope.jobBuildRequest.endTime = 0;

    if (selectedSegment.date_range_start) {
      $scope.jobBuildRequest.startTime = selectedSegment.date_range_start;
    }

    if (selectedSegment.date_range_end) {
      $scope.jobBuildRequest.endTime = selectedSegment.date_range_end;
    }
  };

  // used by cube segments merge
  $scope.mergeStartSelected = function (mergeStartSeg) {
    $scope.jobBuildRequest.startTime = 0;

    if (mergeStartSeg.date_range_start) {
      $scope.jobBuildRequest.startTime = mergeStartSeg.date_range_start;
    }
  };

  $scope.mergeEndSelected = function (mergeEndSeg) {
    $scope.jobBuildRequest.endTime = 0;

    if (mergeEndSeg.date_range_end) {
      $scope.jobBuildRequest.endTime = mergeEndSeg.date_range_end;
    }
  };

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.getAdvRefreshTimeOptions = function(status) {
    if ('start' === status) {
      var startTimeOptions = [];
      var lastInd = $scope.cube.segments.length - 1;
      angular.forEach($scope.cube.segments, function(segment, ind) {
        startTimeOptions.push(segment.date_range_start);
        if (lastInd == ind) {
          startTimeOptions.push(segment.date_range_end);
        }
      });
      return startTimeOptions;
    } else if ('end' === status) {
      var endTimeOptions = [];
      angular.forEach($scope.cube.segments, function(segment, ind) {
        endTimeOptions.push(segment.date_range_end);
      });
      return endTimeOptions;
    }
  };
  $scope.advRefreshStartTimeOptions = $scope.getAdvRefreshTimeOptions('start');
  $scope.advRefreshEndTimeOptions = $scope.getAdvRefreshTimeOptions('end');
  $scope.endTimeTypeCustomize = false;

  $scope.changeEndTimeDisplay = function() {
    $scope.endTimeTypeCustomize = !$scope.endTimeTypeCustomize;
  };

  $scope.setDateRange = function($view, $dates) {
    var minDate = $scope.cube.segments[$scope.cube.segments.length-1].date_range_end;
    // var maxDate = moment().startOf($view).valueOf(); // Now
    angular.forEach($dates, function(date) {
      var utcDateValue = date.utcDateValue;
      date.selectable = utcDateValue >= minDate; // && utcDateValue <= maxDate;
    });
  };

  $scope.changeRefreshType = function (type) {
    $scope.refreshType = type;
    if (type ==='normal') {
      $scope.jobBuildRequest.buildType = 'REFRESH';
    } else if (type === 'advance'){
      $scope.jobBuildRequest.buildType = 'BUILD';
    }
  }
};


var streamingBuildCtrl = function ($scope, $modalInstance,kylinConfig) {
  $scope.kylinConfig = kylinConfig;
  var streamingGuildeUrl = kylinConfig.getProperty("kylin.web.link-streaming-guide");
  $scope.streamingBuildUrl = streamingGuildeUrl?streamingGuildeUrl:"http://kylin.apache.org/";

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };
};

var deleteSegmentCtrl = function($scope, $modalInstance, CubeService, SweetAlert, loadingRequest, cube, scope, MessageBox) {
  $scope.cube = cube;
  $scope.deleteSegments = [];
  $scope.segment = {};

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.deleteSegment = function() {
    SweetAlert.swal({
      title: '',
      text: '确定要删除段 ['+$scope.segment.selected.name+']? ',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "确定",
      cancelButtonText: "取消",
      closeOnConfirm: true
    }, function(isConfirm) {
      if(isConfirm){
        loadingRequest.show();
        CubeService.deleteSegment({cubeId: cube.name, propValue: $scope.segment.selected.name}, {}, function (result) {
          loadingRequest.hide();
          $modalInstance.dismiss('cancel');
          scope.refreshCube(cube).then(function(_cube){
           if(_cube && _cube.name){
              scope.cubeList.cubes[scope.cubeList.cubes.indexOf(cube)] = _cube;
           }
          });
          MessageBox.successNotify('删除段成功');
        },function(e){
          loadingRequest.hide();
          if(e.data&& e.data.exception){
            var message =e.data.exception;
            var msg = !!(message) ? message : '删除段失败.';
            SweetAlert.swal('提示...', msg, 'error');
          }else{
            SweetAlert.swal('提示...', '删除段失败.', 'error');
          }
        });
      }
    });
  };
};

var lookupRefreshCtrl = function($scope, scope, CubeList, $modalInstance, CubeService, cube, SweetAlert, loadingRequest, MessageBox) {
  $scope.cubeList = CubeList;
  $scope.cube = cube;
  $scope.dispalySegment = false;

  $scope.getLookups = function() {
    var modelLookups = cube.model ? cube.model.lookups : [];
    var cubeLookups = [];
    angular.forEach(modelLookups, function(modelLookup, index) {
      var dimensionTables = _.find(cube.detail.dimensions, function(dimension){ return dimension.table === modelLookup.alias;});
      if (!!dimensionTables) {
        if (cubeLookups.indexOf(modelLookup.table) === -1) {
          cubeLookups.push(modelLookup.table);
        }
      }
    });
    return cubeLookups;
  };

  $scope.cubeLookups = $scope.getLookups();

  $scope.lookup = {
    select: {}
  };

  $scope.getReadySegment = function(segment) {
    return segment.status === 'READY';
  };

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.updateLookupTable = function(tableName) {
    var lookupTable = _.find(cube.detail.snapshot_table_desc_list, function(table){ return table.table_name == tableName});
    if (!!lookupTable && lookupTable.global) {
      $scope.dispalySegment = false;
      $scope.lookup.select.segments = [];
    } else {
      $scope.dispalySegment = true;
    }
  };

  $scope.selectAllSegments = function(allSegments) {
    if (allSegments) {
      $scope.lookup.select.segments = $scope.cube.segments;
    } else {
      $scope.lookup.select.segments = [];
    }
  };

  $scope.refresh = function() {
    if (!$scope.lookup.select.table_name) {
      SweetAlert.swal('Warning', 'Lookup table 不能为空', 'warning');
      return;
    }

    // cube advance lookup table
    var lookupTable = _.find(cube.detail.snapshot_table_desc_list, function(table){ return table.table_name == $scope.lookup.select.table_name});
    if (!!lookupTable) {
      if (!lookupTable.global && $scope.lookup.select.segments.length == 0) {
        SweetAlert.swal('Warning', 'Segment 不能为空', 'warning');
        return;
      }
    } else {
      // cube lookup table
      lookupTable = _.find($scope.cubeLookups, function(table){ return table == $scope.lookup.select.table_name});
      if (!lookupTable) {
        SweetAlert.swal('Warning', 'Lookup table 不存在于 cube', 'warning');
        return;
      } else {
        if (!$scope.lookup.select.segments || $scope.lookup.select.segments.length == 0) {
          SweetAlert.swal('Warning', 'Segment 不能为空', 'warning');
          return;
        }
      }
    }

    var lookupSnapshotBuildRequest = {
      lookupTableName: $scope.lookup.select.table_name,
      segmentIDs: _.map($scope.lookup.select.segments, function(segment){ return segment.uuid})
    };

    loadingRequest.show();
    CubeService.lookupRefresh({cubeId: cube.name}, lookupSnapshotBuildRequest, function (job) {
      loadingRequest.hide();
      $modalInstance.dismiss('cancel');
      MessageBox.successNotify('查找刷新任务已成功提交');
      scope.refreshCube(cube).then(function(_cube){
          $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
        });
    }, function (e) {
       loadingRequest.hide();
      if (e.data && e.data.exception) {
        var message = e.data.exception;

        var msg = !!(message) ? message : '操作失败.';
        SweetAlert.swal('提示...', msg, 'error');
      } else {
        SweetAlert.swal('提示...', "操作失败.", 'error');
      }
    });
  };

};
