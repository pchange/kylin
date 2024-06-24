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

KylinApp.controller('AccessCtrl', function ($scope, AccessService, MessageService, AuthenticationService, SweetAlert) {

  $scope.accessTooltip = "<div style='text-align: left'>" +
  "<label>权限对项目意味着什么?</label>" +
  "<ul><li>QUERY: 有权限查询cube</li>" +
  "<li>OPERATION: 重建、恢复和取消作业的权限,还包括访问cube查询.</li>" +
  "<li>MANAGEMENT: 编辑/删除cube的访问权限,还包括访问cube的操作.</li>" +
  "<li>ADMIN: 对cube和任务的完全访问权限，包括访问管理.</li></ul></div>";

  $scope.authorities = null;
  AuthenticationService.authorities({}, function (authorities) {
    $scope.authorities = authorities.stringList;
  });

  $scope.resetNewAcess = function () {
    $scope.newAccess = null;
  }

  $scope.renewAccess = function (entity) {
    $scope.newAccess = {
      uuid: entity.uuid,
      sid: null,
      principal: true,
      permission: 'READ'
    };
  }

  $scope.grant = function (type, entity, grantRequst) {
    var uuid = grantRequst.uuid;
    delete grantRequst.uuid;
    AccessService.grant({type: type, uuid: uuid}, grantRequst, function (accessEntities) {
      entity.accessEntities = accessEntities;
      $scope.resetNewAcess();
//            MessageService.sendMsg('Access granted!', 'success', {});
      SweetAlert.swal('成功!', '授予访问权限!', 'success');
    }, function (e) {
      grantRequst.uuid = uuid;
      if (e.status == 404) {
//                MessageService.sendMsg('User not found!', 'error', {});
        SweetAlert.swal('提示...', '找不到用户!!', 'error');
      }
      else {
        if (e.data && e.data.exception) {
          var message = e.data.exception;
          var msg = !!(message) ? message : '操作失败.';
          SweetAlert.swal('提示...', msg, 'error');
        } else {
          SweetAlert.swal('提示...', "操作失败.", 'error');
        }

      }
    });
  }

  $scope.update = function (type, entity, access, permission) {
    var updateRequst = {
      accessEntryId: access.id,
      permission: permission
    };
    AccessService.update({type: type, uuid: entity.uuid}, updateRequst, function (accessEntities) {
      entity.accessEntities = accessEntities;
//            MessageService.sendMsg('Access granted!', 'success', {});
      SweetAlert.swal('', '授予访问权限!', 'success');
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

  $scope.revoke = function (type, access, entity) {
    SweetAlert.swal({
      title: '',
      text: '确定要撤销该权限吗?',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "确定",
      cancelButtonText: "取消",
      closeOnConfirm: true
    }, function (isConfirm) {
      if(isConfirm){
      var revokeRequst = {
        type: type,
        uuid: entity.uuid,
        accessEntryId: access.id,
        sid: access.sid.principal
      };
      AccessService.revoke(revokeRequst, function (accessEntities) {
        entity.accessEntities = accessEntities.accessEntryResponseList;
        SweetAlert.swal('成功!', '访问权限已被撤销.', 'success');
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
    });

  }
});

