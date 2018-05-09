/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
angular.module('serviceCenter.sc', [])
	.controller('servicesListController', ['$scope', 'httpService', 'apiConstant', 'commonService', '$stateParams', '$mdDialog',
		function($scope, httpService, apiConstant, commonService, $stateParams, $mdDialog){

			$scope.appList = 'fetching';
			$scope.serviceList = 'serviceList';
			$scope.rowsPerPage = [5, 10];
			
			$scope.tableHeaders = [
				{
					'key': 'name'
				},
				{
					'key': 'status'
				},
				{
					'key': 'appId'
				},
				{
					'key': 'version'
				},
				{
					'key': 'createdAt'
				},
				{
					'key': 'instances'
				},
				{
					'key': 'operation'
				}
			];

			$scope.refreshAppList = function() {
				angular.element(document.querySelector('.fa-refresh')).addClass('fa-spin');
				$scope.getAllServices();
			};

            var deleteService = function(response){
            	if(response == "yes"){
            		$(".loader").show();
        			var url = apiConstant.api.deleteService.url;
        			var api =  url.replace("{{serviceId}}", $scope.deleteServiceId);
					var method = apiConstant.api.deleteService.method;
					httpService.apiRequest(api, method, null, null, "nopopup").then(function(response){
						if(response && response.status == 200){
							$(".loader").hide();
							$scope.refreshAppList();
							commonService.oneBtnMsg("prompt", "serviceDeletedSuccessfully")
						}else{
							$(".loader").hide();
							commonService.oneBtnMsg("error","unableToDeleteService")
						}
					},function(error){
							$(".loader").hide();
							commonService.oneBtnMsg("error","unableToDeleteService")
					})
            	}
            };

		  	$scope.removeService = function(serviceId, instances) {
		  		$scope.deleteServiceId = serviceId;
		  		if(instances == 0){
		  			commonService.twoBtnMsg("warning", "areYouSureToDelete", deleteService);
		  		}else {
		  			commonService.oneBtnMsg("prompt", "cannotDeleteServiceWhenInstanceIsAvailable");
		  		}
            };

			$scope.getAllServices = function() {
				var filter = '';
				if($stateParams.status) {
					filter = $stateParams.status;
				}
				var url = apiConstant.api.microservice.url;
				var method = apiConstant.api.microservice.method;

				httpService.apiRequest(url, method).then(function(response){
					if(response && response.data && response.data.services){
						$scope.services = [];
						response.data.services.forEach(function(service){
							var instanceApi = apiConstant.api.instances.url;
							var instanceUrl = instanceApi.replace("{{serviceId}}", service.serviceId);
							var instanceMethod = apiConstant.api.instances.method;
							var headers = {"X-ConsumerId": service.serviceId};
							if(filter && service.status.toLowerCase() === filter) {
								var servicesList = {
									serviceName: service.serviceName,
									status: service.status.toLowerCase(),
									appId: service.appId.toLowerCase(),
									version: service.version,
									createdAt: commonService.timeFormat(service.timestamp),
									instances: 0,
									operation: '',
									serviceId: service.serviceId,
									disableBtn: false
								};
								httpService.apiRequest(instanceUrl, instanceMethod, null, headers, "nopopup").then(function(resp){
									if(resp && resp.data && resp.data.instances){
									   servicesList.instances = resp.data.instances.length;
									   if(servicesList.instances > 0){
									   		servicesList.disableBtn = true;
									   }
									}
								});
							
								$scope.services.push(servicesList);
							}
							if(!filter){
								var servicesList = {
									serviceName: service.serviceName,
									status: service.status.toLowerCase(),
									appId: service.appId.toLowerCase(),
									version: service.version,
									createdAt: commonService.timeFormat(service.timestamp),
									instances: 0,
									operation: '',
									serviceId: service.serviceId,
									disableBtn: false
								};
								httpService.apiRequest(instanceUrl, instanceMethod, null, headers, "nopopup").then(function(resp){
									if(resp && resp.data && resp.data.instances){
									   servicesList.instances = resp.data.instances.length;
									   if(servicesList.instances > 0){
									   		servicesList.disableBtn = true;
									   }
									}
								});
							
								$scope.services.push(servicesList);
							}
						});

						if($scope.services.length <= 0){
							$scope.appList = 'empty';
						}
						else {
							$scope.appList = '';
						}
						angular.element(document.querySelector('.fa-refresh')).removeClass('fa-spin');
					}
					else {
						$scope.appList = 'empty';
					}
				},function(error){
					angular.element(document.querySelector('.fa-refresh')).removeClass('fa-spin');
					$scope.appList = 'failed';
				})
			};
		
			$scope.getAllServices();
		
	}]);
