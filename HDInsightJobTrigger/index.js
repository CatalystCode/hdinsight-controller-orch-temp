var azure = require('azure-storage');
var async = require('async');
var request = require('request');

var config = require('./config');
var FunctionsManager = require('./manage-functions');
var HDInsightManager = require('./manage-hdinsight');

var lastInactiveCheck = null;
var MAX_INACTIVE_TIME = 15; // Minutes

module.exports = function (context, checkTimer) {

  if (checkTimer.isPastDue)
  {
    context.log('Check Timer is running late...');
  }

  // 1. Check statuses
  context.log('Initializing statuses');
  var hdinsightManager = new HDInsightManager();
  var functionsManager = new FunctionsManager();
  var appServiceClient = null;
  var status = {
    queueError: null,
    queueLength: 0,
    funcError: null,
    funcActive: false,
    hdinsightError: null,
    hdinsightActive: false,
    hdinsightStatus: null,
    livyError: null,
    livyJobs: 0
  };

  async.parallel([
    
    // 1.1. Get queue count from azure storage queue
    checkQueue,
    
    // 1.2. Get function state from ARM
    checkFunction,
    
    // 1.3. Get HDInsight state from ARM
    checkHDInsight,
    
    // 1.4. If alive ==> Get livy jobs
    checkLivy

  ], function (err, result) {

    if (err) { return sendAlert({ error: error }); }
    if (status.queueError) { return sendAlert({ error: status.queueError }); }
    if (status.funcError) { return sendAlert({ error: status.funcError }); }
    if (status.hdinsightError) { return sendAlert({ error: status.hdinsightError }); }
    if (status.livyError) { return sendAlert({ error: status.livyError }); }

    // Queue not empty
    // ================
    // 2. If queue is not empty && HDInsight is ResourceNotFound ==> create HDInsight
    context.log('If queue is not empty && HDInsight is ResourceNotFound ==> create HDInsight');
    if (status.queueLength > 0 && status.hdinsightStatus == 'ResourceNotFound') {
      context.log('Creating hdinsight');
      return hdinsightManager.createHDInsight(function (err) {
        if (err) { sendAlert({ error: err }); }
        context.log('Operation completed successfully');
        return context.done();
      })
    }

    // 3. If queue is not empty && HDInsight is Running && Livy is alive && function is down ==> wake up function
    context.log('If queue is not empty && HDInsight is Running && Livy is alive && function is down ==> wake up function');
    if (status.queueLength > 0 && status.hdinsightStatus == 'Running' && !status.funcActive) {
      context.log('Starting proxy app');
      return appServiceClient.start(function (err) {
        if (err) { sendAlert({ error: err }); }
        context.log('Operation completed successfully');
        return context.done();
      });
    }

    // Queue is empty
    // ================
    // 4. If queue is empty && Livy jobs == 0 && function is up | more than 15 minutes ==> shut down functions
    context.log('If queue is empty && Livy jobs == 0 && function is up | more than 15 minutes ==> shut down functions');
    if (status.queueLength === 0 && status.livyJobs === 0 && status.hdinsightStatus != 'ResourceNotFound' && status.funcActive) {
      var now = new Date();
      if (!lastInactiveCheck) {
        lastInactiveCheck = now;
        context.log('Operation completed successfully - initialized check time');
        return context.done();
      }

      if (getMinutes(now - lastInactiveCheck) >= MAX_INACTIVE_TIME) {
        context.log('Stopping proxy app');
        return appServiceClient.stop(function (err) {
          if (err) { sendAlert({ error: err }); }
          context.log('Operation completed successfully');
          return context.done();
        })
      }
    }
    
    // 5. If queue is empty && Livy jobs == 0 && function is down | more than 15 minutes ==> shut down HDInsight
    context.log('If queue is empty && Livy jobs == 0 && function is down | more than 15 minutes ==> shut down HDInsight');
    if (status.queueLength === 0 && status.livyJobs === 0 && status.hdinsightStatus != 'ResourceNotFound' && status.funcActive) {
      var now = new Date();
      if (!lastInactiveCheck) {
        lastInactiveCheck = now;
        context.log('Operation completed successfully - initialized check time');
        return context.done();;
      }

      if (getMinutes(now - lastInactiveCheck) >= MAX_INACTIVE_TIME) {
        return hdinsightManager.deleteHDInsight(function (err) {
          if (err) { 
            sendAlert({ error: err }); 
          }
          else {
            lastInactiveCheck = now; // If after 15 minutes hdinsight not down, try to delete again
          }
          context.log('Operation completed successfully');
          return context.done();
        })
      }
    }    
  });

  return context.done();
  
  // 1.1. Get queue count from azure storage queue
  function checkQueue(callback) {

    context.log('Checking queue size');
    var queueSvc = azure.createQueueService(config.clusterStorageAccountName, config.clusterStorageAccountKey);
    queueSvc.createQueueIfNotExists(config.inputQueueName, function(err, result, response){
      if (err) {
        status.queueError = err;
        return callback();
      }

      queueSvc.getQueueMetadata(config.inputQueueName, function(err, result, response){
        if (err) {
          status.queueError = err;
          return callback();
        }

        status.queueLength = result.approximateMessageCount;
        context.log('Queue size: ' + status.queueLength);
        return callback();
      });
    });
  }

  // 1.2. Get function state from ARM
  function checkFunction(callback) {
    context.log('Checking proxy app');
    functionsManager.init(function (err, _appServiceClient) {
      if (err) {
        status.funcError = err;
        return callback();
      }
      
      appServiceClient = _appServiceClient;
      appServiceClient.get(function (err, result) {
        if (err) {
          status.funcError = err;
          return callback();
        }

        status.funcActive = result && result.properties && result.properties.state == 'Running';
        context.log('proxy app active: ' + status.funcActive);
        return callback();
      })
    });
  }

  // 1.3. Get HDInsight state from ARM
  function checkHDInsight(callback) {

    context.log('Checking hdinsight');
    hdinsightManager.init(function (err) {

      if (err) {
        status.hdinsightError = err;
        return callback();
      }

      hdinsightManager.checkHDInsight(function (err, result) {

        if (err) {
          if (err.code != 'ResourceNotFound') {
            status.hdinsightError = err;
          }
          return callback();
        }

        if (result && result.cluster && result.cluster.properties && result.cluster.properties.provisioningState) {
          status.hdinsightActive = result.cluster.properties.provisioningState == 'Running';
          status.hdinsightStatus = result.cluster.properties.provisioningState;
        } else {
          status.hdinsightError = new Error('The resulting resource is not in an expected format: ' + result);
        }

        context.log('hdinsight state: ' + status.hdinsightStatus);
        return callback();
      });

    });
  }

  // 1.4. If alive ==> Get livy jobs
  function checkLivy(callback) {
    var authenticationHeader = 'Basic ' + new Buffer(config.clusterLoginUserName + ':' + config.clusterLoginPassword).toString('base64');
    var options = {
      uri: 'https://' + config.clusterName + '.azurehdinsight.net/livy/batches',
      method: 'GET',
      headers: { "Authorization": authenticationHeader },
      json: { }
    };

    context.log('Checking livy state');
    request(options, function (err, response, body) {

      if (err || !response || response.statusCode != 200) {
        status.livyError = err ? err : !response ? 
          new Error ('No response received') :
          new Error ('Status code is not 200');
        return callback();
      }

      // Need to check validity and probably filter only running jobs
      status.livyJobs = response.batches.length;
      context.log('livy jobs: ' + status.livyJobs);
      return callback();
    });
  }

  function sendAlert(alert) {

    context.log('ALERT: ' + alert);

    var options = {
      uri: sendAlertUrl,
      method: 'POST',
      json: { alert: alert }
    };

    // Currently, not handling problems with alerts
    request(options);
  }

  function getMinutes(diffMs) {
    return Math.round(((diffMs % 86400000) % 3600000) / 60000);
  }
};