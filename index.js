'use strict';

//definition library
const aws = require('aws-sdk');
const array = require('array-sort');
const co = require('co');
const moment = require('moment');
const tz = require('moment-timezone');

//difinition variables
const cloudwatchlogs = new aws.CloudWatchLogs();
const s3 = new aws.S3();

exports.handler = (event, context, callback) => {
  console.log('start function');

  // event params
  console.log(JSON.stringify(event));

  // s3 bucket name
  const bucket = event.Records[0].s3.bucket.name;

  // object key
  const key = event.Records[0].s3.object.key;

  // today
  const today = moment().tz('Asia/Tokyo').format('YYYY-MM-DD');

  co(function* () {
    // check loggroup exists
    console.log('check loggroup exists');
    const logGroup = yield describeLogGroups();
    if (logGroup.logGroups.length === 0) {
      // create loggroup
      console.log('create loggroup');
      yield createLogGroup();
    }

    // check logstream exists
    console.log('check logstream exists');
    const logStream = yield describeLogStreams();
    if (logStream.logStreams.length === 0) {
      // create logstream
      console.log('create logstream');
      yield createLogStream();
    }

    // s3 log
    const data = yield getObject();

    // encoding s3 log
    const contentEncoding = data.ContentEncoding;
    const messages = data.Body.toString(contentEncoding);

    // create logdata
    console.log('create logdata');
    const logEvents = [];
    messages.split('\n').forEach((value) => {
      if (value.length > 0) {
        const timestamp = moment(value.match(/\[.*\]/)[0].replace(/\[/, '').replace(/\]/, ''), 'DD/MMM/YYYY:HH:mm:ss Z').valueOf();
        const logEvent = {
          message: value,
          timestamp: timestamp
        };
        logEvents.push(logEvent);
      }
    });

    // sort timestamp
    console.log('sort timestamp');
    array(logEvents, 'timestamp');

    // perform put cloudwatchlogs
    console.log('put cloudwatchlogs');
    yield putLogEvents(logEvents, logStream);
    return null;
  }).then(onEnd).catch(onError);

  // check loggroup exists
  function describeLogGroups() {
    return new Promise((resolve,reject) => {
      const params = {
        logGroupNamePrefix: bucket
      };
      cloudwatchlogs.describeLogGroups(params, (error, data) => {
        if (!error) {
          console.log(JSON.stringify(data));
          resolve(data);
        } else {
          reject(error);
        }
      });
    });
  }

  // create loggroup
  function createLogGroup() {
    return new Promise((resolve, reject) => {
      const params = {
        logGroupName: bucket
      };
      cloudwatchlogs.createLogGroup(params, (error, data) => {
        if (!error) {
          resolve(data);
        } else {
          reject(error);
        }
      });
    });
  }

  // check logstream exists
  function describeLogStreams() {
    return new Promise((resolve, reject) => {
      const params = {
        logGroupName: bucket,
        logStreamNamePrefix: today
      };
      cloudwatchlogs.describeLogStreams(params, (error, data) => {
        if (!error) {
          console.log(JSON.stringify(data));
          resolve(data);
        } else {
          reject(error);
        }
      });
    });
  }

  // create logstream
  function createLogStream() {
    return new Promise((resolve, reject) => {
      const params = {
        logGroupName: bucket,
        logStreamName: today
      };
      cloudwatchlogs.createLogStream(params, (error, data) => {
        if (!error) {
          resolve(data);
        } else {
          reject(error);
        }
      });
    });
  }

  // s3 log
  function getObject() {
    return new Promise((resolve, reject) => {
      const params = {
        Bucket: bucket,
        Key: key
      };
      s3.getObject(params, (error, data) => {
        if (!error) {
           resolve(data);
        } else {
          reject(error);
        }
      });
    });
  }

  // perform put cloudwatchlogs
  function putLogEvents(logEvents, logStream) {
    return new Promise((resolve, reject) => {
      const params = {
        logEvents: logEvents,
        logGroupName: bucket,
        logStreamName: today
      };
      if (logStream.logStreams.length !== 0 && logStream.logStreams[0].uploadSequenceToken) {
        params.sequenceToken = logStream.logStreams[0].uploadSequenceToken;
      }
      cloudwatchlogs.putLogEvents(params, (error, data) => {
        if (!error) {
          console.log(JSON.stringify(data));
          resolve(data);
        } else {
          reject(error);
        }
      });
    });
  }

  // end
  function onEnd(result) {
    console.log('finish function');
    callback(null, 'succeed');
  }

  // error
  function onError(error) {
    console.log(error, error.stack);
    callback(error, error.stack);
  }
};