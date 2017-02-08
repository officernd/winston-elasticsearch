'use strict';

var Promise = require('promise');
var debug = require('debug')('bulk writer');

var BulkWriter = function BulkWriter(client, interval, waitForActiveShards) {
  this.client = client;
  this.interval = interval || 5000;
  this.waitForActiveShards = waitForActiveShards;

  this.bulk = []; // bulk to be flushed
  this.running = false;
  this.timer = false;
  debug('created', this);
};

BulkWriter.prototype.start = function start() {
  this.stop();
  this.running = true;
  this.tick();
  debug('started');
};

BulkWriter.prototype.stop = function stop() {
  this.running = false;
  if (!this.timer) {
    return;
  }
  clearTimeout(this.timer);
  this.timer = null;
  debug('stopped');
};

BulkWriter.prototype.schedule = function schedule() {
  var thiz = this;
  this.timer = setTimeout(function () {
    thiz.tick();
  }, this.interval);
};

BulkWriter.prototype.tick = function tick() {
  debug('tick');
  var thiz = this;
  if (!this.running) {
    return;
  }
  this.flush().catch(function (e) {
    throw e;
  }).then(function () {
    thiz.schedule();
  });
};

BulkWriter.prototype.flush = function flush() {
  // write bulk to elasticsearch
  var thiz = this;
  if (this.bulk.length === 0) {
    debug('nothing to flush');

    return new Promise(function (resolve) {
      return resolve();
    });
  }

  var bulk = this.bulk.concat();
  this.bulk = [];
  debug('going to write', bulk);
  return this.client.bulk({
    body: bulk,
    waitForActiveShards: this.waitForActiveShards,
    timeout: this.interval + 'ms',
    type: this.type
  }).catch(function (e) {
    // rollback this.bulk array
    thiz.bulk = bulk.concat(thiz.bulk);
    throw e;
  });
};

BulkWriter.prototype.append = function append(index, type, doc) {
  this.bulk.push({
    index: {
      _index: index, _type: type
    }
  });
  this.bulk.push(doc);
};

module.exports = BulkWriter;