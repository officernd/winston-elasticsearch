'use strict';

var util = require('util');
var Promise = require('promise');
var stream = require('stream');
var winston = require('winston');
var moment = require('moment');
var _ = require('lodash');
var retry = require('retry');
var elasticsearch = require('elasticsearch');

var defaultTransformer = require('./transformer');
var BulkWriter = require('./bulk_writer');

/**
 * Constructor
 */
var Elasticsearch = function Elasticsearch(options) {
  this.options = options || {};
  if (!options.timestamp) {
    this.options.timestamp = function timestamp() {
      return new Date().toISOString();
    };
  }
  // Enforce context
  if (!(this instanceof Elasticsearch)) {
    return new Elasticsearch(options);
  }

  // Set defaults
  var defaults = {
    level: 'info',
    index: null,
    indexPrefix: 'logs',
    indexSuffixPattern: 'YYYY.MM.DD',
    messageType: 'log',
    transformer: defaultTransformer,
    ensureMappingTemplate: true,
    flushInterval: 2000,
    waitForActiveShards: 1,
    handleExceptions: false
  };
  _.defaults(options, defaults);
  winston.Transport.call(this, options);

  // Use given client or create one
  if (options.client) {
    this.client = options.client;
  } else {
    // As we don't want to spam stdout, create a null stream
    // to eat any log output of the ES client
    var NullStream = function NullStream() {
      stream.Writable.call(this);
    };
    util.inherits(NullStream, stream.Writable);
    // eslint-disable-next-line no-underscore-dangle
    NullStream.prototype._write = function _write(chunk, encoding, next) {
      next();
    };

    var defaultClientOpts = {
      clientOpts: {
        log: [{
          type: 'stream',
          level: 'error',
          stream: new NullStream()
        }]
      }
    };
    _.defaults(options, defaultClientOpts);

    // Create a new ES client
    // http://localhost:9200 is the default of the client already
    this.client = new elasticsearch.Client(this.options.clientOpts);
  }

  this.bulkWriter = new BulkWriter(this.client, options.flushInterval, options.waitForActiveShards);
  this.bulkWriter.start();

  // Conduct initial connection check (sets connection state for further use)
  this.checkEsConnection().then(function (connectionOk) {});

  return this;
};

util.inherits(Elasticsearch, winston.Transport);

Elasticsearch.prototype.name = 'elasticsearch';

/**
 * log() method
 */
Elasticsearch.prototype.log = function log(level, message, meta, callback) {
  var logData = {
    message: message,
    level: level,
    meta: meta,
    timestamp: this.options.timestamp()
  };
  var entry = this.options.transformer(logData);

  this.bulkWriter.append(this.getIndexName(this.options), this.options.messageType, entry);

  callback(); // write is deferred, so no room for errors here :)
};

Elasticsearch.prototype.getIndexName = function getIndexName(options) {
  var indexName = options.index;
  if (indexName === null) {
    var now = moment();
    var dateString = now.format(options.indexSuffixPattern);
    indexName = options.indexPrefix + '-' + dateString;
  }
  return indexName;
};

Elasticsearch.prototype.checkEsConnection = function checkEsConnection() {
  var thiz = this;
  thiz.esConnection = false;

  var operation = retry.operation({
    retries: 3,
    factor: 3,
    minTimeout: 1 * 1000,
    maxTimeout: 60 * 1000,
    randomize: false
  });

  return new Promise(function (fulfill, reject) {
    operation.attempt(function (currentAttempt) {
      thiz.client.ping().then(function (res) {
        thiz.esConnection = true;
        // Ensure mapping template is existing if desired
        if (thiz.options.ensureMappingTemplate) {
          thiz.ensureMappingTemplate(fulfill, reject);
        } else {
          fulfill(true);
        }
      }, function (err) {
        if (operation.retry(err)) {
          return;
        }
        thiz.esConnection = false;
        thiz.emit('error', err);
        reject(false);
      });
    });
  });
};

Elasticsearch.prototype.search = function search(q) {
  var index = this.getIndexName(this.options);
  var query = {
    index: index,
    q: q
  };
  return this.client.search(query);
};

Elasticsearch.prototype.ensureMappingTemplate = function ensureMappingTemplate(fulfill, reject) {
  var thiz = this;
  var mappingTemplate = thiz.options.mappingTemplate;
  if (mappingTemplate === null || typeof mappingTemplate === 'undefined') {
    // eslint-disable-next-line import/no-unresolved, import/no-extraneous-dependencies
    mappingTemplate = require('index-template-mapping.json');
  }
  var tmplCheckMessage = {
    name: 'template_' + thiz.options.indexPrefix
  };
  thiz.client.indices.getTemplate(tmplCheckMessage).then(function (res) {
    fulfill(res);
  }, function (res) {
    if (res.status && res.status === 404) {
      var tmplMessage = {
        name: 'template_' + thiz.options.indexPrefix,
        create: true,
        body: mappingTemplate
      };
      thiz.client.indices.putTemplate(tmplMessage).then(function (res1) {
        fulfill(res1);
      }, function (err1) {
        reject(err1);
      });
    }
  });
};

module.exports = winston.transports.Elasticsearch = Elasticsearch;