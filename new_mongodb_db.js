/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var MongoClient = require('mongodb').MongoClient;

/**
 * "settings" must be an object with the following properties:
 *   - host           (string): mandatory if no url is provided on settings
 *   - dbname         (string): mandatory if no url is provided on settings
 *   - port           (number): mandatory if no url is provided on settings
 *   - url            (string): full connection url, following the documentation on
 *                              https://docs.mongodb.com/manual/reference/connection-string/
 *   - user           (string)
 *   - password       (string)
 *   - extra          (object): optional connection settings, as described on
 *                              http://mongodb.github.io/node-mongodb-native/2.2/api/MongoClient.html#.connect
 *   - collectionName (string): defaults to "store"
 */
exports.database = function(settings) {
  var assertions = {
    exist: function(v) {return v !== undefined && v !== null},
    isString: function(v) {return typeof v === 'string'},
    isNumber: function(v) {return typeof v === 'number'},
  }
  var assert = function(value, assertion, message) { if (!assertion(value)) throw message }

  assert(settings, assertions.exist, 'you need to inform the settings');

  // some settings are only necessary when the full url is not provided
  if (!settings.url) {
    assert(settings.host, assertions.isString, 'you need to inform a valid host (string)');
    assert(settings.dbname, assertions.isString, 'you need to inform a valid dbname (string)');
    assert(settings.port, assertions.isNumber, 'you need to inform a valid port (number)');
  }

  this.settings = settings;
  this.settings.collectionName = assertions.isString(this.settings.collectionName) ? this.settings.collectionName : 'store';

  // these values are used by CacheAndBufferLayer
  this.settings.cache = 1000;
  this.settings.writeInterval = 100;
  this.settings.json = true;
}

// Samples: normal url, authentitcation url, replicatset url, ssl url
// var url = 'mongodb://<HOST>:<PORT>/<DB>';
// var url = 'mongodb://<USER>:<PASSWORD>@<HOST>:<PORT>?authSource=<AUTHENTICATION_DB>';
// var url = 'mongodb://<HOST1>:<PORT1>,<HOST2>:<PORT2>/<DB>?replicaSet=<REPLICA_SET>';
// var url = 'mongodb://<HOST>:<PORT>/<DB>?ssl=true';
//
// settings.extra - http://mongodb.github.io/node-mongodb-native/2.2/reference/connecting/connection-settings/
// could be ssl validation ex: server: {sslValidate: true, sslCA: ca}
exports.database.prototype._buildUrl = function(settings) {
  var protocol = "mongodb://";
  var authentication = settings.user && settings.password ? settings.user + ":" + settings.password + "@" : "";
  return protocol + authentication + settings.host + ":" + settings.port + "/" + settings.dbname
}

exports.database.prototype.init = function(callback) {
  var url = this.settings.url || this._buildUrl(this.settings);
  this.onMongoReady = callback

  var options = this.settings.extra || {};
  MongoClient.connect(url, options, this._onMongoConnect.bind(this));
}

exports.database.prototype._onMongoConnect = function(error, db) {
  if (error) {throw "an error occored [" + error + "] on mongo connect"}

  this.db = db;
  this.collection = this.db.collection(this.settings.collectionName);
  this.db.ensureIndex(this.settings.collectionName, {key: 1}, {unique:true, background:true},
    function(err, indexName) {console.log("index created [" + indexName + "]")});

  exports.database.prototype.set = function (key, value, callback) {
    this.collection.update({key: key}, {key: key, val: value}, {safe: true, upsert: true}, callback);
  }

  exports.database.prototype.get = function (key, callback) {
    this.collection.findOne({key: key}, callback);
  }

  exports.database.prototype.remove = function (key, callback) {
    this.collection.remove({key: key}, {safe: true}, callback);
  }

  exports.database.prototype.findKeys = function (key, notKey, callback) {
    var findRegex = this.createFindRegex(key, notKey);
    this.collection.find({key: findRegex}).toArray(function(err, docs) {
      var keys = docs.map(function(doc) { return doc.key });
      callback(err, keys);
    });
  }
  exports.database.prototype.doBulk = function (bulk, callback) {
    var operations = {
      "set": "insertOne", "remove": "deleteOne"
    }
    var mongoBulk = [];
    for (var i in bulk) {
      var eachOperation = bulk[i];
      var keyOperation = operations[eachOperation.type]
      var eachBulk = {}
      eachBulk[keyOperation] = {document: {key: eachOperation.key, value: eachOperation.value}, upsert:true}
      mongoBulk.push(eachBulk);
    }

    this.collection.bulkWrite(mongoBulk, callback);
  }
  exports.database.prototype.close = function (callback) {this.db.close(callback)}

  this.onMongoReady(error, this);
}
