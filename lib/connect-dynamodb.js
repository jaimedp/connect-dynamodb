/*!
* Connect - DynamoDB
* Copyright(c) 2015 Mike Carson <ca98am79@gmail.com>
* MIT Licensed
*/

/**
* Module dependencies.
*/
var AWS = require('aws-sdk');

/**
* One day in milliseconds.
*/

var oneDayInMilliseconds = 86400000;

/**
* Return the `DynamoDBStore` extending `connect`'s session Store.
*
* @param {object} connect
* @return {Function}
* @api public
*/

module.exports = function (connect) {
  /**
  * Connect's Store.
  */

  var Store = connect.session.Store;

  /**
  * Initialize DynamoDBStore with the given `options`.
  *
  * @param {Object} options
  * @api public
  */

  function DynamoDBStore(options) {
    options = options || {};
    Store.call(this, options);
    this.prefix = options.prefix == null ? 'sess:' : options.prefix;

    if (options.client) {
      this.client = options.client;
    } else {
      if (options.AWSConfigPath) {
        AWS.config.loadFromPath(options.AWSConfigPath);
      } else {
        this.AWSRegion = options.AWSRegion || 'us-east-1';
        AWS.config.update({region: this.AWSRegion});
      }

      this.client = new AWS.DynamoDB();
    }

    this.docClient = new AWS.DynamoDB.DocumentClient({service: this.client});
    this.table = options.table || 'sessions';
    this.reapInterval = options.reapInterval || (10 * 60 * 1000);

    if (this.reapInterval > 0) {
      this._reap = setInterval(this.reap.bind(this), this.reapInterval);
    }

    // check if sessions table exists, otherwise create it
    this.client.describeTable({
      TableName: this.table
    }, function (error, info) {
      if (error) {
        this.client.createTable({
          TableName: this.table,
          AttributeDefinitions: [{
            AttributeName: 'id',
            AttributeType: 'S'
          }],
          KeySchema: [{
            AttributeName: 'id',
            KeyType: 'HASH'
          }],
          ProvisionedThroughput: {
            ReadCapacityUnits: options.readCapacityUnits || 5,
            WriteCapacityUnits: options.writeCapacityUnits || 5
          }
        }, console.log);
      }
    }.bind(this));
  };

  /*
  *  Inherit from `Store`.
  */
  DynamoDBStore.prototype.__proto__ = Store.prototype; // eslint-disable-line

  /**
  * Attempt to fetch session by the given `sid`.
  *
  * @param {String} sid
  * @param {Function} fn
  * @api public
  */
  DynamoDBStore.prototype.get = function (sid, fn) {

    sid = this.prefix + sid;
    var now = +new Date();

    this.docClient.get({
      TableName: this.table,
      Key: {
        id: sid
      }
    }, function (err, result) {

      if (err) {
        fn(err);
      } else {
        try {
          if (!result.Item) return fn(null, null);
          else if (result.Item.expires && now >= result.Item.expires) {
            fn(null, null);
          } else {
            var sess = result.Item.session;
            fn(null, sess);
          }
        } catch (err) {
          fn(err);
        }
      }
    });
  };

  /**
  * Commit the given `sess` object associated with the given `sid`.
  *
  * @param {String} sid
  * @param {Session} sess
  * @param {Function} fn
  * @api public
  */
  DynamoDBStore.prototype.set = function (sid, sess, fn) {
    sid = this.prefix + sid;
    var expires = typeof sess.cookie.maxAge === 'number' ?
      (+new Date()) + sess.cookie.maxAge : (+new Date()) + oneDayInMilliseconds;
    sess = JSON.parse(JSON.stringify(sess));

    var params = {
      TableName: this.table,
      Item: {
        id: sid,
        expires: expires,
        type: 'connect-session',
        session: sess || {}
      }
    };
    this.docClient.put(params, fn);
  };

  /**
  * Cleans up expired sessions
  *
  * @param {Function} fn
  * @api public
  */
  DynamoDBStore.prototype.reap = function (fn) {
    var now = +new Date();
    var params = {
      TableName: this.table,
      FilterExpression: 'expires < :value',
      ExpressionAttributeValues: {
        ':value': now
      },
      AttributesToGet: ['id']
    };
    this.docClient.scan(params, function (err, data) {
      if (err) return fn && fn(err);
      destroy.call(this, data, fn);
    }.bind(this));
  };

  function destroy(data, fn) {
    var self = this;

    function destroyDataAt(index) {
      if (data.Count > 0 && index < data.Count) {
        var sid = data.Items[index].id;
        sid = sid.substring(self.prefix.length, sid.length);
        self.destroy(sid, function () {
          destroyDataAt(index + 1);
        });
      } else {
        return fn && fn();
      }
    }
    destroyDataAt(0);
  }

  /**
  * Destroy the session associated with the given `sid`.
  *
  * @param {String} sid
  * @param {Function} fn
  * @api public
  */

  DynamoDBStore.prototype.destroy = function (sid, fn) {
    sid = this.prefix + sid;
    this.docClient.delete({
      TableName: this.table,
      Key: {
        id: sid
      }
    }, fn || function () {});
  };

  /**
  * Clear intervals
  *
  * @api public
  */

  DynamoDBStore.prototype.clearInterval = function () {
    if (this._reap) clearInterval(this._reap);
  };

  return DynamoDBStore;
};
