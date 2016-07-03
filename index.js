var EventEmitter = require('events').EventEmitter
var uuid = require('uuid').v4
var duplexJSONStream = require('duplex-json-stream')
var inherits = require('util').inherits
var reconnect = require('reconnect-net')

module.exports = TCPLogClient

function TCPLogClient (options) {
  if (!(this instanceof TCPLogClient)) return new TCPLogClient(options)
  validateOptions(options)
  var tcpOptions = {
    port: options.port,
    host: options.host || 'localhost',
    family: options.family || 4
  }

  var self = this
  var emit = this.emit.bind(this)
  this._timeout = options.timeout || 1000
  var head = 0
  var writes = this._writes = {}

  this._reconnect = reconnect(function (stream) {
    var json = self._json = duplexJSONStream(stream)
      .on('data', function (message) {
        if ('current' in message) emit('current')
        else if (message.event === 'wrote') {
          onWrote(message.index, message.id)
        } else if ('entry' in message) {
          onEntry(message.entry, message.index)
        }
      })
    Object.keys(writes).forEach(function (id) {
      json.write(writeMessage(writes[id].entry, id))
    })
    json.write({type: 'read', from: head})
  })
    .on('connect', function () { emit('connect') })
    .on('reconnect', function () { emit('reconnect') })
    .on('error', function (error) { emit('error', error) })
    .connect(tcpOptions)

  function onWrote (index, id) {
    var write = writes[id]
    clearTimeout(write.timeout)
    var callback = write.callback
    onEntry(write.entry, index)
    if (callback) callback(null, index)
    delete writes[id]
  }

  function onEntry (entry, index) {
    if (index > head) head = index
    emit('entry', entry, index)
  }
}

inherits(TCPLogClient, EventEmitter)

TCPLogClient.prototype.write = function (entry, callback) {
  var writes = this._writes
  var id = uuid()
  writes[id] = {
    entry: entry,
    callback: callback || false,
    timeout: setTimeout(function () {
      writes[id].callback(new Error('timeout'))
      delete writes[id]
    }, this._timeout)
  }
  if (this._json) this._json.write(writeMessage(entry, id))
}

function writeMessage (entry, id) {
  return {type: 'write', id: id, entry: entry}
}

TCPLogClient.prototype.reconnect = function () {
  this._reconnect.disconnect()
}

TCPLogClient.prototype.disconnect = function () {
  this._reconnect.reconnect = false
  this._reconnect.disconnect()
}

var optionValidations = {
  port: isPositiveInteger,
  host: optional(isString),
  family: optional(ipVersion),
  reconnect: optional(reconnectOptions),
  timeout: optional(isPositiveInteger)
}

function validateOptions (options) {
  Object.keys(optionValidations).forEach(function (option) {
    if (!optionValidations[option](options[option])) {
      throw new Error('Invalid ' + option)
    }
  })
}

function optional (predicate) {
  return function (option) {
    return option === undefined || predicate(option)
  }
}

function isPositiveInteger (integer) {
  return Number.isInteger(integer) && integer > 0
}

function isString (string) {
  return typeof string === 'string' && string.length !== 0
}

function ipVersion (version) {
  return version === 4 || version === 6
}

function reconnectOptions (options) {
  return typeof options === 'object'
}
