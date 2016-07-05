var EventEmitter = require('events').EventEmitter
var duplexJSONStream = require('duplex-json-stream')
var inherits = require('util').inherits
var net = require('net')
var reconnect = require('reconnect-core')
var uuid = require('uuid').v4

module.exports = TCPLogClient

function TCPLogClient (options) {
  if (!(this instanceof TCPLogClient)) return new TCPLogClient(options)
  validateOptions(options)
  var tcpOptions = {
    port: options.port,
    host: options.host || 'localhost',
    family: options.family || 4
  }
  var reconnectOptions = options.reconnect || {}

  var self = this
  var emit = this.emit.bind(this)
  this._timeout = options.timeout || 1000
  var head = 0
  self._json = false
  var writes = this._writes = {}

  this._reconnect = reconnect(function (options) {
    return net.connect(options).setKeepAlive(true)
  })(reconnectOptions, function (stream) {
    stream.on('close', function () { self._json = false })
    var json = self._json = duplexJSONStream(stream)
    .on('data', function (message) {
      if (isEntry(message)) {
        onEntry(message.entry, message.index)
      } else if (isConfirmation(message)) {
        onWrote(message.index, message.id)
      } else if (isCurrent(message)) {
        emit('current')
      } else if (isWriteError(message)) {
        onWriteError(message.index, message.error)
      } else if (isReadError(message)) {
        emit('error', message.index, message.error)
      }
    })
    Object.keys(writes).forEach(function (id) {
      if (writes[id].sent === false) {
        json.write(writeMessage(writes[id].entry, id))
        writes[id].sent = true
      }
    })
    json.write({type: 'read', from: head + 1})
  })
  .on('connect', function () { emit('connect') })
  .on('reconnect', function () { emit('reconnect') })
  .on('disconnect', function (error) {
    self._json = false
    emit('disconnect', error)
  })
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

  function onWriteError (id, error) {
    var write = writes[id]
    clearTimeout(write.timeout)
    var callback = write.callback
    if (callback) callback(error)
    delete writes[id]
  }

  function onEntry (entry, index) {
    // TODO: Address cases where index is more than head + 1
    // TODO: Ensure that entry events are emitted in index order
    if (index > head) head = index
    emit('entry', entry, index)
  }
}

inherits(TCPLogClient, EventEmitter)

TCPLogClient.prototype.write = function (entry, callback) {
  var writes = this._writes
  var id = uuid()
  writes[id] = {
    sent: false,
    entry: entry,
    callback: callback || false,
    timeout: setTimeout(function () {
      if (callback) callback(new Error('timeout'))
      delete writes[id]
    }, this._timeout)
  }
  if (this._json) {
    this._json.write(writeMessage(entry, id))
    writes[id].sent = true
  }
}

function writeMessage (entry, id) {
  return {id: id, entry: entry}
}

TCPLogClient.prototype.reconnect = function () {
  this._reconnect.disconnect()
}

TCPLogClient.prototype.disconnect = function () {
  this._reconnect.reconnect = false
  this._reconnect.disconnect()
}

function isCurrent (message) {
  return 'current' in message && message.current === true
}

function isEntry (message) {
  return 'entry' in message && 'index' in message
}

function isConfirmation (message) {
  return 'id' in message && 'index' in message
}

function isWriteError (message) {
  return 'id' in message && 'error' in message
}

function isReadError (message) {
  return 'index' in message && 'error' in message
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
