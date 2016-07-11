var EventEmitter = require('events').EventEmitter
var reconnect = require('reconnect-core')
var inherits = require('util').inherits
var ndjson = require('ndjson')
var net = require('net')
var pump = require('pump')
var through2 = require('through2')
var uuid = require('uuid').v4

module.exports = TCPLogClient

function TCPLogClient (options) {
  if (!(this instanceof TCPLogClient)) {
    return new TCPLogClient(options)
  }

  var client = this

  var serverOptions = options.server
  var from = options.from || 0
  var reconnectOptions = options.reconnect || {}

  client.connected = false
  client.readStream = null

  client._connection = null
  var successfullyConnected = true
  client._writes = {}

  client._reconnect = reconnect(function () {
    return net.connect(serverOptions).setKeepAlive(true)
  })(reconnectOptions, function (stream) {
    successfullyConnected = true
    var readStream = createReadStream(stream)
    .once('current', function () { client.emit('current') })
    stream.write(JSON.stringify({from: from + 1}) + '\n')
    client.readStream = readStream
    client._connection = stream
    client.emit('ready')
  })
  .on('connect', function (connection) {
    client.connected = true
    client.emit('connect', connection)
  })
  .on('reconnect', function (number, delay) {
    client.connected = true
    client.emit('reconnect', number, delay)
  })
  .on('disconnect', function (error) {
    clearWrites('Disconnected from server.')
    client.emit('disconnect', error)
  })
  .on('error', function (error) {
    if (successfullyConnected) {
      var code = error.code
      if (code === 'EPIPE') clearWrites('Server closed the connection.')
      else if (code === 'ECONNRESET') return
      else if (code === 'ECONNREFUSED') return
      else client.emit('error', error)
    } else client.emit('error', error)
  })

  client._reconnect.connect()

  function createReadStream (socket) {
    var returned = pump(
      socket,
      ndjson.parse({strict: true}),
      through2.obj(function (message, _, done) {
        if (message.current === true) returned.emit('current')
        else if ('index' in message) {
          if ('error' in message) returned.emit('error', message)
          else if ('entry' in message) {
            from = message.index
            this.push(message)
          } else if ('id' in message) {
            var id = message.id
            var callback = client._writes[id]
            delete client._writes[id]
            callback(null, message.index)
          }
        }
        done()
      })
    )
    return returned
  }

  function clearWrites (message) {
    Object.keys(client._writes).forEach(function (id) {
      var callback = client._writes[id]
      callback(new Error(message))
    })
    client._writes = {}
  }
}

inherits(TCPLogClient, EventEmitter)

TCPLogClient.prototype.destroy = function () {
  this._reconnect.reconnect = false
  this._reconnect.disconnect()
}

TCPLogClient.prototype.write = function (entry, callback) {
  if (!this.connected) {
    throw new Error('Cannot write when disconnected.')
  } else {
    var id = uuid()
    this._writes[id] = callback || noop
    this._connection.write(JSON.stringify({id: id, entry: entry}) + '\n')
  }
}

function noop () { return }
