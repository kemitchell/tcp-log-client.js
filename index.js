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
  client.writeStream = null
  var writes = {}

  var successfullyConnected = true

  client._reconnect = reconnect(function () {
    return net.connect(serverOptions).setKeepAlive(true)
  })(reconnectOptions, function (stream) {
    successfullyConnected = true
    writes = {}
    var readStream = createReadStream(stream)
    .once('current', function () { client.emit('current') })
    stream.write(JSON.stringify({from: from + 1}) + '\n')
    client.readStream = readStream
    client.writeStream = createWriteStream(stream)
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
    client.emit('disconnect', error)
  })
  .on('error', function (error) {
    if (!successfullyConnected || !trapError(error)) {
      client.emit('error', error)
    }
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
          }
        }
        done()
      })
    )
    return returned
  }

  function createWriteStream (socket) {
    var returned = through2.obj(function (chunk, _, done) {
      var id = uuid()
      writes[id] = chunk
      done(null, {id: id, entry: chunk})
    })
    pump(returned, ndjson.stringify(), socket)
    return returned
  }
}

function trapError (error) {
  return error.code === 'ECONNRESET' || error.code === 'ECONNREFUSED'
}

inherits(TCPLogClient, EventEmitter)

TCPLogClient.prototype.destroy = function () {
  this._reconnect.reconnect = false
  this._reconnect.disconnect()
}
