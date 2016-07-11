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
  if (!(this instanceof TCPLogClient)) return new TCPLogClient(options)

  var client = this

  // Apply default options.
  var serverOptions = options.server
  var from = options.from || 0
  var reconnectOptions = options.reconnect || {}

  // Whether the client is currently connected.
  client.connected = false

  // When the client is connected, a stream of log entries.
  client.readStream = through2.obj(function (chunk, _, done) {
    // Advance the last-entry-seen counter.
    from = chunk.index
    done(null, chunk)
  })

  // The stream provided for consumption by reconnect-core.
  client._stream = null

  // Whether the client has ever successfully connected to the server.
  // Affects error event handling.
  var everConnected = true

  // A UUID-to-function map of callbacks for writes to the log. Used to
  // issue callbacks when the server responds with write confirmations.
  client._writeCallbacks = {}

  // Create a reconnect-core instance for TCP connection to server.
  client._reconnect = reconnect(function () {
    return net.connect(serverOptions).setKeepAlive(true)
  })(reconnectOptions, function (stream) {
    everConnected = true
    // Create a stream to filter out entries for reading.
    var filterStream = createReadStream(stream)
    .once('current', function () { client.emit('current') })
    // Issue a read request from one past the last-seen index.
    stream.write(JSON.stringify({from: from + 1}) + '\n')
    if (client._filterStream) {
      client._filterStream.removeAllListeners()
      client._filterStream.unpipe()
    }
    client._filterStream = filterStream
    filterStream.pipe(client.readStream, {end: false})
    client._stream = stream
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
  .on('backoff', function () { client.emit('backoff') })
  .on('disconnect', function (error) {
    if (client.readStream) client.readStream.unpipe()
    client.connected = false
    failPendingWrites('Disconnected from server.')
    client.emit('disconnect', error)
  })
  .on('error', function (error) {
    if (everConnected) {
      var code = error.code
      if (code === 'EPIPE') failPendingWrites('Server closed the connection.')
      else if (code === 'ECONNRESET') return
      else if (code === 'ECONNREFUSED') return
      else client.emit('error', error)
    } else client.emit('error', error)
  })
  .on('fail', function () { client.emit('fail') })

  function createReadStream (socket) {
    var returned = pump(
      socket,
      ndjson.parse({strict: true}),
      through2.obj(function (message, _, done) {
        if (message.current === true) returned.emit('current')
        else if ('index' in message) {
          if ('error' in message) returned.emit('error', message)
          // Pass through log entries.
          else if ('entry' in message) {
            this.push(message)
          // Callback for confirmed writes.
          } else if ('id' in message) {
            var id = message.id
            var callback = client._writeCallbacks[id]
            delete client._writeCallbacks[id]
            callback(null, message.index)
          }
        }
        done()
      })
    )
    return returned
  }

  function failPendingWrites (message) {
    Object.keys(client._writeCallbacks).forEach(function (id) {
      client._writeCallbacks[id](new Error(message))
    })
    client._writeCallbacks = {}
  }
}

inherits(TCPLogClient, EventEmitter)

TCPLogClient.prototype.destroy = function () {
  this._reconnect.reconnect = false
  this._reconnect.disconnect()
  this.readStream.end()
}

TCPLogClient.prototype.write = function (entry, callback) {
  if (!this.connected) {
    throw new Error('Cannot write when disconnected.')
  } else {
    // Generate a UUID for the write. The server will echo the UUID back to
    // confirm the write.
    var id = uuid()
    this._writeCallbacks[id] = callback || noop
    return this._stream.write(JSON.stringify({id: id, entry: entry}) + '\n')
  }
}

function noop () { return }

TCPLogClient.prototype.connect = function () {
  this._reconnect.connect()
  return this
}
