var EventEmitter = require('events').EventEmitter
var inherits = require('util').inherits
var ndjson = require('ndjson')
var net = require('net')
var pump = require('pump')
var reconnect = require('reconnect-core')
var through2 = require('through2')
var uuid = require('uuid').v4

module.exports = TCPLogClient

function TCPLogClient (options) {
  /* istanbul ignore if */
  if (!(this instanceof TCPLogClient)) return new TCPLogClient(options)

  var client = this

  // Apply default options.
  var serverOptions = options.server
  var reconnectOptions = options.reconnect || {}
  var highestIndexReceived = Number(options.from) || 0
  var keepAlive = Boolean(options.keepalive) || true
  var noDelay = Boolean(options.noDelay) || true

  // Whether the client is currently connected.
  client.connected = false

  // When the client is connected, a stream of log entries.
  client.readStream = through2.obj(function (chunk, _, done) {
    // Advance the last-entry-seen counter.
    highestIndexReceived = chunk.index
    done(null, chunk)
  })

  // The stream provided for consumption by reconnect-core.
  client._socketStream = null

  // Whether the client has ever successfully connected to the server.
  // Affects error event handling.
  var everConnected = true

  // A UUID-to-function map of callbacks for writes to the log. Used to
  // issue callbacks when the server responds with write confirmations.
  client._writeCallbacks = {}

  // Create a reconnect-core instance for TCP connection to server.
  var reconnecter = client._reconnecter = reconnect(function () {
    return net.connect(serverOptions)
    .setKeepAlive(keepAlive)
    .setNoDelay(noDelay)
  })(reconnectOptions, function (newSocketStream) {
    // Create a stream to filter out entries for reading.
    var filterStream = createReadStream(newSocketStream)
    // Replace the old duplex stream in the pipeline.
    if (client._filterStream) {
      client._filterStream.removeAllListeners()
      client._filterStream.unpipe()
    }
    client._filterStream = filterStream
    filterStream.pipe(client.readStream, {end: false})
    client._socketStream = newSocketStream
    // Issue a read request from one past the last-seen index.
    proxyEvent(filterStream, 'current')
    var readMessage = {from: highestIndexReceived + 1}
    newSocketStream.write(JSON.stringify(readMessage) + '\n')
    client.emit('ready')
  })
  .on('error', function (error) {
    if (!everConnected) client.emit('error', error)
    else {
      var code = error.code
      if (code === 'EPIPE') {
        failPendingWrites('Server closed the connection.')
      } else if (code === 'ECONNRESET') return
      else if (code === 'ECONNREFUSED') return
      else client.emit('error', error)
    }
  })

  proxyEvent(reconnecter, 'disconnect', function () {
    if (client.readStream) client.readStream.unpipe()
    client.connected = false
    failPendingWrites('Disconnected from server.')
  })
  proxyEvent(reconnecter, 'reconnect', function () {
    client.connected = true
  })
  proxyEvent(reconnecter, 'connect', function (connection) {
    everConnected = true
    client.connected = true
  })
  proxyEvent(reconnecter, 'backoff')
  proxyEvent(reconnecter, 'fail')

  function proxyEvent (emitter, event, optionalCallback) {
    emitter.on(event, function () {
      if (optionalCallback) optionalCallback.apply(client, arguments)
      client.emit(event)
    })
  }

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
    var callbacks = client._writeCallbacks
    Object.keys(client._writeCallbacks).forEach(function (id) {
      var callback = callbacks[id]
      delete callbacks[id]
      callback(new Error(message))
    })
  }
}

inherits(TCPLogClient, EventEmitter)

TCPLogClient.prototype.destroy = function () {
  this._reconnecter.reconnect = false
  this._reconnecter.disconnect()
  this.readStream.end()
}

TCPLogClient.prototype.write = function (entry, callback) {
  if (!this.connected) {
    throw new Error(
      'Cannot write when disconnected. ' +
      'Check `client.connected` before calling `client.write()`.'
    )
  } else {
    // Generate a UUID for the write. The server will echo the UUID back
    // to confirm the write.
    var id = uuid()
    this._writeCallbacks[id] = callback || noop
    var message = JSON.stringify({id: id, entry: entry}) + '\n'
    return this._socketStream.write(message)
  }
}

function noop () { }

TCPLogClient.prototype.connect = function () {
  this._reconnecter.connect()
  return this
}
