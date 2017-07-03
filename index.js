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
  if (!(this instanceof TCPLogClient)) {
    return new TCPLogClient(options)
  }

  var client = this

  // Apply default options.
  var serverOptions = options.server
  var reconnectOptions = options.reconnect || {}
  var highestIndexReceived = Number(options.from) || 0
  var keepAlive = Boolean(options.keepalive) || true
  var noDelay = Boolean(options.noDelay) || true
  var batchSize = Number(options.batchSize) || 5

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
  var everConnected = false

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
    pullData()
    client.emit('ready')
  })
    .on('error', function (error) {
      // If the client has never successfully connected to the server,
      // it passes all errors through.
      /* istanbul ignore next */
      if (!everConnected) {
        client.emit('error', error)
      // Once the client has successfully connected, it may encounter
      // errors from which it can recover by reconnecting. Those errors
      // aren't passed through.
      } else {
        var code = error.code
        if (code === 'EPIPE') {
          failPendingWrites('Server closed the connection.')
        } else if (code === 'ECONNRESET') {
          // pass
        } else if (code === 'ECONNREFUSED') {
          // pass
        } else {
          client.emit('error', error)
        }
      }
    })

  proxyEvent(reconnecter, 'disconnect', function () {
    client.readStream.unpipe()
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

  // Emit events from a stream the client manages as the client's own.
  function proxyEvent (emitter, event, optionalCallback) {
    emitter.on(event, function () {
      if (optionalCallback) {
        optionalCallback.apply(client, arguments)
      }
      client.emit(event)
    })
  }

  function pullData () {
    var readMessage = {
      from: highestIndexReceived + 1,
      read: batchSize
    }
    client._socketStream.write(JSON.stringify(readMessage) + '\n')
  }

  function createReadStream (socket) {
    var returned = pump(
      socket,
      ndjson.parse({strict: true}),
      through2.obj(function (message, _, done) {
        /* istanbul ignore else */
        if (message.current === true) {
          returned.emit('current')
        } else if ('index' in message) {
          /* istanbul ignore if */
          /* istanbul ignore else */
          if ('error' in message) {
            returned.emit('error', message)
          // Pass log entries through.
          } else if ('entry' in message) {
            this.push(message)
          // Call back for confirmed writes.
          } else if ('id' in message) {
            var id = message.id
            var callback = client._writeCallbacks[id]
            delete client._writeCallbacks[id]
            callback(null, message.index)
          /* istanbul ignore next */
          } else {
            emitBadMessageError(message)
          }
        } else if ('head' in message) {
          if (client.readStream._readableState === false) {
            client.readStream.once('drain', function () {
              pullData()
            })
          } else {
            pullData()
          }
        } else {
          emitBadMessageError(message)
        }
        done()
      })
    )
    return returned
  }

  /* istanbul ignore next */
  function emitBadMessageError (message) {
    var error = new Error('invalid message')
    error.message = message
    client.emit('error', error)
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
