var EventEmitter = require('events').EventEmitter
var InMemoryBlobStore = require('abstract-blob-store')
var TCPLogClient = require('./')
var asyncMapSeries = require('async.mapseries')
var devnull = require('dev-null')
var levelLogs = require('level-logs')
var levelup = require('levelup')
var logServerHandler = require('tcp-log-server')
var memdown = require('memdown')
var net = require('net')
var pino = require('pino')
var sha256 = require('sha256')
var streamSet = require('stream-set')
var tape = require('tape')

function startTestServer (callback) {
  memdown.clearGlobalStore()
  // Use an in-memdown LevelUP storage back-end for testing.
  var level = levelup('', {db: memdown})
  var handler = logServerHandler(
    // Provide a pino logger, but pipe it to nowhere.
    pino({}, devnull()),
    levelLogs(level, {valueEncoding: 'json'}),
    // Use an in-memory blob store for testing.
    new InMemoryBlobStore(),
    new EventEmitter(),
    sha256
  )
  // Track connections so tests can close to simulate problems.
  var connections = streamSet()
  net.createServer()
  .on('connection', function (socket) { connections.add(socket) })
  .on('connection', handler)
  .once('close', function () { level.close() })
  .listen(0, function () {
    callback(this, this.address().port, connections)
  })
}

tape('start a test server', function (test) {
  startTestServer(function (server, port) {
    test.pass('started a server')
    server.close()
    test.end()
  })
})

var entries = [{a: 1}, {b: 2}, {c: 3}]

tape('read and write from same client', function (test) {
  startTestServer(function (server, port) {
    var received = []
    var client = new TCPLogClient({server: {port: port}})
    .connect()
    .once('ready', function () {
      client.readStream.on('data', function (data) {
        received.push(data.entry)
        if (received.length === entries.length) {
          test.deepEqual(received, entries, 'received entries')
          client.destroy()
          server.close()
          test.end()
        }
      })
      entries.forEach(function (entry) { client.write(entry) })
    })
  })
})

tape('writes call back with indices', function (test) {
  startTestServer(function (server, port) {
    var client = new TCPLogClient({server: {port: port}})
    .connect()
    .once('ready', function () {
      var expected = []
      var received = []
      asyncMapSeries(entries, function (entry, done) {
        client.write(entry, function (error, index) {
          test.ifError(error)
          expected.push({index: index, entry: entry})
        })
      })
      client.readStream.on('data', function (data) {
        received.push(data)
        if (received.length === expected.length) {
          test.deepEqual(received, expected, 'received entries')
          client.destroy()
          server.close()
          test.end()
        }
      })
    })
  })
})

tape('read another client\'s writes', function (test) {
  startTestServer(function (server, port) {
    var options = {server: {port: port}}
    var reader = new TCPLogClient(options)
    .connect()
    .once('ready', function () {
      var writer = new TCPLogClient(options)
      .connect()
      .once('ready', function () {
        var received = []
        reader.readStream.on('data', function (data) {
          received.push(data.entry)
          if (received.length === entries.length) {
            test.deepEqual(received, entries, 'received entries')
            reader.destroy()
            writer.destroy()
            server.close()
            test.end()
          }
        })
        entries.forEach(function (entry) { writer.write(entry) })
      })
    })
  })
})

tape('read another client\'s previous writes', function (test) {
  startTestServer(function (server, port) {
    var options = {server: {port: port}}
    var reader = new TCPLogClient(options)
    .connect()
    .once('ready', function () {
      var writer = new TCPLogClient(options)
      .connect()
      .once('ready', function () {
        entries.forEach(function (entry) { writer.write(entry) })
        setImmediate(function () {
          var received = []
          reader.readStream.on('data', function (data) {
            received.push(data.entry)
            if (received.length === entries.length) {
              test.deepEqual(received, entries, 'received entries')
              reader.destroy()
              writer.destroy()
              server.close()
              test.end()
            }
          })
        })
      })
    })
  })
})

tape('current event', function (test) {
  startTestServer(function (server, port) {
    var options = {server: {port: port}}
    var writer = new TCPLogClient(options)
    .connect()
    .once('ready', function () {
      entries.forEach(function (entry) { writer.write(entry) })
      var reader = new TCPLogClient(options)
      .connect()
      .once('ready', function () {
        reader.once('current', function (data) {
          test.pass('current event emitted')
          reader.destroy()
          writer.destroy()
          server.close()
          test.end()
        })
      })
    })
  })
})

tape('reconnect', function (test) {
  startTestServer(function (server, port, connections) {
    var options = {server: {port: port}}
    var client = new TCPLogClient(options)
    .connect()
    .once('ready', function () { destroyAll(connections) })
    .once('reconnect', function () {
      test.pass('reconnected')
      client.destroy()
      server.close()
      test.end()
    })
  })
})

tape('read after reconnect', function (test) {
  startTestServer(function (server, port, connections) {
    var options = {server: {port: port}}
    var received = []
    var client = new TCPLogClient(options)
    .connect()
    .once('ready', function () {
      client.write(entries[0], function (error) {
        test.ifError(error, 'no error')
        client.once('ready', function () {
          client.write(entries[1])
          client.write(entries[2])
        })
        destroyAll(connections)
      })
    })
    client.readStream.on('data', function (data) {
      received.push(data.entry)
      if (received.length === entries.length) {
        test.deepEqual(received, entries, 'received entries')
        client.destroy()
        server.close()
        test.end()
      }
    })
  })
})

tape('fail', function (test) {
  startTestServer(function (server, port, connections) {
    var options = {
      server: {port: port},
      reconnect: {initialDelay: 1, maxDelay: 2, failAfter: 1}
    }
    var client = new TCPLogClient(options)
    .connect()
    .once('ready', function () {
      server.close()
      destroyAll(connections)
    })
    .once('fail', function () {
      test.pass('fail')
      client.destroy()
      server.close()
      test.end()
    })
  })
})

tape('destroy ends read stream', function (test) {
  startTestServer(function (server, port, connections) {
    var options = {server: {port: port}}
    var client = new TCPLogClient(options)
    .connect()
    .once('ready', function () { client.destroy() })
    client.readStream.once('finish', function () {
      test.pass('stream ended')
      server.close()
      test.end()
    })
  })
})

function destroyAll (connections) {
  connections.forEach(function (connection) {
    connection.destroy()
  })
}
