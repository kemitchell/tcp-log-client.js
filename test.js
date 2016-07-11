var EventEmitter = require('events').EventEmitter
var TCPLogClient = require('./')
var abs = require('abstract-blob-store')
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
  var level = levelup('', {db: memdown})
  var logs = levelLogs(level, {valueEncoding: 'json'})
  var blobs = abs()
  var log = pino({}, devnull())
  var emitter = new EventEmitter()
  var handler = logServerHandler(log, logs, blobs, emitter, sha256)
  var connections = streamSet()
  var server = net.createServer()
  .on('connection', function (socket) { connections.add(socket) })
  .on('connection', handler)
  .once('close', function () { level.close() })
  .listen(0, function () {
    callback(server, this.address().port, connections)
  })
}

tape('start a test server', function (test) {
  startTestServer(function (server, port) {
    test.pass('started a server')
    server.close()
    test.end()
  })
})

tape('read and write from same client', function (test) {
  startTestServer(function (server, port) {
    var entries = [{a: 1}, {b: 2}, {c: 3}]
    var received = []
    var client = new TCPLogClient({server: {port: port}})
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
      entries.forEach(function (entry) {
        client.writeStream.write(entry)
      })
    })
  })
})

tape('read another client\'s writes', function (test) {
  startTestServer(function (server, port) {
    var options = {server: {port: port}}
    var reader = new TCPLogClient(options).once('ready', function () {
      var writer = new TCPLogClient(options).once('ready', function () {
        var entries = [{a: 1}, {b: 2}, {c: 3}]
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
        entries.forEach(function (entry) {
          writer.writeStream.write(entry)
        })
      })
    })
  })
})

tape('read another client\'s previous writes', function (test) {
  startTestServer(function (server, port) {
    var options = {server: {port: port}}
    var reader = new TCPLogClient(options).once('ready', function () {
      var writer = new TCPLogClient(options).once('ready', function () {
        var entries = [{a: 1}, {b: 2}, {c: 3}]
        entries.forEach(function (entry) {
          writer.writeStream.write(entry)
        })
        setTimeout(function () {
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
        }, 100)
      })
    })
  })
})

tape('current event', function (test) {
  startTestServer(function (server, port) {
    var options = {server: {port: port}}
    var writer = new TCPLogClient(options).once('ready', function () {
      var entries = [{a: 1}, {b: 2}, {c: 3}]
      entries.forEach(function (entry) {
        writer.writeStream.write(entry)
      })
      var reader = new TCPLogClient(options).once('ready', function () {
        reader.readStream.once('current', function (data) {
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
    .once('ready', function () {
      connections.forEach(function (connection) {
        connection.destroy()
      })
    })
    .once('reconnect', function () {
      test.pass('reconnected')
      client.destroy()
      server.close()
      test.end()
    })
  })
})
