var EventEmitter = require('events').EventEmitter
var runParallel = require('run-parallel')
var abs = require('abstract-blob-store')
var TCPLogClient = require('./')
var devnull = require('dev-null')
var levelLogs = require('level-logs')
var levelup = require('levelup')
var memdown = require('memdown')
var net = require('net')
var pino = require('pino')
var tape = require('tape')
var logServerHandler = require('tcp-log-server')

tape('start a test server', function (test) {
  withTestServer(function (server, port) {
    test.pass('started a server')
    server.close()
    test.end()
  })
})

tape('read and write', function (test) {
  withTestServer(function (server, port) {
    var calledBack = false
    var entryEvent = false
    var client = new TCPLogClient({port: port})
      .on('entry', function (entry, index) {
        test.pass('entry event')
        test.deepEqual(entry, {a: 1}, 'event with entry')
        test.equal(index, 1, 'event with index')
        entryEvent = true
        done()
      })
    client.write({a: 1}, function (error, index) {
      test.pass('callback')
      test.ifError(error, 'callback without error')
      test.equal(index, 1, 'callback with index')
      calledBack = true
      done()
    })
    function done () {
      if (!entryEvent || !calledBack) return
      client.disconnect()
      server.close()
      test.end()
    }
  })
})

tape('read previous writes', function (test) {
  withTestServer(function (server, port) {
    var writer = new TCPLogClient({port: port})
    var entries = [{a: 1}, {b: 2}, {c: 3}]
    var writes = entries.map(function (entry) {
      return writer.write.bind(writer, entry)
    })
    runParallel(writes, function (error) {
      test.ifError(error, 'no writer error')
      writer.disconnect()
      var received = []
      var receivedCurrent = false
      var receivedEntries = false
      var reader = new TCPLogClient({port: port})
        .once('current', function () {
          test.pass('current event')
          receivedCurrent = true
          done()
        })
        .on('entry', function (entry, index) {
          received.push(entry)
          if (received.length === entries.length) {
            test.deepEqual(received, entries, 'received entries')
            receivedEntries = true
            done()
          }
        })
      function done () {
        if (!receivedEntries || receivedCurrent) return
        reader.disconnect()
        server.close()
        test.end()
      }
    })
  })
})

function withTestServer (callback) {
  memdown.clearGlobalStore()
  var level = levelup('', {db: memdown})
  var logs = levelLogs(level, {valueEncoding: 'json'})
  var blobs = abs()
  var log = pino({}, devnull())
  var emitter = new EventEmitter()
  var handler = logServerHandler(log, logs, blobs, emitter)
  var server = net.createServer()
    .on('connection', handler)
    .once('close', function () { level.close() })
    .listen(0, function () {
      callback(server, this.address().port)
    })
}
