var EventEmitter = require('events').EventEmitter
var abs = require('abstract-blob-store')
var client = require('./')
var devnull = require('dev-null')
var levelLogs = require('level-logs')
var levelup = require('levelup')
var logServerHandler = require('tcp-log-server')
var memdown = require('memdown')
var net = require('net')
var pino = require('pino')
var sha256 = require('sha256')
var tape = require('tape')

tape('start a test server', function (test) {
  withTestServer(function (server, port) {
    test.pass('started a server')
    server.close()
    test.end()
  })
})

tape('read and write', function (test) {
  withTestServer(function (server, port) {
    var readStream = client.createReadStream(socket(port))
    .on('data', function (data) {
      test.deepEqual(data.entry, {a: 1}, 'event with entry')
      test.equal(data.index, 1, 'event with index')
      writeStream.end()
      readStream.destroy()
      server.close()
      test.end()
    })
    var writeStream = client.createWriteStream(socket(port))
    writeStream.write({a: 1})
  })
})

tape('read previous writes', function (test) {
  withTestServer(function (server, port) {
    var writeStream = client.createWriteStream(socket(port))
    var entries = [{a: 1}, {b: 2}, {c: 3}]
    var received = []
    var receivedCurrent = false
    var receivedEntries = false
    var readStream = client.createReadStream(socket(port))
    .once('current', function () {
      test.pass('current event')
      receivedCurrent = true
      done()
    })
    .on('data', function (data) {
      received.push(data.entry)
      if (received.length === entries.length) {
        test.deepEqual(received, entries, 'received entries')
        receivedEntries = true
        done()
      }
    })
    function done () {
      if (receivedEntries && receivedCurrent) {
        writeStream.end()
        readStream.destroy()
        server.close()
        test.end()
      }
    }
    entries.forEach(function (entry) {
      writeStream.write(entry)
    })
  })
})

function socket (port) {
  return net.connect(port).setKeepAlive(true)
}

function withTestServer (callback) {
  memdown.clearGlobalStore()
  var level = levelup('', {db: memdown})
  var logs = levelLogs(level, {valueEncoding: 'json'})
  var blobs = abs()
  var log = pino({}, devnull())
  var emitter = new EventEmitter()
  var handler = logServerHandler(log, logs, blobs, emitter, sha256)
  var server = net.createServer()
  .on('connection', handler)
  .once('close', function () { level.close() })
  .listen(0, function () { callback(server, this.address().port) })
}
