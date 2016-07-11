Stream and write [tcp-log-server] entries.  Reconnect automatically.

[tcp-log-server]: https://npmjs.com/packages/tcp-log-server

```javascript
var client = new TCPLogClient({
  // Options for `require('net').connect(options)`
  server: {host: 'localhost', port: port},
  // TCP keepalive. Enabled by default.
  keepAlive: true,
  // Nagle algorithm. Disabled by default.
  noDelay: true,
  // Log index to start from
  from: 1,
  // Stop trying to reconnect and fail after 5 attempts.
  reconnect: {failAfter: 5}
})
.on('error', function (error) { console.error(error) })
.on('fail', function () { console.error('Could not connect.') })
.once('ready', function () {
  if (client.connected) {
    client.write({example: 'entry'}, function (error, index) {
      // ...
      client.destroy()
    })
  }
})

// Readable stream of log entries.
// Entries added with `client.write()` will be streamed, too.
client.readStream.on('data', function (chunk) {
  console.log(chunk.index)
  console.log(chunk.entry)
})
```
