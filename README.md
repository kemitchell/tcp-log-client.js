Stream and write [tcp-log-server] entries.  Reconnect automatically.

[tcp-log-server]: https://npmjs.com/packages/tcp-log-server

```javascript
var client = new TCPLogClient({
  // Use these options for `require('net').connect(options)`.
  server: {port: port},
  // Enable TCP keepalive. Enabled by default.
  keepAlive: true,
  // Disable the Nagle algorithm. Disabled by default.
  noDelay: true,
  // Start reading from entry index 1. 1 by default.
  from: 1,
  // Stop trying to reconnect and fail after 5 attempts.
  reconnect: {failAfter: 5}
})
.on('error', function (error) { console.error(error) })
.on('fail', function () { console.error('Failed to reconnect.') })
.once('ready', function () {
  if (client.connected) {
    client.write({example: 'entry'}, function (error, index) {
      console.log('New entry index is %d', index)
      // Permanently disconnect and end `client.readStream`.
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

See also:

- [net documentation](https://nodejs.org/api/net.html#net_socket_connect_options_connectlistener) for `options.server`
- [reconnect-core](https://www.npmjs.com/package/reconnect-core) for `options.reconnect`
- [migrate-versioned-log](https://www.npmjs.com/package/migrate-versioned-log) for versioning log entries
