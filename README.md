Sync with a [tcp-log-server].

[tcp-log-server]: https://npmjs.com/packages/tcp-log-server

```javascript
var client = new TCPLogClient({port: port})

client.on('entry', function (entry, index) { })

client.write({a: 1}, function (error, index) { })

client.disconnect()
```
