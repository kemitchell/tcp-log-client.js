Sync with [tcp-log-server]s.

[tcp-log-server]: https://npmjs.com/packages/tcp-log-server

```javascript
var client = new TCPLogClient({port: port})

client.on('entry', function (entry, index) {
  console.log(entry)
})

client.write({a: 1})

// ...

client.disconnect()
```
