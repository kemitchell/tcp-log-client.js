var ndjson = require('ndjson')
var pump = require('pump')
var through2 = require('through2')
var uuid = require('uuid').v4

module.exports = {
  createReadStream: createReadStream,
  createWriteStream: createWriteStream
}

function createReadStream (socket, from) {
  var returned = pump(
    socket,
    ndjson.parse({strict: true}),
    through2.obj(function (message, _, done) {
      if (message.current === true) returned.emit('current')
      else if ('index' in message) {
        if ('error' in message) returned.emit('error', message)
        else if ('entry' in message) this.push(message)
      }
      done()
    })
  )
  socket.write(JSON.stringify({from: from || 1}) + '\n')
  return returned
}

function createWriteStream (socket) {
  var returned = through2.obj(function (chunk, _, done) {
    done(null, {id: uuid(), entry: chunk})
  })
  pump(returned, ndjson.stringify(), socket)
  return returned
}
