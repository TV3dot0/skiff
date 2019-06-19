'use strict'

const debug = require('debug')('skiff.network.passive.server')
const net = require('net')
const Duplex = require('stream').Duplex
const Msgpack = require('msgpack5')
const merge = require('deepmerge')

const defaultOptions = {
  objectMode: true
}

class Server extends Duplex {

  constructor (_options) {
    const options = merge(_options, defaultOptions)
    debug('building server with options %j', options)
    super(options)
    this._options = options
    this._server = net.createServer(this._onConnection.bind(this))
    this._server.once('close', () => {
      this.emit('closed')
    })
    this._peers = {}
    this._listen()
  }

  close () {
    this._server.close()
  }

  _listen () {
    this._server.listen(this._options, () => {
      console.log('server listening with options %j', this._options)
      this.emit('listening', this._options)
    })
  }

  _read () {
    // do nothing
  }

  _write (message, _, callback) {
    debug('server trying to write %j', message)
    const peer = this._peers[message.to]
    if (peer) {
      debug('I have peer for message to %s', message.to)
      peer.write(message, callback)
    } else {
      debug('I have no peer to send to')
      callback()
    }
  }

  _onConnection (conn) {
    console.log('new server connection')
    const server = this
    const msgpack = Msgpack()

    conn.once('finish', () => console.log('connection ended'))

    const fromPeer = msgpack.decoder()
    conn
      .pipe(fromPeer)
      .on('error', onPeerError)

    const toPeer = msgpack.encoder()
    toPeer
      .pipe(conn)
      .on('error', onPeerError)

    fromPeer.on('data', this._onMessage.bind(this, conn, toPeer))

    function onPeerError (err) {
      console.log('peer error: %s', err.stack)
      server.emit('warning', err)
    }
  }

  _onMessage (conn, toPeer, message) {
    //if (message.action === 'AppendEntries' && Array.isArray(message.params.entries) && message.params.entries.length > 0)
      //console.log('incoming message',
      //    message.from,
      //    message.to,
      //    message.params.entries.reduce((acc,e) => (acc += (e.c.type + ' ' + (e.c.key? e.c.key.toString('hex'):' ') + ''), acc), ''));
    const from = message.from
    if (from) {
      const peer = this._peers[from]
      if (!peer || peer !== toPeer) {
        debug('setting up peer %s', from)
        this._peers[from] = toPeer
        conn.once('finish', () => delete this._peers[from])
        if (peer) {
          peer.end()
        }
      } else {
        debug('no need to setup new peer')
      }
      debug('pushing out message from %s', from)
      this.push(message)
    } else {
      debug('no .from in message')
    }
  }

}

module.exports = Server
