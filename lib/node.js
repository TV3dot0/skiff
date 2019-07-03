'use strict'

const async = require('async');

const debug = require('debug')('skiff.node')
const Through = require('through2')
const EventEmitter = require('events')
const assert = require('assert')
const timers = require('timers')

const States = require('./states')
const Log = require('./log')
const RPC = require('./rpc')
const Client = require('./client')
const NotLeaderError = require('./utils/not-leader-error')

const importantStateEvents = ['election timeout']

class Node extends EventEmitter {

  constructor (id, connections, dispatcher, db, peers, options, stateId) {
    super()
    this.setMaxListeners(200);
    this.id = id
    this.stateId = stateId
    this._stopped = false
    this._connections = connections
    this._options = options
    this._dispatcher = dispatcher
    this._db = db
    this._getPeers = peers
    this.passive = this._outStream()
    this.active = this._outStream()
    this._replies = this._replyStream()

    this._stateName = undefined
    this._handlingRequest = false // to detect race conditions
    this._weakenedBefore = Date.now()

    this._leaving = []

    // persisted state
    this._term = 0
    this._votedFor = null
    this._log = new Log(
      {
        id: this.id,
        applyEntries: this._applyEntries.bind(this),
        term: this._getTerm.bind(this)
      },
      options)
    this._peers = options.peers.filter(address => address !== this.id.toString())

    debug('id:', this.id.toString())
    debug('peers:', this._peers)

    this._stateServices = {
      id,
      name: this._getStateName.bind(this),
      term: this._getTerm.bind(this),
      setTerm: this._setTerm.bind(this),
      transition: this._transition.bind(this),
      incrementTerm: this._incrementTerm.bind(this),
      getVotedFor: this._getVotedFor.bind(this),
      setVotedFor: this._setVotedFor.bind(this),
      log: this._log,
      db,
      untilNotWeakened: this._untilNotWeakened.bind(this)
    }

    this._rpc = RPC(this._stateServices, this.active, this._replies, this, this._options)

    this._client = new Client({
      id,
      rpc: this._rpc.bind(this),
      leader: this._getLeader.bind(this),
      peers: this._getLocalPeerList.bind(this),
      command: this.command.bind(this)
    }, this._options)

    this._networkingServices = {
      id: this.id,
      rpc: this._rpc,
      reply: this._reply.bind(this),
      isMajority: this._isMajority.bind(this),
      peers: this._getLocalPeerList.bind(this),
      setPeers: this._setPeers.bind(this)
    }

    this._dbServices = {
      snapshot: this._getPersistableState.bind(this),
      logEntries: this.getLogEntries.bind(this),
      applyTopologyCommand: this._applyTopologyCommand.bind(this)
    }

    this._dispatch()
  }

  stop () {
    this._stopped = true
    if (this._state) {
      this._state.stop()
    }
  }

  is (state) {
    return this._stateName === state
  }

  // -------------
  // Peers

  join (address, done) {
    if (this._peers.indexOf(address) >= 0) {
      process.nextTick(done)
    } else {
      this.command({type: 'join', peer: address}, {}, done)
    }
  }

  leave (address, done) {
    debug('%s: leave %s', this.id, address)
    if (address !== this.id.toString() && this._peers.indexOf(address) === -1) {
      process.nextTick(done)
    } else {
      this.command({type: 'leave', peer: address}, {}, done)
    }
  }

  peers (network, done) {
    if (this._stateName === 'leader') {
      if (network && network.active) {
        const peers = this._peers
          .map(peer => {
            return { id: peer }
          })
          .filter(peer => peer.id !== this.id.toString())
          .concat({
            id: this.id.toString(),
            leader: true
          })

        peers.forEach(peer => {
          peer.stats = network.active._out.peerStats(peer.id)
          if (peer.stats) {
            peer.stats.lastReceivedAgo = Date.now() - peer.stats.lastReceived
            peer.stats.lastSentAgo = Date.now() - peer.stats.lastSent
            delete peer.stats.lastReceived
            delete peer.stats.lastSent
          }
          peer.connected = this._connections.isConnectedTo(peer.id)
        })
        done(null, peers)
      } else {
        done(null, {})
      }
    } else {
      this._client.command('peers', {tries: 0}, done)
    }
  }

  _getLocalPeerList () {
    return this._peers.slice()
  }

  _setPeers (peers) {
    this._peers = peers.filter(p => p !== this.id.toString())
    this._peers.forEach(peer => this._state.join(peer))
  }

  _ensurePeer (address) {
    if ((this._peers.indexOf(address) < 0) && address !== this.id.toString()) {
      debug('%s is joining %s', this.id, address)
      this._peers.push(address)
    }
  }

  _isMajority (count) {
    const quorum = Math.floor((this._peers.length + 1) / 2) + 1
    const isMajority = count >= quorum
    //debug('%s: is %d majority? %j', this.id, count, isMajority)
    if (!isMajority) {
      //debug('%s: still need %d votes to reach majority', this.id, quorum - count)
    }
    return isMajority
  }

  // -------------
  // Internal state

  _transition (state, force) {
    debug('%s: asked to transition to state %s', this.id, state)
    if (force || state !== this._stateName) {
      debug('node %s is transitioning to state %s', this.id, state)
      const oldState = this._state
      if (oldState) {
        oldState.stop()
      }

      const State = States(state)
      this._state = new State({
        id: this.id.toString(),
        state: this._stateServices,
        network: this._networkingServices,
        log: this._log,
        command: this.command.bind(this),
        leader: this._getLeader.bind(this)
      }, this._options)

      importantStateEvents.forEach(event => {
        this._state.on(event, arg => this.emit(event, arg))
      })
      this._stateName = state
      this._state.start()

      this.emit('new state', state)
      this.emit(state)
    }
  }

  _getStateName () {
    return this._stateName
  }

  _incrementTerm () {
    this._votedFor = null
    const term = ++this._term
    return term
  }

  _getTerm () {
    return this._term
  }

  _setTerm (term) {

    this._votedFor = null
    this._term = typeof term !== 'number' ? term.parseInt : term;
    return this._term
  }

  _getVotedFor () {
    return this._votedFor
  }

  _setVotedFor (peer) {
    //debug('%s: setting voted for to %s', this.id, peer)
    this._votedFor = peer
  }

  weaken (duration) {
    this._weakenedBefore = Date.now() + duration
    this._transition('weakened')
  }

  _untilNotWeakened (callback) {
    const now = Date.now()
    if (this._weakenedBefore > now) {
      timers.setTimeout(callback, this._weakenedBefore - now)
    } else {
      process.nextTick(callback)
    }
  }

  // -------------
  // Networking

  _reply (to, messageId, params, callback) {
    //debug('%s: replying to: %s, messageId: %s, params: %j', this.id, to, messageId, params)
    this.passive.write({
      to: to,
      type: 'reply',
      from: this.id,
      id: messageId,
      params
    }, callback)
  }

  _dispatch () {
    debug('%s: _dispatch', this.id)

    if (this._stopped) {
      return
    }

    const message = this._dispatcher.next()
    if (!message) {
      this._dispatcher.once('readable', this._dispatch.bind(this))
    } else {
      //debug('%s: got message from dispatcher: %j', this.id, message)

      this.emit('message received')

      if (message.params) {
        if (message.params.term < this._term) {
          // discard message if term is greater than current term
          debug('%s: message discarded because term %d is smaller than my current term %d',
            this.id, message.params.term, this._term)
          return process.nextTick(this._dispatch.bind(this))
        }

        if (message.params.leaderId) {
          this._leaderId = message.params.leaderId
        }

        debug('%s: current term: %d', this.id, this._term)

        if (message.params.term > this._term) {
          debug('%s is going to transition to state follower because of outdated term', this.id)
          this._setTerm(message.params.term)
          this._transition('follower')
        }
      }

      if (message.type === 'request') {
        //debug('%s: request message from dispatcher: %j', this.id, message)
        this._handleRequest(message, this._dispatch.bind(this))
      } else if (message.type === 'reply') {
        //debug('%s: reply message from dispatcher: %j', this.id, message)
        this._handleReply(message, this._dispatch.bind(this))
      }
    }
  }

  _handleRequest (message, done) {
    //assert(!this._handlingRequest, 'race: already handling request')
    if (this._handlingRequest) done(new Error('race: already handling request'));
    this.emit('rpc received', message.action)
    this._handlingRequest = true

    const from = message.from
    if (from) {
      debug('%s: handling message: %j', this.id, message)
      this._ensurePeer(from)
      if (this._state) {
        this._state.handleRequest(message, err => {
          this.persist(persistError => {
            debug('%s: persisted', this.id)
            this._handlingRequest = false

            if (err) {
              done(err)
            } else {
              done(persistError)
            }
          })
        })
      } else {
        debug('error: no state available to handle request');
        done(new Error('no state available to handle request'))
      }
    } else {
      done()
    }
  }

  _handleReply (message, done) {
    debug('%s: handling reply %j', this.id, message)
    this._replies.write(message)
    done()
  }

  _outStream () {
    const self = this
    return Through.obj(transform)

    function transform (message, _, callback) {
      message.from = self.id.toString()
      this.push(message)
      callback()
    }
  }

  _replyStream () {
    const stream = Through.obj(transform)
    stream.setMaxListeners(Infinity)
    return stream

    function transform (message, _, callback) {
      this.push(message)
      callback()
    }
  }

  _getLeader () {
    return this._leaderId
  }

  // -------
  // Commands


  async command (command, options, done) {
    if (this._stateName !== 'leader') {

      // if this is not leader send command to one
      if (!options.remote) {

        //const self = this;
        const cmdPromise = (cmd) =>
            new Promise((resolve, reject) =>
                this._client.command(cmd, options, (err, data) => { if (err) reject(err); else resolve(data) }));

        if (Array.isArray(command)) {
          //debug('state command batch @follower', this.stateId, command);
          await command
              .reduce((prev, cmd) => {return prev.then(() => cmdPromise(cmd))}, Promise.resolve())
              .then(res => done(res))
              .catch(err => {
                //debug(err);
                done(err)
              });
        } else {
          //debug('state command @follower', this.stateId, command.type, command.key ? command.key.toString('hex') : '');
          this._client.command(command, options, done)
        }
      } else {
        done(new NotLeaderError(this._leaderId))
      }
    } else {

      // we are at the leader
      const consensuses = [this._peers.slice()]

      if (command === 'peers') {
        return this._getPeers(done)
      }

      // joint consensus
      if (command.type === 'join') {
        if (this._peers.indexOf(command.peer) < 0 && command.peer !== this.id.toString()) {
          this._peers.push(command.peer)
        }
        consensuses.push(this._peers.concat(command.peer))
      } else if (command.type === 'leave') {
        consensuses.push(this._peers.filter(p => p !== command.peer))
      }

      const stateCmdPromise = (cmd) =>
          new Promise((resolve, reject) =>
              this._state.command(consensuses, cmd, options, (err, _) => { if (err) reject(err); else resolve(_) }));

      const dbCmdPromise = (cmd) =>
          new Promise((resolve, reject) =>
              this._db.command(this._dbServices, cmd, options, (err, _) => { if (err) reject(err); else resolve(_) }));

      if (Array.isArray(command)) {
        //debug('state command @leader', this.stateId, command);
        await command
            .reduce((prev, cmd) => {return prev.then(() => stateCmdPromise(cmd).then(_ => dbCmdPromise(cmd)))}, Promise.resolve())
            .then(res => done(res))
            .catch(err => {
              //debug(err);
              done(err)
            });
      } else {
        //debug('state command @leader', this.stateId, command.type, command.key ? command.key.toString('hex') : '');
        this._state.command(consensuses, command, options, (err, result) => {
          if (err) {
            done(err)
          } else {
            this._db.command(this._dbServices, command, options, done)
          }
        })
      }
    }
  }

  readConsensus (done) {
    this.command({ type: 'read' }, { alsoWaitFor: this.id.toString() }, done)
  }

  waitFor (peer, done) {
    this.command({ type: 'read' }, { alsoWaitFor: peer }, done)
  }

  // -------
  // Persistence

  _getPersistableState () {
    return {
      currentTerm: this._getTerm(),
      votedFor: this._votedFor,
      peers: this._peers
    }
  }

  getLogEntries () {
    return this._log.entries()
  }

  _applyEntries (entries, done) {
    this._db.applyEntries(entries, this._applyTopologyCommands.bind(this), done)
  }

  _applyTopologyCommands (commands) {
    //debug('%s: _applyTopologyCommands %j', this.id, commands)
    commands.forEach(this._applyTopologyCommand.bind(this))
  }

  _applyTopologyCommand (command) {
    //debug('%s: applying topology command: %j', this.id, command)
    if (command.type === 'join') {
      if (command.peer !== this.id.toString()) {
        if (this._peers.indexOf(command.peer) === -1) {
          this._peers = this._peers.concat(command.peer)
        }
        this._state.join(command.peer)
      }
      this.emit('joined', command.peer)
    } else if (command.type === 'leave') {
      //debug('%s: applying leave command: %j', this.id, command)
      if (this._leaving.indexOf(command.peer) < 0) {
        this._leaving.push(command.peer)
        timers.setTimeout(() => {
          this._segregatePeer(command.peer)
          this._leaving = this._leaving.filter(p => p !== command.peer)
        }, this._options.waitBeforeLeaveMS)
      }
    }
  }

  _segregatePeer (peer) {
    //debug('%s: segregating peer', this.id, peer)
    this._peers = this._peers.filter(p => p !== peer)
    //debug('%s: peers now are: %j', this.id, this._peers)
    this._state.leave(peer)
    if (this._network) {
      this._network.active.disconnect(peer)
    }
    this.emit('left', peer)
    //debug('%s: emitted left for peer', this.id, peer)
  }

  persist (done) {
    debug('%s: persisting', this.id)
    this._db.persist(this._dbServices, done)
  }

}

module.exports = Node
