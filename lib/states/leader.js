'use strict'

const debug = require('debug')('skiff.states.leader')
const async = require('async')
const timers = require('timers')
const once = require('once')

const Base = require('./base')
const PeerLeader = require('../peer-leader')

class Leader extends Base {

  constructor (node, _options) {
    const options = Object.assign({}, _options || {}, { electionTimeout: true }) //???
    super(node, options)
    this.name = 'leader';
    this._followers = {}
    debug('new leader', node.id)
  }

  start () {
    debug('%s is leader', this.id);
    debug('peers', this._node.network.peers());
    this._followers = this._node.network.peers().reduce((followers, address) => {
      const peerLeader = new PeerLeader(address, this._node, this._options);
      followers[address] = peerLeader;
      return followers
    }, {});
    debug('leader followers', Object.keys(this._followers));
    super.start();
    this._waitForConsensus(this._node.state.log._commitIndex, {}, this._node.network.peers())
  }

  stop () {
    Object.keys(this._followers)
      .map(address => this._followers[address])
      .forEach(follower => {
        follower.stop()
        follower.removeAllListeners()
      })

    super.stop()
  }

  join (address) {
    const follower = this._followers[address]
    if (!follower) {
      this._followers[address] = new PeerLeader(address, this._node, this._options)
    }
  }

  leave (address) {
    const follower = this._followers[address]
    if (follower) {
      follower.stop()
      delete this._followers[address]
    }
  }

  peers () {
    return Object.keys(this._followers)
      .map(addr => this._followers[addr])
      .map(peer => peer.state())
  }

  command (consensuses, command, options, done) {
    const index = this._node.log.push(command)

    process.nextTick(() => {
      async.eachSeries(consensuses, this._waitForConsensus.bind(this, index, options), (err) => {
        if (err) {
          done(err)
        } else {
          this._node.state.log.commit(index, done)
        }
      })
    })
  }

  _waitForConsensus (waitingForIndex, options, consensus, _done) {
    console.log('_waitForConsensus %d', waitingForIndex)
    //console.log('this', this)
    const done = once(_done || noop)

    // vote for self
    let votes = 1

    if (!consensus.length) {
      return done()
    }

    let waitingFor = options.alsoWaitFor
    if (!Array.isArray(waitingFor)) {
      waitingFor = [waitingFor]
    }
    waitingFor = waitingFor.filter(address => address && address !== this.id)

    // TODO: consider using another options as timeout value (waitForConsensusTimeout?)
    const timeout = timers.setTimeout(onTimeout, this._options.rpcTimeoutMS)
    const peers = consensus.map(address => {
      let follower = this._followers[address]
      if (!follower) {
        follower = this._followers[address] = new PeerLeader(address, this._node, this._options)
      }
      return follower
    })

    peers.forEach(peer => {
      peer.on('committed', onPeerCommit)
      peer.needsIndex(waitingForIndex)
    })

    function onPeerCommit (peer, peerIndex) {
      if (peerIndex >= waitingForIndex) {
        votes++
        peer.removeListener('committed', onPeerCommit)
        waitingFor = waitingFor.filter(addr => peer._address !== peer._address)
      }
      if (isMajority(consensus, votes) && !waitingFor.length) {
        console.log('have consensus for index %d', waitingForIndex)
        cleanup()
        done()
      }
    }

    function onTimeout () {
      cleanup()
      const err = new Error('timedout waiting for consensus')
      err.code = 'ETIMEOUT'
      done(err)
    }

    function cleanup () {
      timers.clearTimeout(timeout)
      peers.forEach(peer => {
        peer.removeListener('committed', onPeerCommit)
      })
    }
  }

  _onElectionTimeout () {
    // do nothing, we're the leader
  }
}

module.exports = Leader

function noop () {}

function isMajority (consensus, count) {
  const quorum = Math.floor((consensus.length + 1) / 2) + 1
  return consensus.length && count >= quorum
}
