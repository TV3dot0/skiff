'use strict';

const debug = require('debug')('skiff.db');
const timers = require('timers');
const Sublevel = require('level-sublevel');
const Once = require('once');
const async = require('async');
const ConcatStream = require('concat-stream');
const Leveldown = require('leveldown');
const Levelup = require('levelup');
const clearDB = require('./utils/clear-db');
const join = require('path').join;
const encode = require('encoding-down');


const ALLOWED_TYPES = ['put', 'del'];

const sublevelOptions = {
  keyEncoding: 'utf8',
  valueEncoding: 'json',
  asBuffer: true
};

class DB {

  // this._db = new DB(this._options.location, this.id, this._options.db, this._options.levelup)
  constructor (_location, id, db, options) {
    console.log(id);
    this.id = id;
    const dbName = id.toString().replace(/\//g, '_').replace(/\./g, '_');
    const location = join(_location, dbName);
    console.log('location', location);
    const leveldown = new db(location, options) || Leveldown;

    this._levelup = new Levelup(encode(leveldown), options);
    this._leveldown = this._levelup.db; // new DeferredLevelDown(leveldown)
    this.db = Sublevel(this._levelup, sublevelOptions);

    this.log = this.db.sublevel('log');
    this.meta = this.db.sublevel('meta');
    this.state = this.db.sublevel('state');
    this.state.clear = clearDB;

    // for debugging purposes
    this.log.toJSON = function () { return 'log' };
    this.meta.toJSON = function () { return 'meta' };
    this.state.toJSON = function () { return 'state' };

    console.log('skiff db created')
  }

  load (done) {
    async.parallel({
      log: cb => {
        const s = this.log.createReadStream()
        s.once('error', cb)
        s.pipe(ConcatStream(entries => {
          cb(null, entries.sort(sortEntries).map(fixLoadedEntry))
        }))
      },
      meta: cb => {
        async.parallel({
          currentTerm: cb => this.meta.get('currentTerm', notFoundIsOk(cb)),
          votedFor: cb => this.meta.get('votedFor', notFoundIsOk(cb)),
          peers: cb => this.meta.get('peers', notFoundIsOk(cb))
        }, cb)
      }
    }, done)

    function sortEntries (a, b) {
      const keyA = a.key
      const keyB = b.key
      const keyAParts = keyA.split(':')
      const keyBParts = keyB.split(':')
      const aTerm = Number(keyAParts[0])
      const bTerm = Number(keyBParts[0])
      if (aTerm !== bTerm) {
        return aTerm - bTerm
      }
      const aIndex = Number(keyAParts[1])
      const bIndex = Number(keyBParts[1])

      return aIndex - bIndex
    }

    function notFoundIsOk (cb) {
      return function (err, result) {
        if (err && err.message.match(/not found/i)) {
          cb()
        } else {
          cb(err, result)
        }
      }
    }
  }

  persist (state, done) {
    debug('persisting state', state)
    this._getPersistBatch(state, (err, batch) => {
      if (err) {
        done(err)
      } else {
        debug('state persisted')
        //batch.map(o => console.log(o.key, ':', o.value))
        this.db.batch(batch, done)
      }
    })
  }

  command (state, command, options, done) {
    this._getPersistBatch(state, (err, batch) => {
      if (err) {
        done(err)
      } else {
        console.log('command0', this.id, command.type, command.key?command.key.toString('hex'):'');
        if (typeof command.type === 'undefined') console.log('undefined', command);
        const isQuery = (command.type === 'get')
        const isTopology = (command.type === 'join' || command.type === 'leave')
        debug('%s: going to apply batch: %j', this.id, batch)
        this.db.batch(batch, err => {
          debug('%s: applied batch command err = %j', this.id, err)
          if (!err) {
            if (isQuery) {
              console.log('command query', command.key.toString('hex'))
              this.state.get(command.key, done)
            } else if (isTopology) {
              state.applyTopologyCommand(command)
              done()
            } else {
              done()
            }
          } else {
            done(err)
          }
        })
      }
    })
  }

  applyEntries (entries, applyTopology, done) {
    if (entries.length) {
      console.log('applying entries', this.id,
          entries.reduce((acc,e) => acc += (e.type + ': ' + (e.key?e.key.toString('hex'):'')), ''));
    }

    let dbCommands = []
    const topologyCommands = []
    entries.forEach(command => {
      if (command.type === 'join' || command.type === 'leave') {
        topologyCommands.push(command)
      } else {
        dbCommands.push(command)
      }
    })
    if (topologyCommands.length) {
      applyTopology(topologyCommands)
    }

    dbCommands = dbCommands.reduce((acc, command) => acc.concat(command), [])

    const batch = dbCommands
      .filter(entry => ALLOWED_TYPES.indexOf(entry.type) >= 0)
      .map(entry => Object.assign(entry, { prefix: this.state.toJSON() }))

    if (batch.length) {
      this.db.batch(batch, done)
    } else {
      timers.setImmediate(done)
    }
  }

  _getPersistBatch (state, done) {
    this._getPersistLog(state, (err, _batch) => {
      if (err) {
        done(err)
      } else {
        done(null, _batch.concat(this._getPersistMeta(state)))
      }
    })
  }

  _getPersistMeta (state) {
    const snapshot = state.snapshot()
    return [
      {
        key: 'currentTerm',
        value: snapshot.currentTerm,
        prefix: this.meta.toJSON()
      },
      {
        key: 'votedFor',
        value: snapshot.votedFor,
        prefix: this.meta.toJSON()
      }
    ]
  }

  _getPersistLog (state, _done) {
    //console.log('%s: persisting log', this.id, state.logEntries());
    const done = Once(_done)
    const entries = state.logEntries()
    const byKey = entries.reduce((acc, entry) => {
      const key = `${entry.t}:${entry.i}`
      acc[key] = entry.c
      return acc
    }, {})
    //console.log('%s: persisting log', this.id, state.logEntries());
    debug('%s: log by key: %j', this.id, byKey)
    const removeKeys = []
    this.log.createKeyStream()
      .on('data', key => {
        if (!byKey.hasOwnProperty(key)) {
          // remove key not present in the log any more
          console.log('remove key', this.id, key);
          removeKeys.push(key)
        } else {
          // remove entries already in the database
          console.log('delete entry by key', this.id, key);
          delete byKey[key]
        }
      })
      .once('error', done)
      .once('end', () => {
        debug('%s: will remove keys: %j', this.id, byKey)
        const operations =
          removeKeys.map(removeKey => {
            return {
              type: 'del',
              key: removeKey,
              prefix: this.log.toJSON()
            }
          })
          .concat(Object.keys(byKey).map(key => {
            return {
              type: 'put',
              key: key,
              value: byKey[key],
              prefix: this.log.toJSON()
            }
          }))

        done(null, operations)
      })
  }

  _commandToBatch (command) {
    return (Array.isArray(command) ? command : [command])
      .map(this._transformCommand.bind(this))
  }

  _transformCommand (command) {
    return Object.assign({}, command, { prefix: this.state.toJSON() })
  }

}

function fixLoadedEntry (entry) {
  console.log('fix loaded entry', entry)
  const keyParts = entry.key.split(':')
  const term = Number(keyParts[0])
  const index = Number(keyParts[1])
  return {
    i: index,
    t: term,
    c: entry.value
  }
}

module.exports = DB
