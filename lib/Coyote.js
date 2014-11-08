'use strict';

var _      = require('lodash');
var util   = require('util');
var rabbit = require('rabbit.js');
var FSM    = require('finite-spaghetti-machine');

// Argument and payload validation
var Joi    = require('joi');
var isuri  = require('isuri');
var assert = require('assert');

var es = require('event-stream');
var through = require('through');

var typeSchema = Joi.string()
  .allow('SUBSCRIBE', 'PULL', 'REPLY', 'WORKER')
  .insensitive()
  .strict();

var queueSchema = Joi.string().min(1).strict();

var optionsSchema = Joi.object().keys({
  topic      : Joi.string().optional(),
  expiration : Joi.number().min(0).optional(),
  prefetch   : Joi.number().min(0).optional(),
  persistent : Joi.boolean().optional(),
  routing    : Joi.string().optional()
});

var MODE_READ  = 1;
var MODE_WRITE = 2;
var MODE_ACK   = 4;

function Coyote(amqpUri, type, queue, options) {

  FSM.call(this, {
    INITIALIZING : {
      name : 'Initializing',
      enter : function() {
        this.initContext();
        this.transitionTo(this.STATE.CONNECTING);
      }
    },
    CONNECTING : {
      name : 'Connecting',
      enter : function() {
        this.connect();
      },
      exit : function() {
        this.setupListeners();
      },
      events : {
        socketConnect : function() {
          this.transitionTo(this.STATE.READY);
        }
      }
    },
    READY : {
      name : 'Ready',
      events : {
        pause : function() {
          this.transitionTo(this.STATE.PAUSED);
        },
        receiveJob : function(job) {
          this.transitionTo(this.STATE.WORKING);

          _.defer(function() {
            this.emit('job', job, this.onJobComplete.bind(this, job));
          }.bind(this));
        },
        shutdown : function() {
          this.transitionTo(this.state.SHUTDOWN);
        }
      }
    },
    WORKING : {
      name : 'Working',
      events : {
        pause : function() {
          this.transitionTo(this.STATE.PAUSING);
        },
        shutdown : function() {
          this.transitionTo(this.STATE.STOPPING);
        },
        completeJob : function() {
          this.transitionTo(this.STATE.READY);
        }
      }
    },
    PAUSING : {
      name : 'Pausing',
      events : {
        resume : function() {
          this.transitionTo(this.STATE.WORKING);
        },
        shutdown : function() {
          this.transitionTo(this.STATE.STOPPING);
        },
        completeJob : function() {
          this.transitionTo(this.STATE.PAUSED);
        }
      }
    },
    PAUSED : {
      name : 'Paused',
      enter : function() {
        this.socket.pause();
      },
      events : {
        resume : function() {
          this.socket.resume();
          this.transitionTo(this.STATE.READY);
        },
        shutdown : function() {
          this.transitionTo(this.STATE.SHUTDOWN);
        }
      }
    },
    STOPPING : {
      name : 'Stopping',
      events : {
        completeJob : function() {
          this.transitionTo(this.STATE.SHUTDOWN);
        }
      }
    },
    SHUTDOWN : {
      name : 'Shutdown',
      enter : function() {
        this.cleanupConnection();
      },
      events : {
        socketClose : function() {
          this.transitionTo(this.STATE.FINAL);
        }
      }
    },
    FINAL : {
      name : 'Final'
    }
  });

  assert(isuri.test(amqpUri), 'Provided AMQP URI is not valid');

  Joi.assert(type, typeSchema);
  Joi.assert(queue, queueSchema);
  Joi.assert(options, optionsSchema);

  this.amqpUri = amqpUri;
  this.type    = type.toUpperCase();
  this.queue   = queue;
  this.options = _.defaults(options || {}, {
    prefetch : 1
  });

  this.socket   = null;
  this.inbound  = null;
  this.outbound = null;

  this.mode = 0;
  switch (this.type) {
    case 'PUBLISH':
    case 'PUSH':
      this.mode = MODE_WRITE;
      break;
    case 'SUBSCRIBE':
    case 'PULL':
      this.mode = MODE_READ;
      break;
    case 'WORKER':
      this.mode = MODE_READ | MODE_ACK;
      break;
    case 'REQUEST':
    case 'REPLY':
      this.mode = MODE_READ | MODE_WRITE;
      break;
  }

  // Allow the caller to set up callbacks
  _.defer(function() {
    this.transitionTo(this.STATE.INITIALIZING);
  }.bind(this));
}

util.inherits(Coyote, FSM);

Coyote.prototype.initContext = function() {
  this.context = rabbit.createContext(this.amqpUri);
};

Coyote.prototype.connect = function() {
  this.emit('debug', 'Socket options', this.options);
  this.socket = this.context.socket(this.type, this.options);

  this.socket.connect(this.queue, function() {
    this.dispatchEvent('socketConnect');
  }.bind(this));
};

Coyote.prototype.setupListeners = function() {
  // Pipe the read stream into es.parse, which will run JSON.parse on each chunk
  // and save this stream as this.inbound so we can reference both it and the
  // original socket

  if (this.mode & MODE_READ) {
    this.inbound = this.socket.pipe(es.parse());

    this.inbound.on('data', function(payload) {
      this.dispatchEvent('receiveJob', payload);
    }.bind(this));
  }

  // Create a write stream that coverts its chunks into a utf8 buffer of a JSON
  // string. Pipe it back into the socket for writes

  if (this.mode & MODE_WRITE) {
    this.outbound = through(function write(data) {
      this.emit('data', new Buffer(JSON.stringify(data), 'utf8'));
    });

    this.outbound.pipe(this.socket);
  }
};

Coyote.prototype.setHandler = function(handler) {
  this.on('job', function(payload, callback) {
    var result = handler(payload, callback);

    if (_.isObject(result) && _.isFunction(result.then) && _.isFunction(result.catch)) {
      result.bind(this)
        .then(function(response) {
          this.onJobComplete(payload, null, response);
        })
        .catch(function(error) {
          this.onJobComplete(payload, error);
        });
    }
  }.bind(this));
};

Coyote.prototype.onJobComplete = function(job, error, response) {
  if (error) {
    this.emit('debug', 'Job failed', job, error);
    this.emit('jobFailure', job, error);

    if (this.type === 'WORKER') {
      this.socket.discard();
    } else if (this.type === 'REPLY') {
      this.write({ error : error.message });
    }
  } else {
    this.write(response);
  }

  this.dispatchEvent('completeJob');
};

Coyote.prototype.write = function(response) {
  if (this.mode & MODE_WRITE) {
    this.emit('debug', 'Writing response', response);
    this.outbound.write(response);
  } else if (this.mode & MODE_ACK) {
    this.emit('debug', 'ACKing job');
    this.socket.ack();
  }
};

Coyote.prototype.pause = function() {
  this.dispatchEvent('pause');
};

Coyote.prototype.resume = function() {
  this.dispatchEvent('resume');
};

Coyote.prototype.cleanupConnection = function() {
  this.emit('debug', 'Closing socket');

  this.socket.on('close', function() {
    this.dispatchEvent('socketClose');
  }.bind(this));

  this.socket.close();
};

module.exports = Coyote;
