# Coyote

An abstraction layer on top of [RabbitJS](https://github.com/squaremo/rabbit.js)
designed to simplify your workflow and reduce boilerplate.

### IO

#### Types

`SUBSCRIBE`, `PULL`, `REPLY`, `WORKER`

#### Signals

`ready`, `job`, `response`, `data`, `jobFailure`

### Example

#### Callback API

```javascript
var Coyote = require('coyote');

var coyote = new Coyote('amqp://localhost', 'REPLY', 'rabbits');

coyote.on('job', function(rabbit, callback) {
  eat(rabbit, function() {
    callback();
  });
})''
```

#### Promise API

```javascript
var Coyote = require('coyote');

var coyote = new Coyote('amqp://localhost', 'REPLY', 'rabbits');

coyote.setHandler(function handleJob(rabbit) {
  return consume(rabbit)
    .then(function() {
      return 'Yum!';
    })
    .catch(RabbitTooFastError, function() {
      return 'It got away :(';
    });
});
```
