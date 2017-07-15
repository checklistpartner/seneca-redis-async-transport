/* Copyright (c) 2014-2015 Richard Rodger, MIT License */
'use strict'

var _ = require('lodash')
var Redis = require('redis')

module.exports = function (options) {
  var seneca = this
  var plugin = 'redis-async-transport'

  var so = seneca.options()

  options = seneca.util.deepextend(
    {
      redis: {
        timeout: so.timeout ? so.timeout - 555 : 22222,
        type: 'redis-async',
        host: 'localhost',
        port: 6379
      }
    },
    so.transport,
    options)

  var tu = seneca.export('transport/utils')

  seneca.add({role: 'transport', hook: 'listen', type: 'redis-async'}, hook_listen_redis)
  seneca.add({role: 'transport', hook: 'client', type: 'redis-async'}, hook_client_redis)

  function hook_listen_redis (args, done) {
    var seneca = this
    var type = args.type
    var listen_options = seneca.util.clean(_.extend({}, options[type], args))

    var redis_in = Redis.createClient(listen_options.port, listen_options.host,  listen_options.redisOptions || {})

    handle_events(redis_in)

	  redis_in.on('pmessage',function(pattern, channel,msgstr){
		  var data     = tu.parseJSON( seneca, 'listen-', msgstr )
		  data.act.redisAct$ = true;
		  data.act.default$ = true;
      tu.handle_request(seneca, data, listen_options, function (out) {})
    })

    listen_topics(seneca, args, listen_options, function (topic) {
      seneca.log.debug({kind:'transport', transport:'redis-async', message:`listen - subscribe - ${topic}_act`, listen_options:_.omit(listen_options, 'password')})
      redis_in.psubscribe(topic + '_act')
    })

    seneca.add('role:seneca,cmd:close', function (close_args, done) {
      var closer = this

      redis_in.quit()
      closer.prior(close_args, done)
    })

    seneca.log.info({kind:'transport', transport:'redis-async', message:'listen - open', listen_options})

    done()
  }

	function listen_topics( seneca, args, listen_options, do_topic ) {
		var msgprefix = (null == options.msgprefix ? '' : options.msgprefix)
		var pins      = tu.resolve_pins( args )

		if( pins ) {
			_.each( pins, function(pin) {

				var foundPins;
				if(JSON.stringify(pin).indexOf('*') != -1){
					foundPins = [pin];
				}
				else {
					foundPins = seneca.findpins(pin);
				}

				_.each(foundPins, function (foundPin) {
					var sb = []
					_.each( _.keys(foundPin).sort(), function(k){
						sb.push(k)
						sb.push('=')
						sb.push(foundPin[k])
						sb.push(',')
					})

					var topic = msgprefix+(sb.join('')).replace(/[^\w\d\*]+/g,'_')
					do_topic( topic )
				});
			})
		}
		else {
			do_topic( msgprefix+'any' )
		}
	}

  function hook_client_redis (args, clientdone) {
    var seneca = this
    var type = args.type
    var client_options = seneca.util.clean(_.extend({}, options[type], args))

    tu.make_client(make_send, client_options, clientdone)

    function make_send (spec, topic, send_done) {
      var redis_out = Redis.createClient(client_options.port, client_options.host, client_options.redisOptions || {})

      handle_events(redis_out)

      send_done(null, function (args, done) {
	
	      _.set(args, 'meta$.sync', false)
        var outmsg = tu.prepare_request(this, args, done)
        var outstr = tu.stringifyJSON(seneca, 'client-' + type, outmsg)

        redis_out.publish(topic + '_act', outstr)
      })

      seneca.add('role:seneca,cmd:close', function (close_args, done) {
        var closer = this

        redis_out.quit()
        closer.prior(close_args, done)
      })
    }
  }

  function handle_events (redisclient) {
    // Die if you can't connect initially
    redisclient.on('ready', function () {
      redisclient.on('error', function (err) {
        seneca.log.error({kind:'transport', transport:'redis-async', error:err})
      })
    })
  }

  return {
    name: plugin
  }
}
