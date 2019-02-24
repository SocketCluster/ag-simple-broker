const AsyncStreamEmitter = require('async-stream-emitter');
const StreamDemux = require('stream-demux');
let AGChannel = require('ag-channel');

function SimpleExchange(broker) {
  AsyncStreamEmitter.call(this);

  this._broker = broker;
  this._channelMap = {};
  this._channelEventDemux = new StreamDemux();
  this._channelDataDemux = new StreamDemux();

  (async () => {
    for await (let {channel, data} of this._broker.listener('publish')) {
      this._channelDataDemux.write(channel, data);
    }
  })();
}

SimpleExchange.prototype = Object.create(AsyncStreamEmitter.prototype);

SimpleExchange.prototype.getBackpressure = function () {
  return Math.max(
    this.getAllListenersBackpressure(),
    this.getAllChannelsBackpressure()
  );
};

SimpleExchange.prototype.destroy = function () {
  this._broker.closeAllListeners();
};

SimpleExchange.prototype._triggerChannelSubscribe = function (channel) {
  let channelName = channel.name;

  channel.state = AGChannel.SUBSCRIBED;

  this._channelEventDemux.write(`${channelName}/subscribe`, {});
  this.emit('subscribe', {channel: channelName});
};

SimpleExchange.prototype._triggerChannelUnsubscribe = function (channel) {
  let channelName = channel.name;

  delete this._channelMap[channelName];
  if (channel.state === AGChannel.SUBSCRIBED) {
    this._channelEventDemux.write(`${channelName}/unsubscribe`, {});
    this.emit('unsubscribe', {channel: channelName});
  }
};

SimpleExchange.prototype.transmitPublish = async function (channelName, data) {
  return this._broker.transmitPublish(channelName, data);
};

SimpleExchange.prototype.invokePublish = async function (channelName, data) {
  return this._broker.invokePublish(channelName, data);
};

SimpleExchange.prototype.subscribe = function (channelName) {
  let channel = this._channelMap[channelName];

  if (!channel) {
    channel = {
      name: channelName,
      state: AGChannel.PENDING
    };
    this._channelMap[channelName] = channel;
    this._triggerChannelSubscribe(channel);
  }

  let channelDataStream = this._channelDataDemux.stream(channelName);
  let channelIterable = new AGChannel(
    channelName,
    this,
    this._channelEventDemux,
    channelDataStream
  );

  return channelIterable;
};

SimpleExchange.prototype.unsubscribe = async function (channelName) {
  let channel = this._channelMap[channelName];

  if (channel) {
    this._triggerChannelUnsubscribe(channel);
  }
};

SimpleExchange.prototype.channel = function (channelName) {
  let currentChannel = this._channelMap[channelName];

  let channelDataStream = this._channelDataDemux.stream(channelName);
  let channelIterable = new AGChannel(
    channelName,
    this,
    this._channelEventDemux,
    channelDataStream
  );

  return channelIterable;
};

SimpleExchange.prototype.closeChannel = function (channelName) {
  this._channelDataDemux.close(channelName);
};

SimpleExchange.prototype.closeAllChannels = function () {
  this._channelDataDemux.closeAll();
};

SimpleExchange.prototype.killChannel = function (channelName) {
  this._channelDataDemux.kill(channelName);
};

SimpleExchange.prototype.killAllChannels = function () {
  this._channelDataDemux.killAll();
};

SimpleExchange.prototype.killChannelConsumer = function (consumerId) {
  this._channelDataDemux.killConsumer(consumerId);
};

SimpleExchange.prototype.getChannelConsumerStats = function (consumerId) {
  return this._channelDataDemux.getConsumerStats(consumerId);
};

SimpleExchange.prototype.getChannelConsumerStatsList = function (channelName) {
  return this._channelDataDemux.getConsumerStatsList(channelName);
};

SimpleExchange.prototype.getAllChannelsConsumerStatsList = function () {
  return this._channelDataDemux.getConsumerStatsListAll();
};

SimpleExchange.prototype.getChannelBackpressure = function (channelName) {
  return this._channelDataDemux.getBackpressure(channelName);
};

SimpleExchange.prototype.getAllChannelsBackpressure = function () {
  return this._channelDataDemux.getBackpressureAll();
};

SimpleExchange.prototype.getChannelConsumerBackpressure = function (consumerId) {
  return this._channelDataDemux.getConsumerBackpressure(consumerId);
};

SimpleExchange.prototype.hasChannelConsumer = function (channelName, consumerId) {
  return this._channelDataDemux.hasConsumer(channelName, consumerId);
};

SimpleExchange.prototype.hasAnyChannelConsumer = function (consumerId) {
  return this._channelDataDemux.hasConsumerAll(consumerId);
};

SimpleExchange.prototype.getChannelState = function (channelName) {
  let channel = this._channelMap[channelName];
  if (channel) {
    return channel.state;
  }
  return AGChannel.UNSUBSCRIBED;
};

SimpleExchange.prototype.getChannelOptions = function (channelName) {
  return {};
};

SimpleExchange.prototype.subscriptions = function (includePending) {
  let subs = [];
  Object.keys(this._channelMap).forEach((channelName) => {
    if (includePending || this._channelMap[channelName].state === AGChannel.SUBSCRIBED) {
      subs.push(channelName);
    }
  });
  return subs;
};

SimpleExchange.prototype.isSubscribed = function (channelName, includePending) {
  let channel = this._channelMap[channelName];
  if (includePending) {
    return !!channel;
  }
  return !!channel && channel.state === AGChannel.SUBSCRIBED;
};


function AGSimpleBroker() {
  AsyncStreamEmitter.call(this);

  this.isReady = false;
  this._codec = null;
  this._exchangeClient = new SimpleExchange(this);
  this._clientSubscribers = {};
  this._clientSubscribersCounter = {};

  setTimeout(() => {
    this.isReady = true;
    this.emit('ready', {});
  }, 0);
}

AGSimpleBroker.prototype = Object.create(AsyncStreamEmitter.prototype);

AGSimpleBroker.prototype.exchange = function () {
  return this._exchangeClient;
};

AGSimpleBroker.prototype.subscribeSocket = async function (socket, channelName) {
  if (!this._clientSubscribers[channelName]) {
    this._clientSubscribers[channelName] = {};
    this._clientSubscribersCounter[channelName] = 0;
  }
  if (!this._clientSubscribers[channelName][socket.id]) {
    this._clientSubscribersCounter[channelName]++;
    this.emit('subscribe', {
      channel: channelName
    });
  }
  this._clientSubscribers[channelName][socket.id] = socket;
};

AGSimpleBroker.prototype.unsubscribeSocket = async function (socket, channelName) {
  if (this._clientSubscribers[channelName]) {
    if (this._clientSubscribers[channelName][socket.id]) {
      this._clientSubscribersCounter[channelName]--;
      delete this._clientSubscribers[channelName][socket.id];

      if (this._clientSubscribersCounter[channelName] <= 0) {
        delete this._clientSubscribers[channelName];
        delete this._clientSubscribersCounter[channelName];
        this.emit('unsubscribe', {
          channel: channelName
        });
      }
    }
  }
};

AGSimpleBroker.prototype.subscriptions = function () {
  return Object.keys(this._clientSubscribers);
};

AGSimpleBroker.prototype.isSubscribed = function (channelName) {
  return !!this._clientSubscribers[channelName];
};

AGSimpleBroker.prototype.setCodecEngine = function (codec) {
  this._codec = codec;
};

// In this implementation of the broker engine, both invokePublish and transmitPublish
// methods are the same. In alternative implementations, they could be different.
AGSimpleBroker.prototype.invokePublish = async function (channelName, data, suppressEvent) {
  return this.transmitPublish(channelName, data, suppressEvent);
};

AGSimpleBroker.prototype.transmitPublish = async function (channelName, data, suppressEvent) {
  let packet = {
    channel: channelName,
    data
  };
  let transmitOptions = {};

  if (this._codec) {
    // Optimization
    try {
      transmitOptions.stringifiedData = this._codec.encode({
        event: '#publish',
        data: packet
      });
    } catch (error) {
      this.emit('error', {error});
      return;
    }
  }

  let subscriberSockets = this._clientSubscribers[channelName] || {};

  Object.keys(subscriberSockets).forEach((i) => {
    subscriberSockets[i].transmit('#publish', packet, transmitOptions);
  });

  if (!suppressEvent) {
    this.emit('publish', packet);
  }
};

module.exports = AGSimpleBroker;
