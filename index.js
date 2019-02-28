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

  let channelIterable = new AGChannel(
    channelName,
    this,
    this._channelEventDemux,
    this._channelDataDemux
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

  let channelIterable = new AGChannel(
    channelName,
    this,
    this._channelEventDemux,
    this._channelDataDemux
  );

  return channelIterable;
};

SimpleExchange.prototype.closeChannelData = function (channelName) {
  this._channelDataDemux.close(channelName);
};

SimpleExchange.prototype.closeChannelListener = function (channelName, eventName) {
  this._channelEventDemux.close(`${channelName}/${eventName}`);
};

SimpleExchange.prototype.closeAllChannelListeners = function (channelName) {
  this.closeChannelListener(channelName, 'kickOut');
  this.closeChannelListener(channelName, 'subscribeStateChange');
  this.closeChannelListener(channelName, 'subscribe');
  this.closeChannelListener(channelName, 'subscribeFail');
  this.closeChannelListener(channelName, 'unsubscribe');
};

SimpleExchange.prototype.closeChannel = function (channelName) {
  this.closeChannelData(channelName);
  this.closeAllChannelListeners(channelName);
};

SimpleExchange.prototype.closeAllChannelsData = function () {
  this._channelDataDemux.closeAll();
};

SimpleExchange.prototype.closeAllChannelsListeners = function () {
  this._channelEventDemux.closeAll();
};

SimpleExchange.prototype.closeAllChannels = function () {
  this.closeAllChannelsData();
  this.closeAllChannelsListeners();
};

SimpleExchange.prototype.killChannelData = function (channelName) {
  this._channelDataDemux.kill(channelName);
};

SimpleExchange.prototype.killChannelListener = function (channelName, eventName) {
  this._channelEventDemux.kill(`${channelName}/${eventName}`);
};

SimpleExchange.prototype.killAllChannelListeners = function (channelName) {
  this.killChannelListener(channelName, 'kickOut');
  this.killChannelListener(channelName, 'subscribeStateChange');
  this.killChannelListener(channelName, 'subscribe');
  this.killChannelListener(channelName, 'subscribeFail');
  this.killChannelListener(channelName, 'unsubscribe');
};

SimpleExchange.prototype.killChannel = function (channelName) {
  this.killChannelData(channelName);
  this.killAllChannelListeners(channelName);
};

SimpleExchange.prototype.killAllChannelsData = function () {
  this._channelDataDemux.killAll();
};

SimpleExchange.prototype.killAllChannelsListeners = function () {
  this._channelEventDemux.killAll();
};

SimpleExchange.prototype.killAllChannels = function () {
  this.killAllChannelsData();
  this.killAllChannelsListeners();
};

SimpleExchange.prototype.killChannelDataConsumer = function (consumerId) {
  this._channelDataDemux.killConsumer(consumerId);
};

SimpleExchange.prototype.killChannelListenerConsumer = function (consumerId) {
  this._channelEventDemux.killConsumer(consumerId);
};

SimpleExchange.prototype.getChannelDataConsumerStats = function (consumerId) {
  return this._channelDataDemux.getConsumerStats(consumerId);
};

SimpleExchange.prototype.getChannelListenerConsumerStats = function (consumerId) {
  return this._channelEventDemux.getConsumerStats(consumerId);
};

SimpleExchange.prototype.getChannelDataConsumerStatsList = function (channelName) {
  return this._channelDataDemux.getConsumerStatsList(channelName);
};

SimpleExchange.prototype.getChannelListenerConsumerStatsList = function (channelName, eventName) {
  return this._channelEventDemux.getConsumerStatsList(`${channelName}/${eventName}`);
};

SimpleExchange.prototype.getAllChannelListenerConsumerStatsList = function (channelName) {
  return this.getChannelListenerConsumerStatsList(channelName, 'kickOut').concat(
    this.getChannelListenerConsumerStatsList(channelName, 'subscribeStateChange'),
    this.getChannelListenerConsumerStatsList(channelName, 'subscribe'),
    this.getChannelListenerConsumerStatsList(channelName, 'subscribeFail'),
    this.getChannelListenerConsumerStatsList(channelName, 'unsubscribe')
  );
};

SimpleExchange.prototype.getAllChannelsDataConsumerStatsList = function () {
  return this._channelDataDemux.getConsumerStatsListAll();
};

SimpleExchange.prototype.getAllChannelsListenerConsumerStatsList = function () {
  return this._channelEventDemux.getConsumerStatsListAll();
};

SimpleExchange.prototype.getChannelDataBackpressure = function (channelName) {
  return this._channelDataDemux.getBackpressure(channelName);
};

SimpleExchange.prototype.getChannelListenerBackpressure = function (channelName, eventName) {
  return this._channelEventDemux.getBackpressure(`${channelName}/${eventName}`);
};

SimpleExchange.prototype.getAllChannelListenersBackpressure = function (channelName) {
  return Math.max(
    this.getChannelListenerBackpressure(channelName, 'kickOut'),
    this.getChannelListenerBackpressure(channelName, 'subscribeStateChange'),
    this.getChannelListenerBackpressure(channelName, 'subscribe'),
    this.getChannelListenerBackpressure(channelName, 'subscribeFail'),
    this.getChannelListenerBackpressure(channelName, 'unsubscribe')
  );
};

SimpleExchange.prototype.getChannelBackpressure = function (channelName) {
  return Math.max(
    this.getChannelDataBackpressure(channelName),
    this.getAllChannelListenersBackpressure(channelName),
  );
};

SimpleExchange.prototype.getAllChannelsDataBackpressure = function () {
  return this._channelDataDemux.getBackpressureAll();
};

SimpleExchange.prototype.getAllChannelsListenersBackpressure = function () {
  return this._channelEventDemux.getBackpressureAll();
};

SimpleExchange.prototype.getAllChannelsBackpressure = function () {
  return Math.max(
    this.getAllChannelsDataBackpressure(),
    this.getAllChannelsListenersBackpressure()
  );
};

SimpleExchange.prototype.getChannelDataConsumerBackpressure = function (consumerId) {
  return this._channelDataDemux.getConsumerBackpressure(consumerId);
};

SimpleExchange.prototype.getChannelListenerConsumerBackpressure = function (consumerId) {
  return this._channelEventDemux.getConsumerBackpressure(consumerId);
};

SimpleExchange.prototype.hasChannelDataConsumer = function (channelName, consumerId) {
  return this._channelDataDemux.hasConsumer(channelName, consumerId);
};

SimpleExchange.prototype.hasChannelListenerConsumer = function (channelName, eventName, consumerId) {
  return this._channelEventDemux.hasConsumer(`${channelName}/${eventName}`, consumerId);
};

SimpleExchange.prototype.hasAnyChannelListenerConsumer = function (channelName, consumerId) {
  return this.hasChannelListenerConsumer(channelName, 'kickOut', consumerId) ||
    this.hasChannelListenerConsumer(channelName, 'subscribeStateChange', consumerId) ||
    this.hasChannelListenerConsumer(channelName, 'subscribe', consumerId) ||
    this.hasChannelListenerConsumer(channelName, 'subscribeFail', consumerId) ||
    this.hasChannelListenerConsumer(channelName, 'unsubscribe', consumerId);
};

SimpleExchange.prototype.hasAnyChannelDataConsumer = function (consumerId) {
  return this._channelDataDemux.hasConsumerAll(consumerId);
};

SimpleExchange.prototype.hasAnyChannelsListenerConsumer = function (consumerId) {
  return this._channelEventDemux.hasConsumerAll(consumerId);
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
