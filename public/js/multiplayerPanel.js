var multiplayer = new function() {
  var self = this;

  this.role = 'none';
  this.sessionId = null;
  this.localOffer = null;
  this.remoteOffer = null;
  this.localAnswer = null;
  this.remoteAnswer = null;
  this.pc = null;
  this.dc = null;
  this.connected = false;
  this.iceState = 'new';
  this.dcState = 'closed';
  this.lastHeartbeatAt = 0;
  this.seq = 0;
  this.lastRemoteSeq = 0;
  this.remoteConfigHash = null;
  this.pendingAnswerPayload = null;
  this.snapshotTimer = null;
  this.deltaTimer = null;
  this.heartbeatTimer = null;
  this.messageWindowStart = 0;
  this.messageCount = 0;
  this.maxMessageSize = 60000; // Keep payloads below conservative ordered data channel limits (~64KB in Firefox).
  this.maxMessagesPerSecond = 80;
  this.debug = false;
  this.statusMessage = 'Idle';
  this.statusIsError = false;
  this.useStun = false;
  this.syncEnabled = false;

  this.stateSnapshotIntervalMs = 1000;
  this.stateDeltaIntervalMs = 200;
  this.heartbeatIntervalMs = 5000;
  this.stunServers = [
    {urls: 'stun:stun.l.google.com:19302'}
  ];

  this.init = function() {
    self.useStun = localStorage.getItem('mpUseStun') == 'true';
    self.role = localStorage.getItem('mpRole') || 'host';
    self.sessionId = localStorage.getItem('mpSessionId') || self.generateSessionId();
    self.persistSession();
    self.emitStatus('Idle');
    self.autoJoinFromHash();
  };

  this.persistSession = function() {
    localStorage.setItem('mpRole', self.role);
    localStorage.setItem('mpSessionId', self.sessionId);
    localStorage.setItem('mpUseStun', self.useStun);
  };

  this.generateSessionId = function() {
    return Math.random().toString(36).substring(2, 8) + '-' + Date.now().toString(36).slice(-4);
  };

  this.setRole = function(role) {
    self.role = role || 'none';
    self.persistSession();
    self.emitStatus(self.connected ? 'Connected' : 'Idle');
  };

  this.setUseStun = function(state) {
    self.useStun = !!state;
    self.persistSession();
  };

  this.isHost = function() {
    return self.role == 'host';
  };

  this.isGuest = function() {
    return self.role == 'guest';
  };

  this.isGuestConnected = function() {
    return self.isGuest() && self.connected;
  };

  this.shouldBlockLocalControl = function() {
    return self.isGuestConnected();
  };

  this.emitStatus = function(message, isError=false) {
    self.statusMessage = message;
    self.statusIsError = isError;
    if (typeof multiplayerPanel != 'undefined' && typeof multiplayerPanel.updateStatus == 'function') {
      multiplayerPanel.updateStatus();
    }
  };

  this.resetSession = function() {
    self.disconnect();
    self.sessionId = self.generateSessionId();
    self.persistSession();
    self.emitStatus('Session reset');
  };

  this.disconnect = function() {
    self.stopSync();
    self.stopHeartbeat();
    self.connected = false;
    self.iceState = 'new';
    self.dcState = 'closed';
    self.lastRemoteSeq = 0;
    self.pendingAnswerPayload = null;
    self.remoteConfigHash = null;
    if (self.dc) {
      try { self.dc.close(); } catch (err) {}
    }
    if (self.pc) {
      try { self.pc.close(); } catch (err) {}
    }
    self.dc = null;
    self.pc = null;
    self.emitStatus('Disconnected');
  };

  this.createPeerConnection = function() {
    self.pc = new RTCPeerConnection({
      iceServers: self.useStun ? self.stunServers : []
    });

    self.pc.oniceconnectionstatechange = function() {
      self.iceState = self.pc.iceConnectionState;
      self.emitStatus(self.connected ? 'Connected' : 'Connecting');
    };

    self.pc.onconnectionstatechange = function() {
      self.emitStatus(self.pc.connectionState);
    };

    self.pc.ondatachannel = function(event) {
      self.registerDataChannel(event.channel);
    };
  };

  this.registerDataChannel = function(channel) {
    self.dc = channel;
    self.dcState = channel.readyState;
    self.dc.onopen = function() {
      self.connected = true;
      self.dcState = 'open';
      self.emitStatus('Connected');
      self.startHeartbeat();
      if (self.isHost()) {
        self.sendHello();
        if (self.syncEnabled) {
          self.startSync();
        }
      } else {
        self.sendMessage('ready', {});
      }
    };
    self.dc.onclose = function() {
      self.dcState = 'closed';
      self.connected = false;
      self.emitStatus('Channel closed');
      self.stopSync();
      self.stopHeartbeat();
    };
    self.dc.onerror = function() {
      self.emitStatus('Channel error', true);
    };
    self.dc.onmessage = function(event) {
      if (typeof event.data !== 'string') {
        return;
      }
      if (event.data.length > self.maxMessageSize) {
        self.emitStatus('Dropped oversized message', true);
        return;
      }
      self.handleMessage(event.data);
    };
  };

  this.waitForIceGatheringComplete = function(pc) {
    if (pc.iceGatheringState == 'complete') {
      return Promise.resolve();
    }
    return new Promise(function(resolve) {
      function checkState() {
        if (pc.iceGatheringState == 'complete') {
          pc.removeEventListener('icegatheringstatechange', checkState);
          resolve();
        }
      }
      pc.addEventListener('icegatheringstatechange', checkState);
    });
  };

  this.encodePayload = function(payload) {
    var json = JSON.stringify(payload);
    var encoder = new TextEncoder();
    var bytes = encoder.encode(json);
    var binary = '';
    bytes.forEach(function(byte) {
      binary += String.fromCharCode(byte);
    });
    var base = btoa(binary);
    return base.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  };

  this.decodePayload = function(payload) {
    var base = payload.replace(/-/g, '+').replace(/_/g, '/');
    while (base.length % 4) {
      base += '=';
    }
    var binary = atob(base);
    var bytes = new Uint8Array(binary.length);
    for (var i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    var decoder = new TextDecoder();
    return JSON.parse(decoder.decode(bytes));
  };

  this.extractPayload = function(text) {
    if (!text) {
      return null;
    }
    var trimmed = text.trim();
    var hashIndex = trimmed.indexOf('#mp=');
    if (hashIndex >= 0) {
      trimmed = trimmed.substring(hashIndex + 4);
    }
    if (trimmed.indexOf('mp=') >= 0) {
      var parts = trimmed.split('mp=');
      trimmed = parts[parts.length - 1];
    }
    trimmed = trimmed.replace(/^#/, '');
    if (trimmed == '') {
      return null;
    }
    return self.decodePayload(trimmed);
  };

  this.buildLink = function(payload) {
    var encoded = self.encodePayload(payload);
    return window.location.origin + window.location.pathname + '#mp=' + encoded;
  };

  this.createHostLink = async function() {
    self.disconnect();
    self.setRole('host');
    self.createPeerConnection();
    self.registerDataChannel(self.pc.createDataChannel('gears'));
    var offer = await self.pc.createOffer();
    await self.pc.setLocalDescription(offer);
    await self.waitForIceGatheringComplete(self.pc);
    self.localOffer = self.pc.localDescription.sdp;
    if (self.pendingAnswerPayload) {
      await self.acceptAnswer(self.pendingAnswerPayload);
      self.pendingAnswerPayload = null;
    }
    self.emitStatus('Offer created');
    return self.buildLink({
      v: 1,
      type: 'offer',
      sessionId: self.sessionId,
      role: 'guest',
      sdp: self.localOffer
    });
  };

  this.joinFromOffer = async function(payload) {
    self.disconnect();
    self.setRole('guest');
    self.sessionId = payload.sessionId || self.sessionId;
    self.persistSession();
    self.createPeerConnection();
    self.remoteOffer = payload.sdp;
    await self.pc.setRemoteDescription({type: 'offer', sdp: payload.sdp});
    var answer = await self.pc.createAnswer();
    await self.pc.setLocalDescription(answer);
    await self.waitForIceGatheringComplete(self.pc);
    self.localAnswer = self.pc.localDescription.sdp;
    self.emitStatus('Answer created');
    return self.buildLink({
      v: 1,
      type: 'answer',
      sessionId: self.sessionId,
      role: 'host',
      sdp: self.localAnswer
    });
  };

  this.acceptAnswer = async function(payload) {
    if (!self.pc) {
      self.pendingAnswerPayload = payload;
      self.emitStatus('Create host link first', true);
      return;
    }
    if (payload.sessionId != self.sessionId) {
      self.emitStatus('Session mismatch', true);
      return;
    }
    self.remoteAnswer = payload.sdp;
    await self.pc.setRemoteDescription({type: 'answer', sdp: payload.sdp});
    self.emitStatus('Answer applied');
  };

  this.applyPayload = async function(payload) {
    if (!payload || !payload.type) {
      self.emitStatus('Invalid payload', true);
      return null;
    }
    if (payload.type == 'offer') {
      return await self.joinFromOffer(payload);
    } else if (payload.type == 'answer') {
      await self.acceptAnswer(payload);
      return null;
    } else {
      self.emitStatus('Unknown payload type', true);
      return null;
    }
  };

  this.autoJoinFromHash = function() {
    var hash = window.location.hash;
    if (!hash || hash.indexOf('mp=') < 0) {
      return;
    }
    try {
      var payload = self.extractPayload(hash);
      if (payload && payload.type == 'offer') {
        self.applyPayload(payload)
          .then(function(link) {
            if (link && typeof multiplayerPanel != 'undefined') {
              multiplayerPanel.setLink(link);
              toastMsg('Offer detected. Share the answer link with the host.');
            }
          });
      } else if (payload && payload.type == 'answer') {
        self.pendingAnswerPayload = payload;
        if (typeof multiplayerPanel != 'undefined') {
          multiplayerPanel.setLink(window.location.href);
        }
        self.emitStatus('Answer detected. Click Apply after creating host link.', true);
      }
    } catch (err) {
      self.emitStatus('Failed to parse link', true);
    }
  };

  this.sendMessage = function(type, data) {
    if (!self.dc || self.dc.readyState != 'open') {
      return;
    }
    var msg = {
      type: type,
      data: data || {},
      sessionId: self.sessionId,
      seq: ++self.seq,
      ts: Date.now()
    };
    var text = JSON.stringify(msg);
    if (text.length > self.maxMessageSize) {
      self.emitStatus('Outbound message too large', true);
      return;
    }
    self.dc.send(text);
  };

  this.handleMessage = function(raw) {
    var now = Date.now();
    if (now - self.messageWindowStart > 1000) {
      self.messageWindowStart = now;
      self.messageCount = 0;
    }
    self.messageCount += 1;
    if (self.messageCount > self.maxMessagesPerSecond) {
      self.emitStatus('Rate limit exceeded', true);
      self.disconnect();
      return;
    }

    var msg = null;
    try {
      msg = JSON.parse(raw);
    } catch (err) {
      self.emitStatus('Bad message format', true);
      return;
    }
    if (!msg || typeof msg.type != 'string') {
      return;
    }
    if (msg.sessionId != self.sessionId) {
      return;
    }
    if (typeof msg.seq != 'number' || msg.seq <= self.lastRemoteSeq) {
      if (self.debug) {
        console.warn('Dropping out-of-order message', msg.seq, self.lastRemoteSeq);
      }
      return;
    }
    self.lastRemoteSeq = msg.seq;
    self.lastHeartbeatAt = now;

    if (msg.type == 'hello') {
      if (self.isGuest()) {
        self.applyRemoteConfig(msg.data);
      }
    } else if (msg.type == 'ready') {
      if (self.isHost()) {
        self.sendHello();
      }
    } else if (msg.type == 'start') {
      if (self.isGuest()) {
        simPanel.setRunIcon('stop');
      }
    } else if (msg.type == 'pause') {
      if (self.isGuest()) {
        simPanel.setRunIcon('run');
      }
    } else if (msg.type == 'reset') {
      if (self.isGuest()) {
        simPanel.resetSim(true);
        self.disablePhysics();
      }
    } else if (msg.type == 'stateSnapshot') {
      if (self.isGuest()) {
        self.applyRemoteConfig(msg.data);
        self.applyRemoteState(msg.data.state);
      }
    } else if (msg.type == 'stateDelta') {
      if (self.isGuest()) {
        self.applyRemoteState(msg.data.state);
      }
    } else if (msg.type == 'ping') {
      if (self.isGuest()) {
        self.sendMessage('pong', {});
      }
    } else if (msg.type == 'pong') {
      // heartbeat ack
    } else if (msg.type == 'error') {
      self.emitStatus(msg.data && msg.data.message ? msg.data.message : 'Remote error', true);
    }
  };

  this.sendHello = function() {
    var config = self.buildConfigPayload();
    self.sendMessage('hello', config);
  };

  this.buildConfigPayload = function() {
    return {
      worldName: babylon.world.name,
      worldOptions: JSON.parse(JSON.stringify(babylon.world.options || {})),
      robotOptions: JSON.parse(JSON.stringify(robot.options || {})),
      running: skulpt.running
    };
  };

  this.applyRemoteConfig = function(config) {
    if (!config) {
      return;
    }
    var hash = JSON.stringify({
      worldName: config.worldName,
      worldOptions: config.worldOptions,
      robotOptions: config.robotOptions
    });
    if (hash == self.remoteConfigHash) {
      if (typeof config.running == 'boolean') {
        simPanel.setRunIcon(config.running ? 'stop' : 'run');
      }
      return;
    }
    self.remoteConfigHash = hash;
    if (config.worldName) {
      var worldMatch = worlds.find(world => world.name == config.worldName);
      if (worldMatch) {
        babylon.world = worldMatch;
      }
    }
    if (config.robotOptions) {
      robot.options = JSON.parse(JSON.stringify(config.robotOptions));
    }
    if (config.worldOptions) {
      babylon.world.setOptions(config.worldOptions).then(function() {
        babylon.resetScene();
        self.disablePhysics();
      });
    }
    if (typeof config.running == 'boolean') {
      simPanel.setRunIcon(config.running ? 'stop' : 'run');
    }
  };

  this.disablePhysics = function() {
    if (babylon.scene && typeof babylon.scene.disablePhysicsEngine == 'function') {
      babylon.scene.disablePhysicsEngine();
    }
  };

  this.buildTransform = function(mesh) {
    if (!mesh) {
      return null;
    }
    var transform = {
      p: [mesh.position.x, mesh.position.y, mesh.position.z]
    };
    if (mesh.rotationQuaternion) {
      transform.rq = [
        mesh.rotationQuaternion.x,
        mesh.rotationQuaternion.y,
        mesh.rotationQuaternion.z,
        mesh.rotationQuaternion.w
      ];
    } else {
      transform.r = [mesh.rotation.x, mesh.rotation.y, mesh.rotation.z];
    }
    return transform;
  };

  this.applyTransform = function(mesh, transform) {
    if (!mesh || !transform) {
      return;
    }
    if (transform.p) {
      mesh.position.x = transform.p[0];
      mesh.position.y = transform.p[1];
      mesh.position.z = transform.p[2];
    }
    if (transform.rq) {
      mesh.rotationQuaternion = new BABYLON.Quaternion(
        transform.rq[0],
        transform.rq[1],
        transform.rq[2],
        transform.rq[3]
      );
    } else if (transform.r) {
      mesh.rotation = new BABYLON.Vector3(transform.r[0], transform.r[1], transform.r[2]);
    }
  };

  this.buildStatePayload = function() {
    return {
      body: self.buildTransform(robot.body),
      leftWheel: {
        mesh: self.buildTransform(robot.leftWheel.mesh),
        position: robot.leftWheel.position
      },
      rightWheel: {
        mesh: self.buildTransform(robot.rightWheel.mesh),
        position: robot.rightWheel.position
      }
    };
  };

  this.applyWheelState = function(wheel, state) {
    if (!wheel || !state) {
      return;
    }
    if (state.mesh) {
      self.applyTransform(wheel.mesh, state.mesh);
    }
    if (typeof state.position == 'number') {
      wheel.position = state.position;
      wheel.actualPosition = state.position;
      wheel.prevPosition = state.position;
      wheel.speed = 0;
      wheel._speed_sp = 0;
      if (wheel.modes) {
        wheel.mode = wheel.modes.STOP;
      }
    }
  };

  this.applyRemoteState = function(state) {
    if (!state || !robot || !robot.body) {
      return;
    }
    if (state.body) {
      self.applyTransform(robot.body, state.body);
      if (robot.body.physicsImpostor) {
        robot.body.physicsImpostor.setLinearVelocity(new BABYLON.Vector3(0, 0, 0));
        robot.body.physicsImpostor.setAngularVelocity(new BABYLON.Vector3(0, 0, 0));
      }
    }
    if (state.leftWheel) {
      self.applyWheelState(robot.leftWheel, state.leftWheel);
    }
    if (state.rightWheel) {
      self.applyWheelState(robot.rightWheel, state.rightWheel);
    }
  };

  this.startSync = function() {
    if (!self.isHost()) {
      return;
    }
    self.syncEnabled = true;
    if (self.snapshotTimer) {
      return;
    }
    self.snapshotTimer = setInterval(function() {
      self.sendMessage('stateSnapshot', {
        state: self.buildStatePayload(),
        worldName: babylon.world.name,
        worldOptions: JSON.parse(JSON.stringify(babylon.world.options || {})),
        robotOptions: JSON.parse(JSON.stringify(robot.options || {}))
      });
    }, self.stateSnapshotIntervalMs);
    self.deltaTimer = setInterval(function() {
      self.sendMessage('stateDelta', {
        state: self.buildStatePayload()
      });
    }, self.stateDeltaIntervalMs);
    self.emitStatus('Syncing');
  };

  this.stopSync = function() {
    self.syncEnabled = false;
    if (self.snapshotTimer) {
      clearInterval(self.snapshotTimer);
      self.snapshotTimer = null;
    }
    if (self.deltaTimer) {
      clearInterval(self.deltaTimer);
      self.deltaTimer = null;
    }
  };

  this.startHeartbeat = function() {
    if (self.heartbeatTimer) {
      return;
    }
    self.heartbeatTimer = setInterval(function() {
      if (self.isHost()) {
        self.sendMessage('ping', {});
      }
    }, self.heartbeatIntervalMs);
  };

  this.stopHeartbeat = function() {
    if (self.heartbeatTimer) {
      clearInterval(self.heartbeatTimer);
      self.heartbeatTimer = null;
    }
  };

  this.onLocalRunToggled = function(isRunning) {
    if (!self.isHost() || !self.connected) {
      return;
    }
    if (isRunning) {
      self.sendMessage('start', {});
    } else {
      self.sendMessage('pause', {});
    }
  };

  this.onLocalReset = function() {
    if (!self.isHost() || !self.connected) {
      return;
    }
    self.sendMessage('reset', {});
  };
}

var multiplayerPanel = new function() {
  var self = this;

  this.init = function() {
    self.$role = $('#mpRole');
    self.$sessionId = $('#mpSessionId');
    self.$newSession = $('.mpNewSession');
    self.$useStun = $('#mpUseStun');
    self.$createLink = $('.mpCreateLink');
    self.$generateAnswer = $('.mpGenerateAnswer');
    self.$link = $('#mpLink');
    self.$copyLink = $('.mpCopyLink');
    self.$applyLink = $('.mpApplyLink');
    self.$status = $('#mpStatus');
    self.$roleBadge = $('#mpRoleBadge');
    self.$iceState = $('#mpIceState');
    self.$channelState = $('#mpChannelState');
    self.$heartbeat = $('#mpHeartbeat');
    self.$startSync = $('.mpStartSync');
    self.$resetSession = $('.mpResetSession');
    self.$disconnect = $('.mpDisconnect');

    self.$role.val(multiplayer.role);
    self.$sessionId.val(multiplayer.sessionId);
    self.$useStun.prop('checked', multiplayer.useStun);

    self.$role.change(function() {
      multiplayer.setRole(self.$role.val());
      self.updateStatus();
    });
    self.$newSession.click(self.newSession);
    self.$useStun.change(function() {
      multiplayer.setUseStun(self.$useStun.prop('checked'));
    });
    self.$createLink.click(self.createHostLink);
    self.$generateAnswer.click(self.generateAnswerLink);
    self.$applyLink.click(self.applyLink);
    self.$copyLink.click(self.copyLink);
    self.$startSync.click(self.startSync);
    self.$resetSession.click(self.resetSession);
    self.$disconnect.click(self.disconnect);

    self.updateStatus();
  };

  this.onActive = function() {
    self.updateStatus();
  };

  this.setLink = function(value) {
    self.$link.val(value || '');
  };

  this.updateStatus = function() {
    self.$status.text(multiplayer.statusMessage || 'Idle');
    self.$status.toggleClass('error', multiplayer.statusIsError);
    self.$roleBadge.text('Role: ' + (multiplayer.role || 'none'));
    self.$iceState.text(multiplayer.iceState || 'new');
    self.$channelState.text(multiplayer.dcState || 'closed');
    if (multiplayer.lastHeartbeatAt) {
      var seconds = Math.round((Date.now() - multiplayer.lastHeartbeatAt) / 1000);
      self.$heartbeat.text(seconds + 's ago');
    } else {
      self.$heartbeat.text('-');
    }
    self.$sessionId.val(multiplayer.sessionId || '');
    self.$role.val(multiplayer.role);
    self.$useStun.prop('checked', multiplayer.useStun);
  };

  this.createHostLink = function() {
    multiplayer.createHostLink()
      .then(function(link) {
        self.$link.val(link);
        toastMsg('Share this link with the guest.');
        self.updateStatus();
      })
      .catch(function() {
        toastMsg('Failed to create host link.');
        self.updateStatus();
      });
  };

  this.generateAnswerLink = function() {
    var input = self.$link.val();
    var payload = null;
    try {
      payload = multiplayer.extractPayload(input);
    } catch (err) {
      toastMsg('Invalid offer payload.');
      return;
    }
    if (!payload || payload.type != 'offer') {
      toastMsg('Paste a host offer link first.');
      return;
    }
    multiplayer.applyPayload(payload)
      .then(function(link) {
        if (link) {
          self.$link.val(link);
          toastMsg('Share this answer link with the host.');
        }
        self.updateStatus();
      })
      .catch(function() {
        toastMsg('Failed to generate answer link.');
        self.updateStatus();
      });
  };

  this.applyLink = function() {
    var input = self.$link.val();
    if (!input) {
      return;
    }
    var payload = null;
    try {
      payload = multiplayer.extractPayload(input);
    } catch (err) {
      toastMsg('Invalid link or payload.');
      return;
    }
    multiplayer.applyPayload(payload)
      .then(function(link) {
        if (link) {
          self.$link.val(link);
        }
        self.updateStatus();
      })
      .catch(function() {
        toastMsg('Failed to apply link.');
        self.updateStatus();
      });
  };

  this.copyLink = function() {
    var value = self.$link.val();
    if (!value) {
      return;
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(value);
      toastMsg('Copied to clipboard.');
    } else {
      toastMsg('Clipboard API not available in this browser.');
    }
  };

  this.newSession = function() {
    multiplayer.resetSession();
    self.$sessionId.val(multiplayer.sessionId);
    self.updateStatus();
  };

  this.startSync = function() {
    multiplayer.startSync();
    self.updateStatus();
  };

  this.resetSession = function() {
    multiplayer.resetSession();
    self.$link.val('');
    self.updateStatus();
  };

  this.disconnect = function() {
    multiplayer.disconnect();
    self.updateStatus();
  };

  this.focusLinkInput = function() {
    main.tabClicked('navMultiplayer');
    self.$link.focus();
  };
}

multiplayer.init();
multiplayerPanel.init();
