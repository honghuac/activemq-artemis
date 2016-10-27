/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.proton;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.protocol.amqp.broker.AMQPConnectionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.EventHandler;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.ExtCapability;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.ProtonHandler;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.VersionLoader;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.jboss.logging.Logger;

import io.netty.buffer.ByteBuf;

public class AMQPConnectionContext extends ProtonInitializable {

   private static final Logger log = Logger.getLogger(AMQPConnectionContext.class);

   public static final Symbol CONNECTION_OPEN_FAILED = Symbol.valueOf("amqp:connection-establishment-failed");
   public static final String AMQP_CONTAINER_ID = "amqp-container-id";

   private final ProtonHandler handler;

   private AMQPConnectionCallback connectionCallback;
   private final ScheduledExecutorService scheduledPool;
   private final String containerId;
   private final Map<Symbol, Object> connectionProperties = new HashMap<>();

   private final Map<Session, AMQPSessionContext> sessions = new ConcurrentHashMap<>();

   public AMQPConnectionContext(AMQPConnectionCallback connectionSP,
                                String containerId,
                                int idleTimeout,
                                int maxFrameSize,
                                int channelMax,
                                Executor dispatchExecutor,
                                ScheduledExecutorService scheduledPool) {
      this.connectionCallback = connectionSP;
      this.containerId = (containerId != null) ? containerId : UUID.randomUUID().toString();

      connectionProperties.put(AmqpSupport.PRODUCT, "apache-activemq-artemis");
      connectionProperties.put(AmqpSupport.VERSION, VersionLoader.getVersion().getFullVersion());

      this.scheduledPool = scheduledPool;
      connectionCallback.setConnection(this);
      this.handler = new ProtonHandler(dispatchExecutor);
      Transport transport = handler.getTransport();
      transport.setEmitFlowEventOnSend(false);
      if (idleTimeout > 0) {
         transport.setIdleTimeout(idleTimeout);
      }
      transport.setChannelMax(channelMax);
      transport.setMaxFrameSize(maxFrameSize);
   }

   protected AMQPSessionContext newSessionExtension(Session realSession) throws ActiveMQAMQPException {
      AMQPSessionCallback sessionSPI = connectionCallback.createSessionCallback(this);
      AMQPSessionContext protonSession = new AMQPSessionContext(sessionSPI, this, realSession);

      return protonSession;
   }

   public SASLResult getSASLResult() {
      return handler.getSASLResult();
   }

   public void inputBuffer(ByteBuf buffer) {
      if (log.isTraceEnabled()) {
         ByteUtil.debugFrame(log, "Buffer Received ", buffer);
      }

      handler.inputBuffer(buffer);
   }

   public void destroy() {
      connectionCallback.close();
   }

   public boolean isSyncOnFlush() {
      return false;
   }

   public Object getLock() {
      return handler.getLock();
   }

   public int capacity() {
      return handler.capacity();
   }

   public void outputDone(int bytes) {
      handler.outputDone(bytes);
   }

   public void flush() {
      handler.flush();
   }

   public void close(ErrorCondition errorCondition) {
      handler.close(errorCondition);
   }

   protected AMQPSessionContext getSessionExtension(Session realSession) throws ActiveMQAMQPException {
      AMQPSessionContext sessionExtension = sessions.get(realSession);
      if (sessionExtension == null) {
         // how this is possible? Log a warn here
         sessionExtension = newSessionExtension(realSession);
         realSession.setContext(sessionExtension);
         sessions.put(realSession, sessionExtension);
      }
      return sessionExtension;
   }

   protected boolean validateConnection(Connection connection) {
      return connectionCallback.validateConnection(connection, handler.getSASLResult());
   }

   public boolean checkDataReceived() {
      return handler.checkDataReceived();
   }

   public long getCreationTime() {
      return handler.getCreationTime();
   }

   public void flushBytes() {
      ByteBuf bytes;
      // handler.outputBuffer has the lock
      while ((bytes = handler.outputBuffer()) != null) {
         connectionCallback.onTransport(bytes, this);
      }
   }

   public String getRemoteContainer() {
      return handler.getConnection().getRemoteContainer();
   }

   public String getPubSubPrefix() {
      return null;
   }

   protected void initInternal() throws Exception {
   }

   public Symbol[] getConnectionCapabilitiesOffered() {
      return ExtCapability.getCapabilities();
   }

   public void open() {
      handler.open(containerId);
   }

   public void addEventHandler(EventHandler eventHandler) {
      handler.addEventHandler(eventHandler);
   }

   public void removeSession(Session session) {
      sessions.remove(session);
   }

   public void deleteSessions() {
      for (AMQPSessionContext protonSession : sessions.values()) {
         protonSession.close();
      }
      sessions.clear();
   }

   public String getContainer() {
      return containerId;
   }

   public Map<Symbol, Object> getConnectionProperties() {
      return connectionProperties;
   }

   public void authInit(boolean sasl) {
      if (sasl) {
         handler.createServerSASL(connectionCallback.getSASLMechnisms());
      } else {
         if (!connectionCallback.isSupportsAnonymous()) {
            connectionCallback.sendSASLSupported();
            connectionCallback.close();
            handler.close(null);
         }
      }
   }

   public void startKeepAliveTimer() {
      long nextKeepAliveTime = handler.tick(true);
      flushBytes();
      if (nextKeepAliveTime > 0 && scheduledPool != null) {
         scheduledPool.schedule(new Runnable() {
            @Override
            public void run() {
               long rescheduleAt = (handler.tick(false) - TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
               flushBytes();
               if (rescheduleAt > 0) {
                  scheduledPool.schedule(this, rescheduleAt, TimeUnit.MILLISECONDS);
               }
            }
         }, (nextKeepAliveTime - TimeUnit.NANOSECONDS.toMillis(System.nanoTime())), TimeUnit.MILLISECONDS);
      }
   }
}
