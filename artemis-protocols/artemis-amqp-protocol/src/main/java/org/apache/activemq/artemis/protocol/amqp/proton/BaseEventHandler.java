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

import org.apache.activemq.artemis.protocol.amqp.proton.handler.EventHandler;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.ProtonHandler;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.jboss.logging.Logger;

/**
 * Handles events on a client initiated connection.
 */
public class BaseEventHandler implements EventHandler {
   private static final Logger log = Logger.getLogger(BaseEventHandler.class);

   private AMQPConnectionContext amqpConnectionContext;

   public BaseEventHandler(AMQPConnectionContext amqpConnectionContext) {
      this.amqpConnectionContext = amqpConnectionContext;
   }

   @Override
   public void onInit(Connection connection) throws Exception {

   }

   @Override
   public void onLocalOpen(Connection connection) throws Exception {

   }

   @Override
   public void onLocalClose(Connection connection) throws Exception {

   }

   @Override
   public void onFinal(Connection connection) throws Exception {

   }

   @Override
   public void onInit(Session session) throws Exception {

   }

   @Override
   public void onFinal(Session session) throws Exception {

   }

   @Override
   public void onInit(Link link) throws Exception {

   }

   @Override
   public void onLocalOpen(Link link) throws Exception {

   }

   @Override
   public void onLocalClose(Link link) throws Exception {

   }

   @Override
   public void onFinal(Link link) throws Exception {

   }

   @Override
   public void onAuthInit(ProtonHandler handler, Connection connection, boolean sasl) {
      amqpConnectionContext.authInit(sasl);
   }

   @Override
   public void onTransport(Transport transport) {
      amqpConnectionContext.flushBytes();
   }

   @Override
   public void onRemoteOpen(Connection connection) throws Exception {
      synchronized (amqpConnectionContext.getLock()) {
         try {
            amqpConnectionContext.initInternal();
         } catch (Exception e) {
            log.error("Error init connection", e);
         }
         if (!amqpConnectionContext.validateConnection(connection)) {
            connection.close();
         } else {
            connection.setContext(amqpConnectionContext);
            connection.setContainer(amqpConnectionContext.getContainer());
            connection.setProperties(amqpConnectionContext.getConnectionProperties());
            connection.setOfferedCapabilities(amqpConnectionContext.getConnectionCapabilitiesOffered());
            connection.open();
         }
      }
      amqpConnectionContext.initialise();

      /*
       * This can be null which is in effect an empty map, also we really dont need to check this for in bound connections
       * but its here in case we add support for outbound connections.
       */
      if (connection.getRemoteProperties() == null || !connection.getRemoteProperties().containsKey(AMQPConnectionContext.CONNECTION_OPEN_FAILED)) {
         amqpConnectionContext.startKeepAliveTimer();
      }
   }

   @Override
   public void onRemoteClose(Connection connection) {
      synchronized (amqpConnectionContext.getLock()) {
         connection.close();
         connection.free();
         amqpConnectionContext.deleteSessions();
      }

      // We must force write the channel before we actually destroy the connection
      amqpConnectionContext.flushBytes();
      amqpConnectionContext.destroy();
   }

   @Override
   public void onLocalOpen(Session session) throws Exception {
      amqpConnectionContext.getSessionExtension(session);
   }

   @Override
   public void onRemoteOpen(Session session) throws Exception {
      amqpConnectionContext.getSessionExtension(session).initialise();
      synchronized (amqpConnectionContext.getLock()) {
         session.open();
      }
   }

   @Override
   public void onLocalClose(Session session) throws Exception {
   }

   @Override
   public void onRemoteClose(Session session) throws Exception {
      synchronized (amqpConnectionContext.getLock()) {
         session.close();
         session.free();
      }

      AMQPSessionContext sessionContext = (AMQPSessionContext) session.getContext();
      if (sessionContext != null) {
         sessionContext.close();
         amqpConnectionContext.removeSession(session);
         session.setContext(null);
      }
   }

   @Override
   public void onRemoteOpen(Link link) throws Exception {
      AMQPSessionContext protonSession = amqpConnectionContext.getSessionExtension(link.getSession());

      link.setSource(link.getRemoteSource());
      link.setTarget(link.getRemoteTarget());
      if (link instanceof Receiver) {
         Receiver receiver = (Receiver) link;
         if (link.getRemoteTarget() instanceof Coordinator) {
            Coordinator coordinator = (Coordinator) link.getRemoteTarget();
            protonSession.addTransactionHandler(coordinator, receiver);
         } else {
            protonSession.addReceiver(receiver);
         }
      } else {
         Sender sender = (Sender) link;
         protonSession.addSender(sender);
         sender.offer(1);
      }
   }

   @Override
   public void onFlow(Link link) throws Exception {
      if (link.getContext() != null) {
         ((ProtonDeliveryHandler) link.getContext()).onFlow(link.getCredit(), link.getDrain());
      }
   }

   @Override
   public void onRemoteClose(Link link) throws Exception {
      link.close();
      link.free();
      ProtonDeliveryHandler linkContext = (ProtonDeliveryHandler) link.getContext();
      if (linkContext != null) {
         linkContext.close(true);
      }
   }

   @Override
   public void onRemoteDetach(Link link) throws Exception {
      link.detach();
      link.free();
   }

   @Override
   public void onLocalDetach(Link link) throws Exception {
      Object context = link.getContext();
      if (context instanceof ProtonServerSenderContext) {
         ProtonServerSenderContext senderContext = (ProtonServerSenderContext) context;
         senderContext.close(false);
      }
   }

   @Override
   public void onDelivery(Delivery delivery) throws Exception {
      ProtonDeliveryHandler handler = (ProtonDeliveryHandler) delivery.getLink().getContext();
      if (handler != null) {
         handler.onMessage(delivery);
      } else {
         // TODO: logs
         System.err.println("Handler is null, can't delivery " + delivery);
      }
   }
}
