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
package org.apache.activemq.artemis.integration.amqp;

import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq.artemis.core.server.ConnectorService;
import org.apache.activemq.artemis.core.server.ConnectorServiceFactory;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * TODO: Description
 */
public class AMQPConnectorServiceFactory implements ConnectorServiceFactory {
   private static final String ROUTER_HOST = "host";
   private static final String ROUTER_PORT = "port";
   private static final Set<String> properties = initializeProperties();

   private static Set<String> initializeProperties() {
      Set<String> properties = new LinkedHashSet<>();
      properties.add(ROUTER_HOST);
      properties.add(ROUTER_PORT);
      return properties;
   }

   @Override
   public ConnectorService createConnectorService(String connectorName, Map<String, Object> configuration, StorageManager storageManager, PostOffice postOffice, ScheduledExecutorService scheduledExecutorService) {
      String host = (String) configuration.get(ROUTER_HOST);
      int port = Integer.parseInt((String) configuration.get(ROUTER_PORT));
      ActiveMQAMQPLogger.LOGGER.infof("Creating connector host %s port %d", host, port);
      return new AMQPConnectorService(connectorName, host, port, ((PostOfficeImpl)postOffice).getServer(), scheduledExecutorService);
   }

   @Override
   public Set<String> getAllowableProperties() {
      return properties;
   }

   @Override
   public Set<String> getRequiredProperties() {
      return properties;
   }
}
