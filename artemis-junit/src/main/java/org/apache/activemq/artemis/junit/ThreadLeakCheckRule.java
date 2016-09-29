/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.junit;

import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnector;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.rules.ExternalResource;

/**
 * Messaging tests are usually Thread intensive and a thread leak or a server leakage may affect future tests.
 * This Rule will prevent Threads leaking from one test into another by checking left over threads.
 * This will also clear Client Thread Pools from ActiveMQClient.
 */
public class ThreadLeakCheckRule extends ExternalResource {

   private static Logger log = Logger.getLogger(ThreadLeakCheckRule.class);

   private static Set<String> knownThreads = new HashSet<>();

   boolean enabled = true;

   private Map<Thread, StackTraceElement[]> previousThreads;

   public void disable() {
      enabled = false;
   }

   /**
    * Override to set up your specific external resource.
    *
    * @throws if setup fails (which will disable {@code after}
    */
   @Override
   protected void before() throws Throwable {
      // do nothing

      previousThreads = Thread.getAllStackTraces();

   }

   /**
    * Override to tear down your specific external resource.
    */
   @Override
   protected void after() {
      ActiveMQClient.clearThreadPools();
      InVMConnector.resetThreadPool();

      try {
         if (enabled) {
            boolean failed = true;

            boolean failedOnce = false;

            long timeout = System.currentTimeMillis() + 60000;
            while (failed && timeout > System.currentTimeMillis()) {
               failed = checkThread();

               if (failed) {
                  failedOnce = true;
                  forceGC();
                  try {
                     Thread.sleep(500);
                  } catch (Throwable e) {
                  }
               }
            }

            if (failed) {
               Assert.fail("Thread leaked");
            } else if (failedOnce) {
               System.out.println("******************** Threads cleared after retries ********************");
               System.out.println();
            }

         } else {
            enabled = true;
         }
      } finally {
         // clearing just to help GC
         previousThreads = null;
      }

   }

   private static int failedGCCalls = 0;

   public static void forceGC() {

      if (failedGCCalls >= 10) {
         log.info("ignoring forceGC call since it seems System.gc is not working anyways");
         return;
      }
      log.info("#test forceGC");
      CountDownLatch finalized = new CountDownLatch(1);
      WeakReference<DumbReference> dumbReference = new WeakReference<>(new DumbReference(finalized));

      long timeout = System.currentTimeMillis() + 1000;

      // A loop that will wait GC, using the minimal time as possible
      while (!(dumbReference.get() == null && finalized.getCount() == 0) && System.currentTimeMillis() < timeout) {
         System.gc();
         System.runFinalization();
         try {
            finalized.await(100, TimeUnit.MILLISECONDS);
         } catch (InterruptedException e) {
         }
      }

      if (dumbReference.get() != null) {
         failedGCCalls++;
         log.info("It seems that GC is disabled at your VM");
      } else {
         // a success would reset the count
         failedGCCalls = 0;
      }
      log.info("#test forceGC Done ");
   }

   public static void removeKownThread(String name) {
      knownThreads.remove(name);
   }

   public static void addKownThread(String name) {
      knownThreads.add(name);
   }

   private boolean checkThread() {
      boolean failedThread = false;

      Map<Thread, StackTraceElement[]> postThreads = Thread.getAllStackTraces();

      if (postThreads != null && previousThreads != null && postThreads.size() > previousThreads.size()) {

         for (Thread aliveThread : postThreads.keySet()) {
            if (aliveThread.isAlive() && !isExpectedThread(aliveThread) && !previousThreads.containsKey(aliveThread)) {
               if (!failedThread) {
                  System.out.println("*********************************************************************************");
                  System.out.println("LEAKING THREADS");
               }
               failedThread = true;
               System.out.println("=============================================================================");
               System.out.println("Thread " + aliveThread + " is still alive with the following stackTrace:");
               StackTraceElement[] elements = postThreads.get(aliveThread);
               for (StackTraceElement el : elements) {
                  System.out.println(el);
               }
            }

         }
         if (failedThread) {
            System.out.println("*********************************************************************************");
         }
      }

      return failedThread;
   }

   /**
    * if it's an expected thread... we will just move along ignoring it
    *
    * @param thread
    * @return
    */
   private boolean isExpectedThread(Thread thread) {

      for (String known : knownThreads) {
         if (thread.getName().contains(known)) {
            return true;
         }
      }

      return false;
   }

   protected static class DumbReference {

      private CountDownLatch finalized;

      public DumbReference(CountDownLatch finalized) {
         this.finalized = finalized;
      }

      @Override
      public void finalize() throws Throwable {
         finalized.countDown();
         super.finalize();
      }
   }

}
