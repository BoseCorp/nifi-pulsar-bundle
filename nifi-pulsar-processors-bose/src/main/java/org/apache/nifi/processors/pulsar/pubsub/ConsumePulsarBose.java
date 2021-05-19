/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pulsar.pubsub;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.components.AllowableValue;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.commons.io.IOUtils;

//@SeeAlso({PublishPulsar.class, ConsumePulsarRecord.class, PublishPulsarRecord.class})
@SeeAlso({PublishPulsarBose.class})
@Tags({"Pulsar", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@CapabilityDescription("Consumes messages from Apache Pulsar, with message properties. The complementary NiFi processor for sending messages is PublishPulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
    @WritesAttribute(attribute = "message.header.<headerName>", description = "Message property from Pulsar message"),
    @WritesAttribute(attribute = "message.eventTime", description = "Message property from Pulsar message"),
    @WritesAttribute(attribute = "message.publishTime", description = "Message property from Pulsar message"),
    @WritesAttribute(attribute = "message.sequenceId", description = "Message property from Pulsar message"),
    @WritesAttribute(attribute = "message.topicName", description = "Message property from Pulsar message"),
    @WritesAttribute(attribute = "message.redeliveryCount", description = "Message property from Pulsar message"),
    @WritesAttribute(attribute = "message.id", description = "Message property from Pulsar message"),
    @WritesAttribute(attribute = "message.producerName", description = "Message property from Pulsar message")
})
public class ConsumePulsarBose extends AbstractPulsarConsumerProcessor<byte[]> {

    // repeating these definitions from the base class because they are not accessible across nars
    protected static final AllowableValue EXCLUSIVE = new AllowableValue("Exclusive", "Exclusive", "There can be only 1 consumer on the same topic with the same subscription name");
    protected static final AllowableValue SHARED = new AllowableValue("Shared", "Shared", "Multiple consumer will be able to use the same subscription name and the messages");
    protected static final AllowableValue FAILOVER = new AllowableValue("Failover", "Failover", "Multiple consumer will be able to use the same subscription name but only 1 consumer "
            + "will receive the messages. If that consumer disconnects, one of the other connected consumers will start receiving messages.");

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            Consumer<byte[]> consumer = getConsumer(context, getConsumerId(context, session.get()));

            if (consumer == null) {
                context.yield();
                return;
            }

            if (context.getProperty(ASYNC_ENABLED).asBoolean()) {
                consumeAsync(consumer, context, session);
                handleAsync(consumer, context, session);
            } else {
                consume(consumer, context, session);
            }
        } catch (PulsarClientException e) {
            getLogger().error("Unable to consume from Pulsar Topic ", e);
            context.yield();
            throw new ProcessException(e);
        }
    }

    private void handleAsync(final Consumer<byte[]> consumer, ProcessContext context, ProcessSession session) {
        try {
            Future<List<Message<byte[]>>> done = getConsumerService().poll(5, TimeUnit.SECONDS);

            if (done != null) {

                //final byte[] demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                //    .evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8) : null;

                List<Message<byte[]>> messages = done.get();

                if (CollectionUtils.isNotEmpty(messages)) {
                    AtomicInteger msgCount = new AtomicInteger(0);

                    messages.forEach(msg -> {
                        if (msg.getValue() != null && msg.getValue().length > 0) {
                            FlowFile flowFile = session.create();
                            OutputStream out = session.write(flowFile);
                            try {
                                out.write(msg.getValue());
                                //out.write(demarcatorBytes);
                                msgCount.getAndIncrement();
                            } catch (final IOException ioEx) {
                                session.rollback();
                                return;
                            }
                            IOUtils.closeQuietly(out);

                            session.putAttribute(flowFile, "message.eventTime", Long.toString(msg.getEventTime()));
                            session.putAttribute(flowFile, "message.publishTime", Long.toString(msg.getPublishTime()));
                            session.putAttribute(flowFile, "message.sequenceId", Long.toString(msg.getSequenceId()));

                            String topicName = msg.getTopicName();
                            if (topicName != null) {
                                session.putAttribute(flowFile, "message.topicName", topicName);
                            }

                            session.putAttribute(flowFile, "message.redeliveryCount", Integer.toString(msg.getRedeliveryCount()));
        
                            String key = msg.getKey();
                            if (key != null) {
                                session.putAttribute(flowFile, "message.key", key);
                            }

                            MessageId msgid = msg.getMessageId();
                            if (msgid != null) {
                                session.putAttribute(flowFile, "message.id", Base64.getEncoder().encodeToString(msgid.toByteArray()));
                            }
        
                            String producerName = msg.getProducerName();
                            if (producerName != null) {
                                session.putAttribute(flowFile, "message.producerName", producerName);
                            }

                            // Populate headers as flowfile attributes
                            Map<String,String> msgprops = msg.getProperties();
                            if (msgprops != null) {
                                for (String k: msgprops.keySet()) {
                                    session.putAttribute(flowFile, "message.header." + k, msgprops.get(k));
                                }
                            }

                            session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                            session.transfer(flowFile, REL_SUCCESS);    
                        }
                    });
                    session.commit();
                }
                // Acknowledge consuming the message
                getAckService().submit(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                       return consumer.acknowledgeCumulativeAsync(messages.get(messages.size()-1)).get();
                    }
                });
            }
        } catch (InterruptedException | ExecutionException e) {
            getLogger().error("Trouble consuming messages ", e);
        }
    }

    private void consume(Consumer<byte[]> consumer, ProcessContext context, ProcessSession session) throws PulsarClientException {
 
        try {
            final int maxMessages = context.getProperty(CONSUMER_BATCH_SIZE).isSet() ? context.getProperty(CONSUMER_BATCH_SIZE)
                    .evaluateAttributeExpressions().asInteger() : Integer.MAX_VALUE;

            //final byte[] demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
            //        .evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8) : null;
            
            // Cumulative acks are NOT permitted on Shared subscriptions.
            final boolean shared = context.getProperty(SUBSCRIPTION_TYPE).getValue()
                .equalsIgnoreCase(SHARED.getValue());

            Message<byte[]> msg = null;
            Message<byte[]> lastMsg = null;
            AtomicInteger msgCount = new AtomicInteger(0);
            AtomicInteger loopCounter = new AtomicInteger(0);
            FlowFile flowFile = null;

            while (((msg = consumer.receive(0, TimeUnit.SECONDS)) != null) && loopCounter.get() < maxMessages) {
                try {        
                    lastMsg = msg;
                    loopCounter.incrementAndGet();
                    
                    if (shared) {
                    	consumer.acknowledge(msg);
                    }

                    // Skip empty messages, as they cause NPE's when we write them to the OutputStream
                    if (msg.getValue() == null || msg.getValue().length < 1) {
                      continue;
                    }
                    flowFile = session.create();
                    OutputStream out = session.write(flowFile);
                    out.write(msg.getValue());
                    //out.write(demarcatorBytes);
                    IOUtils.closeQuietly(out);

                    msgCount.getAndIncrement();

                    session.putAttribute(flowFile, "message.eventTime", Long.toString(msg.getEventTime()));
                    session.putAttribute(flowFile, "message.publishTime", Long.toString(msg.getPublishTime()));
                    session.putAttribute(flowFile, "message.sequenceId", Long.toString(msg.getSequenceId()));

                    String topicName = msg.getTopicName();
                    if (topicName != null) {
                        session.putAttribute(flowFile, "message.topicName", topicName);
                    }
                    
                    session.putAttribute(flowFile, "message.redeliveryCount", Integer.toString(msg.getRedeliveryCount()));

                    String key = msg.getKey();
                    if (key != null) {
                        session.putAttribute(flowFile, "message.key", key);
                    }

                    MessageId msgid = msg.getMessageId();
                    if (msgid != null) {
                        session.putAttribute(flowFile, "message.id", Base64.getEncoder().encodeToString(msgid.toByteArray()));
                    }

                    String producerName = msg.getProducerName();
                    if (producerName != null) {
                        session.putAttribute(flowFile, "message.producerName", producerName);
                    }

                    // Populate headers as flowfile attributes
                    Map<String,String> msgprops = msg.getProperties();
                    if (msgprops != null) {
                        for (String k: msgprops.keySet()) {
                            session.putAttribute(flowFile, "message.header." + k, msgprops.get(k));
                        }
                    }

                    session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                    session.transfer(flowFile, REL_SUCCESS);
                    getLogger().debug("Created {} (message number {} in the current batch) received from Pulsar Server and transferred to 'success'",
                       new Object[]{flowFile, msgCount.toString()});
    

                } catch (final IOException ioEx) {
                  getLogger().error("Unable to create flow file ", ioEx);
                  session.rollback();
                  if (!shared) {
                     consumer.acknowledgeCumulative(lastMsg);
                  }
                  return;
                }
            }
            
            if (!shared && lastMsg != null)  {
                consumer.acknowledgeCumulative(lastMsg);
            }

            session.commit();

        } catch (PulsarClientException e) {
        	getLogger().error("Error communicating with Apache Pulsar", e);
            context.yield();
            session.rollback();
        }
    }
}