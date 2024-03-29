/*
 * Copyright 2014 Groupon.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arpnetworking.tsdcore.sinks.circonus;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.sinks.circonus.api.BrokerListResponse;
import com.arpnetworking.tsdcore.sinks.circonus.api.CheckBundle;
import com.arpnetworking.tsdcore.statistics.HistogramStatistic;
import com.arpnetworking.utility.partitioning.PartitionSet;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import it.unimi.dsi.fastutil.doubles.Double2LongMap;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.http.javadsl.model.StatusCodes;
import org.apache.pekko.pattern.PatternsCS;
import play.libs.ws.StandaloneWSResponse;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.duration.FiniteDuration;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Reports data to Circonus HttpTrap.
 *
 * This actor maintains a non-blocking HTTP client to Circonus internally.  It is responsible for
 * creating the necessary check bundles to post to and maintains a mapping of incoming aggregated data
 * and the check bundles that accept that data.
 *
 * Messages:
 *   External -
 *     Aggregation - Sent to the actor to send AggregatedData to the sink.
 *   Internal -
 *     BrokerListResponse - Sent from the Circonus client.  After receiving, used to decide which brokers to send
 *       the check bundle registrations for.
 *     ServiceCheckBinding - Sent internally after registration of a check bundle.  The binding is stored internally
 *       to keep track of check bundle urls.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
@SuppressWarnings("deprecation")
public final class CirconusSinkActor extends AbstractActor {
    /**
     * Creates a {@link org.apache.pekko.actor.Props} for use in Pekko.
     *
     * @param client Circonus client
     * @param broker Circonus broker to push to
     * @param maximumConcurrency the maximum number of parallel metric submissions
     * @param maximumQueueSize the maximum size of the pending metrics queue
     * @param spreadDuration the maximum wait time before starting to send metrics
     * @param enableHistograms true to turn on histogram publication
     * @param partitionSet the partition set to partition the check bundles with
     * @return A new {@link org.apache.pekko.actor.Props}
     */
    public static Props props(
            final CirconusClient client,
            final String broker,
            final int maximumConcurrency,
            final int maximumQueueSize,
            final Duration spreadDuration,
            final boolean enableHistograms,
            final PartitionSet partitionSet) {
        return Props.create(
                CirconusSinkActor.class,
                client,
                broker,
                maximumConcurrency,
                maximumQueueSize,
                spreadDuration,
                enableHistograms,
                partitionSet);
    }

    /**
     * Public constructor.
     *
     * @param client Circonus client
     * @param broker Circonus broker to push to
     * @param maximumConcurrency the maximum number of parallel metric submissions
     * @param maximumQueueSize the maximum size of the pending metrics queue
     * @param spreadDuration the maximum wait time before starting to send metrics
     * @param enableHistograms true to turn on histogram publication
     * @param partitionSet the partition set to partition the check bundles with
     */
    @SuppressFBWarnings(value = "DMI_RANDOM_USED_ONLY_ONCE", justification = "Random is used to spread load, only used once is ok")
    public CirconusSinkActor(
            final CirconusClient client,
            final String broker,
            final int maximumConcurrency,
            final int maximumQueueSize,
            final Duration spreadDuration,
            final boolean enableHistograms,
            final PartitionSet partitionSet) {
        _client = client;
        _brokerName = broker;
        _maximumConcurrency = maximumConcurrency;
        _enableHistograms = enableHistograms;
        _partitionSet = partitionSet;
        _pendingRequests = EvictingQueue.create(maximumQueueSize);
        if (Duration.ZERO.equals(spreadDuration)) {
            _spreadingDelayMillis = 0;
        } else {
            _spreadingDelayMillis = new Random().nextInt((int) spreadDuration.toMillis());
        }
        _dispatcher = getContext().system().dispatcher();
        context().actorOf(BrokerRefresher.props(_client), "broker-refresher");
        _checkBundleRefresher = context().actorOf(CheckBundleActivator.props(_client), "check-bundle-refresher");
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("actor", this.self())
                .put("brokerName", _brokerName)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(EmitAggregation.class, aggregation -> {
                    if (_selectedBrokerCid.isPresent()) {
                        final Collection<AggregatedData> data = aggregation.getData();
                        publish(data);
                    } else {
                        NO_BROKER_LOGGER.warn()
                                .setMessage("Unable to push data to Circonus")
                                .addData("reason", "desired broker not yet discovered")
                                .addData("actor", self())
                                .log();
                    }
                })
                .match(CheckBundleLookupResponse.class, response -> {
                    if (response.isSuccess()) {
                        _bundleMap.put(response.getKey(), response.getCheckBundle().getCid());
                        _checkBundles.put(response.getCheckBundle().getCid(), response.getCheckBundle());
                        _checkBundleRefresher.tell(
                                new CheckBundleActivator.NotifyCheckBundle(response.getCheckBundle()),
                                self());
                    } else {
                        LOGGER.error()
                                .setMessage("Error creating check bundle")
                                .addData("request", response.getCheckBundle())
                                .addData("actor", self())
                                .setThrowable(response.getCause().get())
                                .log();
                        _pendingLookups.remove(response.getKey());
                    }
                })
                .match(CheckBundleActivator.CheckBundleRefreshComplete.class, update -> {
                    _checkBundles.put(update.getCheckBundle().getCid(), update.getCheckBundle());
                })
                .match(BrokerRefresher.BrokerLookupComplete.class, this::handleBrokerLookupComplete)
                .match(PostComplete.class, complete -> {
                    processCompletedRequest(complete);
                    dispatchPending();
                })
                .match(PostFailure.class, failure -> {
                    processFailedRequest(failure);
                    dispatchPending();
                })
                .match(WaitTimeExpired.class, message -> {
                    LOGGER.debug()
                            .setMessage("Received WaitTimeExpired message")
                            .addContext("actor", self())
                            .log();
                    _waiting = false;
                    dispatchPending();
                })
                .build();
    }

    private void handleBrokerLookupComplete(final BrokerRefresher.BrokerLookupComplete message) {
        final BrokerListResponse response = message.getResponse();
        final List<BrokerListResponse.Broker> brokers = response.getBrokers();

        Optional<BrokerListResponse.Broker> selectedBroker = Optional.empty();
        for (final BrokerListResponse.Broker broker : response.getBrokers()) {
            if (broker.getName().equalsIgnoreCase(_brokerName)) {
                selectedBroker = Optional.of(broker);
            }
        }

        if (!selectedBroker.isPresent()) {
            LOGGER.warn()
                    .setMessage("Broker list does not contain desired broker")
                    .addData("brokers", brokers)
                    .addData("desired", _brokerName)
                    .addData("actor", self())
                    .log();
        } else {
            LOGGER.info()
                    .setMessage("Broker list contains desired broker")
                    .addData("brokers", brokers)
                    .addData("desired", _brokerName)
                    .addData("actor", self())
                    .log();
            _selectedBrokerCid = Optional.of(selectedBroker.get().getCid());
        }
    }

    private void processCompletedRequest(final PostComplete complete) {
        _inflightRequestsCount--;
        final int responseStatusCode = complete.getResponse().getStatus();
        if (responseStatusCode == StatusCodes.OK.intValue()) {
            LOGGER.debug()
                    .setMessage("Data submission accepted")
                    .addData("status", responseStatusCode)
                    .addContext("actor", self())
                    .log();
        } else {
            LOGGER.warn()
                    .setMessage("Data submission rejected")
                    .addData("status", responseStatusCode)
                    .addContext("actor", self())
                    .log();
        }
    }

    private void processFailedRequest(final PostFailure failure) {
        _inflightRequestsCount--;
        LOGGER.error()
                .setMessage("Data submission error")
                .addContext("actor", self())
                .setThrowable(failure.getCause())
                .log();
    }

    private Map<String, Object> serialize(final Collection<AggregatedData> data) {
        final Map<String, Object> dataNode = Maps.newHashMap();
        for (final AggregatedData aggregatedData : data) {
            final String name = new StringBuilder()
                    .append(aggregatedData.getPeriod().toString())
                    .append("/")
                    .append(aggregatedData.getFQDSN().getMetric())
                    .append("/")
                    .append(aggregatedData.getFQDSN().getStatistic().getName())
                    .toString();
            // For histograms, if they're enabled, we'll build the histogram data node
            if (_enableHistograms && aggregatedData.getFQDSN().getStatistic() instanceof HistogramStatistic) {
                final HistogramStatistic.HistogramSupportingData histogramSupportingData = (HistogramStatistic.HistogramSupportingData)
                        aggregatedData.getSupportingData();
                final HistogramStatistic.HistogramSnapshot histogram = histogramSupportingData.getHistogramSnapshot();
                final List<String> valueList = new ArrayList<>((int) histogram.getEntriesCount());
                final MathContext context = new MathContext(2, RoundingMode.DOWN);
                for (final Double2LongMap.Entry entry : histogram.getValues()) {
                    for (int i = 0; i < entry.getValue(); i++) {
                        final BigDecimal decimal = new BigDecimal(entry.getKey(), context);
                        final String bucketString = String.format("H[%s]=%d", decimal.toPlainString(), entry.getValue());
                        valueList.add(bucketString);
                    }
                }

                final Map<String, Object> histogramValueNode = Maps.newHashMap();
                histogramValueNode.put("_type", "n"); // Histograms are type "n"
                histogramValueNode.put("_value", valueList);
                dataNode.put(name, histogramValueNode);
            } else {
                dataNode.put(name, aggregatedData.getValue().getValue());
            }
        }
        return dataNode;
    }

    private String getMetricKey(final AggregatedData data) {
        return String.format(
                "%s_%s_%s_%s_%s_%s",
                data.getFQDSN().getService(),
                data.getFQDSN().getCluster(),
                data.getHost(),
                data.getPeriod().toString(),
                data.getFQDSN().getMetric(),
                data.getFQDSN().getStatistic().getName());
    }

    private String getCheckBundleKey(final AggregatedData data) {
        final Integer partitionNumber = _partitionMap.get(getMetricKey(data));
        return String.format(
                "%s_%s_%s_%d",
                data.getFQDSN().getService(),
                data.getFQDSN().getCluster(),
                data.getHost(),
                partitionNumber);
    }

    private void registerMetricPartition(final AggregatedData data) {
        final String metric = getMetricKey(data);
        final Integer partition = _partitionMap.computeIfAbsent(
                metric,
                key -> _partitionSet.getOrCreatePartition(key).orElse(null));
        if (partition == null) {
            CANT_FIND_PARTITION_LOGGER.warn()
                    .setMessage("Cannot find or create partition for check bundle")
                    .addData("actor", self())
                    .addData("metric", metric)
                    .log();
        }

    }

    /**
      * Queues the messages for transmission.
      */
    private void publish(final Collection<AggregatedData> data) {
        final Map<String, List<AggregatedData>> dataMap = data.stream()
                // First, we need to make sure that the partitions for each of metrics has been registered
                .peek(this::registerMetricPartition)
                // Collect the aggregated data by the "key".  In this case the key is unique part of a check_bundle:
                // service, cluster, host, and partition number
                .collect(Collectors.groupingBy(this::getCheckBundleKey));

        final boolean pendingWasEmpty = _pendingRequests.isEmpty();
        final List<RequestQueueEntry> toQueue = Lists.newArrayList();

        for (final Map.Entry<String, List<AggregatedData>> entry : dataMap.entrySet()) {
            final String targetKey = entry.getKey();
            final Collection<AggregatedData> serviceData = entry.getValue();

            // Check to see if we already have a checkbundle for this metric
            final String bundleCid = _bundleMap.get(targetKey);
            if (bundleCid != null) {
                final CheckBundle binding = _checkBundles.get(bundleCid);
                // Queue the request(s)
                toQueue.add(new RequestQueueEntry(binding, serialize(serviceData)));
            } else {
                if (!_pendingLookups.contains(targetKey)) {
                    // We don't have an outstanding request to lookup the URI, create one.
                    final AggregatedData aggregatedData = Iterables.get(serviceData, 0);
                    _pendingLookups.add(targetKey);

                    final CompletionStage<CheckBundleLookupResponse> response = createCheckBundle(targetKey, aggregatedData);

                    // Send the completed, mapped response back to ourselves.
                    PatternsCS.pipe(response, _dispatcher).to(self());
                }

                // We can't send the request to it right now, skip this service
                NO_CHECK_BUNDLE_LOGGER.warn()
                        .setMessage("Unable to push data to Circonus")
                        .addData("reason", "check bundle not yet found or created")
                        .addData("actor", self())
                        .log();
            }
        }

        final int evicted = Math.max(0, toQueue.size() - _pendingRequests.remainingCapacity());
        _pendingRequests.addAll(toQueue);

        if (evicted > 0) {
            LOGGER.warn()
                    .setMessage("Evicted data from Circonus sink queue")
                    .addData("count", evicted)
                    .addContext("actor", self())
                    .log();
        }

        // If we don't currently have anything in-flight, we'll need to wait the spreading duration.
        // If we're already waiting, these requests will be sent after the waiting is over, no need to do anything else.
        if (pendingWasEmpty && !_waiting && _spreadingDelayMillis > 0) {
            _waiting = true;
            LOGGER.debug()
                    .setMessage("Scheduling http requests for later transmission")
                    .addData("delayMs", _spreadingDelayMillis)
                    .addContext("actor", self())
                    .log();
            context().system().scheduler().scheduleOnce(
                    FiniteDuration.apply(_spreadingDelayMillis, TimeUnit.MILLISECONDS),
                    self(),
                    new WaitTimeExpired(),
                    context().dispatcher(),
                    self());
        } else {
            dispatchPending();
        }
    }

    /**
     * Dispatches the number of pending requests needed to drain the pendingRequests queue or meet the maximum concurrency.
     */
    private void dispatchPending() {
        LOGGER.debug()
                .setMessage("Dispatching requests")
                .addContext("actor", self())
                .log();
        while (_inflightRequestsCount < _maximumConcurrency && !_pendingRequests.isEmpty()) {
            fireNextRequest();
        }
    }

    private void fireNextRequest() {
        final RequestQueueEntry request = _pendingRequests.poll();
        if (request != null) {
            _inflightRequestsCount++;

            final CompletionStage<Object> responsePromise = _client.sendToHttpTrap(
                            request.getData(),
                            request.getBinding().getSubmissionUrl())
                    .<Object>thenApply(PostComplete::new)
                    .exceptionally(PostFailure::new);
            PatternsCS.pipe(responsePromise, context().dispatcher()).to(self());
        }
    }

    private CompletionStage<CheckBundleLookupResponse> createCheckBundle(
            final String targetKey,
            final AggregatedData aggregatedData) {
        final Integer partition = _partitionMap.get(getMetricKey(aggregatedData));
        final CheckBundle request = new CheckBundle.Builder()
                .addBroker(_selectedBrokerCid.get())
                .addTag("monitoring_agent:aint")
                .addTag(String.format("monitoring_cluster:%s", aggregatedData.getFQDSN().getCluster()))
                .addTag(String.format("service:%s", aggregatedData.getFQDSN().getService()))
                .addTag(String.format("hostname:%s", aggregatedData.getHost()))
                .setTarget(aggregatedData.getHost())
                .setDisplayName(
                        String.format(
                                "%s/%s/%s",
                                aggregatedData.getFQDSN().getCluster(),
                                aggregatedData.getFQDSN().getService(),
                                partition))
                .setStatus("active")
                .build();

        // Map the response to a ServiceCheckBinding
        return _client.getOrCreateCheckBundle(request)
                .thenApply(
                        response -> {
                            final URI result;
                            result = response.getSubmissionUrl();
                            return CheckBundleLookupResponse.success(targetKey, response);
                        })
                .exceptionally(
                        failure -> CheckBundleLookupResponse.failure(targetKey, failure, request));
    }

    private Optional<String> _selectedBrokerCid = Optional.empty();
    private ActorRef _checkBundleRefresher;
    private int _inflightRequestsCount = 0;
    private boolean _waiting = false;

    private final ExecutionContextExecutor _dispatcher;
    private final Set<String> _pendingLookups = Sets.newHashSet();
    private final Map<String, String> _bundleMap = Maps.newHashMap(); // Holds check bundle name -> cid mapping
    private final Map<String, Integer> _partitionMap = Maps.newHashMap(); // Holds metric name -> partition number
    private final Map<String, CheckBundle> _checkBundles = Maps.newHashMap(); // Holds cid -> check bundle details
    private final CirconusClient _client;
    private final String _brokerName;
    private final int _maximumConcurrency;
    private final boolean _enableHistograms;
    private final PartitionSet _partitionSet;
    private final int _spreadingDelayMillis;
    private final EvictingQueue<RequestQueueEntry> _pendingRequests;

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CirconusSinkActor.class);
    private static final Logger NO_BROKER_LOGGER =
            LoggerFactory.getRateLimitLogger(CirconusSinkActor.class, Duration.ofSeconds(30));
    private static final Logger NO_CHECK_BUNDLE_LOGGER =
            LoggerFactory.getRateLimitLogger(CirconusSinkActor.class, Duration.ofSeconds(30));
    private static final Logger CANT_FIND_PARTITION_LOGGER =
            LoggerFactory.getRateLimitLogger(CirconusSinkActor.class, Duration.ofSeconds(30));

    /**
     * Message class to wrap a list of {@link com.arpnetworking.tsdcore.model.AggregatedData}.
     */
    public static final class EmitAggregation {

        /**
         * Public constructor.
         * @param data Data to emit.
         */
        public EmitAggregation(final Collection<AggregatedData> data) {
            _data = Lists.newArrayList(data);
        }

        public Collection<AggregatedData> getData() {
            return Collections.unmodifiableList(_data);
        }

        private final List<AggregatedData> _data;
    }

    private static final class CheckBundleLookupResponse {
        public static CheckBundleLookupResponse success(final String key, final CheckBundle checkBundle) {
            return new CheckBundleLookupResponse(key, Optional.<Throwable>empty(), checkBundle);
        }

        public static CheckBundleLookupResponse failure(final String key, final Throwable throwable, final CheckBundle request) {
            return new CheckBundleLookupResponse(key, Optional.of(throwable), request);
        }

        private CheckBundleLookupResponse(
                final String key,
                final Optional<Throwable> cause,
                final CheckBundle request) {
            _key = key;
            _cause = cause;
            _checkBundle = request;
        }

        public boolean isSuccess() {
            return !_cause.isPresent();
        }

        public boolean isFailed() {
            return _cause.isPresent();
        }

        public Optional<Throwable> getCause() {
            return _cause;
        }

        public String getKey() {
            return _key;
        }

        public CheckBundle getCheckBundle() {
            return _checkBundle;
        }

        private final String _key;
        private final Optional<Throwable> _cause;
        private final CheckBundle _checkBundle;
    }

    private static final class RequestQueueEntry {
        private RequestQueueEntry(final CheckBundle binding, final Map<String, Object> data) {
            _binding = binding;
            _data = data;
        }

        public CheckBundle getBinding() {
            return _binding;
        }

        public Map<String, Object> getData() {
            return _data;
        }

        private final CheckBundle _binding;
        private final Map<String, Object> _data;
    }

    /**
     * Message class to wrap a completed HTTP request.
     */
    private static final class PostFailure {
        private PostFailure(final Throwable throwable) {
            _throwable = throwable;
        }

        public Throwable getCause() {
            return _throwable;
        }

        private final Throwable _throwable;
    }

    /**
     * Message class to wrap an errored HTTP request.
     */
    private static final class PostComplete {
        private PostComplete(final StandaloneWSResponse response) {
            _response = response;
        }

        public StandaloneWSResponse getResponse() {
            return _response;
        }

        private final StandaloneWSResponse _response;
    }

    private static final class WaitTimeExpired {}
}

