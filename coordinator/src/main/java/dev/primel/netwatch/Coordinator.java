// coordinator/src/main/java/dev/primel/netwatch/Coordinator.java
//
// NetWatch — Distributed Flow Coordinator
// Receives FlowRecord streams from C++ sensor nodes via gRPC,
// aggregates into tumbling windows, applies CUSUM change detection,
// and publishes alerts to Kafka.
//
// Requires: Java 21 (Virtual Threads via Project Loom), gRPC 1.63

package dev.primel.netwatch;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Central coordinator for the NetWatch distributed anomaly detection system.
 *
 * <p>Responsibilities:
 * <ol>
 *   <li>Accept FlowRecord gRPC streams from N sensor nodes</li>
 *   <li>Aggregate flow telemetry into 10-second tumbling windows</li>
 *   <li>Apply per-group CUSUM change detection against calibrated thresholds</li>
 *   <li>Publish AlertRecord messages to Kafka topic {@code netwatch.alerts}</li>
 *   <li>Expose threshold configuration HTTP endpoint for the R calibration module</li>
 * </ol>
 */
public class Coordinator {

    private static final Logger LOG = Logger.getLogger(Coordinator.class.getName());

    // ---------------------------------------------------------------------------
    // Configuration
    // ---------------------------------------------------------------------------

    record Config(
        int grpcPort,
        int httpPort,
        String kafkaBrokers,
        String alertTopic,
        long windowDurationMs,
        double defaultCusumK,   // CUSUM slack parameter
        double defaultCusumH    // CUSUM decision threshold
    ) {
        static Config fromEnv() {
            return new Config(
                Integer.parseInt(env("GRPC_PORT",        "9090")),
                Integer.parseInt(env("HTTP_PORT",        "8081")),
                env("KAFKA_BROKERS",  "localhost:9092"),
                env("ALERT_TOPIC",   "netwatch.alerts"),
                Long.parseLong(env("WINDOW_MS",          "10000")),
                Double.parseDouble(env("CUSUM_K",        "2.0")),
                Double.parseDouble(env("CUSUM_H",        "8.0"))
            );
        }

        private static String env(String key, String def) {
            return System.getenv().getOrDefault(key, def);
        }
    }

    // ---------------------------------------------------------------------------
    // Domain types
    // ---------------------------------------------------------------------------

    /** Aggregated features for a flow group within a tumbling window. */
    record FlowWindow(
        String  flowGroup,       // e.g. "srcSubnet/24"
        long    windowStart,
        long    windowEnd,
        long    totalPackets,
        long    totalBytes,
        double  avgIatUs,
        double  avgEntropy,
        double  synRatio,
        double  rstRatio,
        int     distinctDsts
    ) {}

    record Alert(
        String  alertId,
        String  flowGroup,
        String  reason,
        double  cusumStat,
        double  cusumThreshold,
        long    detectedAt,
        FlowWindow window
    ) {}

    // ---------------------------------------------------------------------------
    // CUSUM change detector (per flow group)
    // ---------------------------------------------------------------------------

    static class CusumDetector {
        private final double k;   // Slack: typically (mu_1 - mu_0) / 2 * sigma
        private final double h;   // Decision threshold
        private double s = 0.0;   // Accumulator
        private double baseline;  // mu_0 — estimated from historical telemetry

        CusumDetector(double baseline, double k, double h) {
            this.baseline = baseline;
            this.k = k;
            this.h = h;
        }

        /**
         * Update the CUSUM statistic with the latest observation.
         *
         * @param x  observed value (e.g., packets per window)
         * @return   true if S_t > h (change detected)
         */
        boolean update(double x) {
            s = Math.max(0.0, s + (x - baseline - k));
            return s > h;
        }

        double stat() { return s; }

        void reset() { s = 0.0; }

        void recalibrate(double newBaseline, double newK, double newH) {
            this.baseline = newBaseline;
            this.k = newK;  // Note: k/h are final in record — using mutable class here
        }
    }

    // ---------------------------------------------------------------------------
    // Flow aggregator (tumbling window)
    // ---------------------------------------------------------------------------

    static class FlowAggregator {
        private final long windowMs;

        // flowGroup -> list of raw flow records within current window
        private final ConcurrentHashMap<String, List<Map<String, Double>>> buckets
            = new ConcurrentHashMap<>();

        private long windowStart = System.currentTimeMillis();

        FlowAggregator(long windowMs) {
            this.windowMs = windowMs;
        }

        void ingest(String flowGroup, Map<String, Double> features) {
            buckets.computeIfAbsent(flowGroup, _ -> Collections.synchronizedList(new ArrayList<>()))
                   .add(features);
        }

        /**
         * If the current window has elapsed, flush and return aggregated windows.
         * Returns an empty list if the window is not yet due.
         */
        List<FlowWindow> flush() {
            long now = System.currentTimeMillis();
            if (now - windowStart < windowMs) return List.of();

            long end   = now;
            long start = windowStart;
            windowStart = end;

            List<FlowWindow> result = new ArrayList<>();

            buckets.forEach((group, records) -> {
                if (records.isEmpty()) return;

                List<Map<String, Double>> snapshot;
                synchronized (records) {
                    snapshot = new ArrayList<>(records);
                    records.clear();
                }

                long   pkts     = snapshot.stream().mapToLong(r -> r.getOrDefault("pkt_count", 0.0).longValue()).sum();
                long   bytes    = snapshot.stream().mapToLong(r -> r.getOrDefault("byte_count", 0.0).longValue()).sum();
                double avgIat   = snapshot.stream().mapToDouble(r -> r.getOrDefault("iat_mean_us", 0.0)).average().orElse(0);
                double avgEnt   = snapshot.stream().mapToDouble(r -> r.getOrDefault("payload_entropy", 0.0)).average().orElse(0);
                double synRatio = snapshot.stream().mapToDouble(r -> r.getOrDefault("syn_ratio", 0.0)).average().orElse(0);
                double rstRatio = snapshot.stream().mapToDouble(r -> r.getOrDefault("rst_ratio", 0.0)).average().orElse(0);
                int    dsts     = (int) snapshot.stream().mapToDouble(r -> r.getOrDefault("dst_ip", 0.0)).distinct().count();

                result.add(new FlowWindow(group, start, end, pkts, bytes, avgIat, avgEnt, synRatio, rstRatio, dsts));
            });

            return result;
        }
    }

    // ---------------------------------------------------------------------------
    // Coordinator core
    // ---------------------------------------------------------------------------

    private final Config config;
    private final FlowAggregator aggregator;
    private final ConcurrentHashMap<String, CusumDetector> detectors = new ConcurrentHashMap<>();
    private final KafkaProducer<String, String> producer;
    private final AtomicLong alertCount = new AtomicLong(0);
    private Server grpcServer;

    Coordinator(Config config) {
        this.config     = config;
        this.aggregator = new FlowAggregator(config.windowDurationMs());
        this.producer   = buildKafkaProducer(config.kafkaBrokers());
    }

    private KafkaProducer<String, String> buildKafkaProducer(String brokers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                   "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,       "lz4");
        props.put(ProducerConfig.LINGER_MS_CONFIG,              "5");
        return new KafkaProducer<>(props);
    }

    /**
     * Returns or creates a CUSUM detector for a flow group.
     * Baseline initialized to a reasonable default; R module recalibrates.
     */
    private CusumDetector getDetector(String flowGroup) {
        return detectors.computeIfAbsent(flowGroup, _ ->
            new CusumDetector(
                /*baseline=*/1000.0,
                config.defaultCusumK(),
                config.defaultCusumH()
            )
        );
    }

    /**
     * Process a flushed window: run CUSUM, emit alert if threshold breached.
     */
    private void evaluateWindow(FlowWindow window) {
        CusumDetector detector = getDetector(window.flowGroup());

        // Use packets-per-window as the primary change indicator
        boolean anomaly = detector.update(window.totalPackets());

        if (anomaly) {
            Alert alert = new Alert(
                UUID.randomUUID().toString(),
                window.flowGroup(),
                "CUSUM_THRESHOLD_BREACH",
                detector.stat(),
                config.defaultCusumH(),
                Instant.now().toEpochMilli(),
                window
            );
            publishAlert(alert);
            detector.reset();
        }
    }

    private void publishAlert(Alert alert) {
        long count = alertCount.incrementAndGet();
        String json = String.format(
            """
            {"alert_id":"%s","flow_group":"%s","reason":"%s",
             "cusum_stat":%.3f,"threshold":%.3f,"detected_at":%d,
             "total_packets":%d,"avg_entropy":%.4f}
            """,
            alert.alertId(), alert.flowGroup(), alert.reason(),
            alert.cusumStat(), alert.cusumThreshold(), alert.detectedAt(),
            alert.window().totalPackets(), alert.window().avgEntropy()
        ).replaceAll("\\s+", " ");

        ProducerRecord<String, String> record =
            new ProducerRecord<>(config.alertTopic(), alert.flowGroup(), json);

        producer.send(record, (metadata, ex) -> {
            if (ex != null) {
                LOG.severe("Failed to publish alert: " + ex.getMessage());
            } else {
                LOG.info(String.format("[alert #%d] group=%s stat=%.2f partition=%d",
                    count, alert.flowGroup(), alert.cusumStat(), metadata.partition()));
            }
        });
    }

    // ---------------------------------------------------------------------------
    // Lifecycle
    // ---------------------------------------------------------------------------

    void start() throws IOException, InterruptedException {
        // Use virtual threads (Project Loom) for gRPC handling
        var executor = Executors.newVirtualThreadPerTaskExecutor();

        grpcServer = ServerBuilder
            .forPort(config.grpcPort())
            .executor(executor)
            // .addService(new FlowReceiverService(aggregator))  // wire up gRPC stub
            .build()
            .start();

        LOG.info("gRPC server listening on :" + config.grpcPort());

        // Window flush scheduler
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            List<FlowWindow> windows = aggregator.flush();
            windows.forEach(this::evaluateWindow);
        }, config.windowDurationMs(), config.windowDurationMs(), TimeUnit.MILLISECONDS);

        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down coordinator...");
            grpcServer.shutdown();
            producer.close();
            scheduler.shutdown();
        }));

        grpcServer.awaitTermination();
    }

    // ---------------------------------------------------------------------------
    // Entry point
    // ---------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        LOG.info("NetWatch Coordinator starting | grpc=:" + config.grpcPort()
            + " kafka=" + config.kafkaBrokers());
        new Coordinator(config).start();
    }
}
