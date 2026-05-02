// capture/PacketAnalyzer.cpp
//
// NetWatch — Packet Capture and Flow Tracking Layer
// Wire-speed packet ingestion via libpcap. Maintains a five-tuple
// flow table and emits FlowRecord telemetry to the Java coordinator
// via gRPC streaming.
//
// Build: cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

#include <arpa/inet.h>
#include <netinet/if_ether.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <pcap/pcap.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstring>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// ---------------------------------------------------------------------------
// Five-tuple flow key
// ---------------------------------------------------------------------------

struct FlowKey {
    uint32_t src_ip;
    uint32_t dst_ip;
    uint16_t src_port;
    uint16_t dst_port;
    uint8_t  proto;

    bool operator==(const FlowKey& o) const noexcept {
        return src_ip   == o.src_ip   &&
               dst_ip   == o.dst_ip   &&
               src_port == o.src_port &&
               dst_port == o.dst_port &&
               proto    == o.proto;
    }
};

struct FlowKeyHash {
    std::size_t operator()(const FlowKey& k) const noexcept {
        // FNV-1a-inspired combination
        std::size_t h = 14695981039346656037ULL;
        auto mix = [&](auto v) {
            h ^= static_cast<std::size_t>(v);
            h *= 1099511628211ULL;
        };
        mix(k.src_ip);
        mix(k.dst_ip);
        mix(k.src_port);
        mix(k.dst_port);
        mix(k.proto);
        return h;
    }
};

// ---------------------------------------------------------------------------
// Flow state
// ---------------------------------------------------------------------------

using Clock     = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Micros    = std::chrono::microseconds;

struct FlowState {
    uint64_t  packet_count   = 0;
    uint64_t  byte_count     = 0;
    uint32_t  syn_count      = 0;
    uint32_t  rst_count      = 0;
    uint32_t  fin_count      = 0;
    TimePoint first_seen;
    TimePoint last_seen;
    int64_t   last_iat_us    = 0;    ///< Last inter-arrival time (µs)
    double    iat_mean_us    = 0.0;
    double    iat_m2         = 0.0;  ///< Welford's online M2 for variance
    double    payload_entropy= 0.0;  ///< Exponentially weighted entropy

    void update_iat(TimePoint now) {
        if (packet_count > 0) {
            int64_t iat = std::chrono::duration_cast<Micros>(now - last_seen).count();
            last_iat_us = iat;
            // Welford's online variance update
            packet_count++;
            double delta  = iat - iat_mean_us;
            iat_mean_us  += delta / static_cast<double>(packet_count);
            double delta2 = iat - iat_mean_us;
            iat_m2       += delta * delta2;
        } else {
            first_seen = now;
        }
        last_seen = now;
    }

    double iat_variance() const noexcept {
        return packet_count > 1 ? iat_m2 / static_cast<double>(packet_count - 1) : 0.0;
    }
};

// ---------------------------------------------------------------------------
// Shannon entropy of a byte buffer (payload)
// ---------------------------------------------------------------------------

static double payload_entropy(const uint8_t* buf, std::size_t len) noexcept {
    if (len == 0) return 0.0;
    std::array<uint32_t, 256> freq{};
    for (std::size_t i = 0; i < len; ++i) freq[buf[i]]++;
    double entropy = 0.0;
    double n = static_cast<double>(len);
    for (uint32_t f : freq) {
        if (f == 0) continue;
        double p = f / n;
        entropy -= p * std::log2(p);
    }
    return entropy;
}

// ---------------------------------------------------------------------------
// PacketAnalyzer
// ---------------------------------------------------------------------------

class PacketAnalyzer {
public:
    struct Config {
        std::string interface  = "eth0";
        std::string bpf_filter = "";        // e.g. "tcp or udp"
        int         snaplen    = 65535;
        int         timeout_ms = 100;
        std::size_t max_flows  = 1'000'000;
        uint32_t    flow_idle_timeout_s = 300;
    };

    explicit PacketAnalyzer(Config cfg)
        : cfg_(std::move(cfg)), running_(false) {}

    ~PacketAnalyzer() { stop(); }

    /// Callback type: called with a snapshot of a flow's state on expiry or flush.
    using FlowCallback = std::function<void(const FlowKey&, const FlowState&)>;

    void on_flow_expired(FlowCallback cb) { expiry_cb_ = std::move(cb); }

    bool start() {
        char errbuf[PCAP_ERRBUF_SIZE];
        handle_ = pcap_open_live(
            cfg_.interface.c_str(),
            cfg_.snaplen,
            /*promisc=*/1,
            cfg_.timeout_ms,
            errbuf
        );

        if (!handle_) {
            std::cerr << "[pcap] open_live failed: " << errbuf << "\n";
            return false;
        }

        // Apply BPF filter if specified
        if (!cfg_.bpf_filter.empty()) {
            bpf_program fp{};
            if (pcap_compile(handle_, &fp, cfg_.bpf_filter.c_str(), 1, PCAP_NETMASK_UNKNOWN) < 0 ||
                pcap_setfilter(handle_, &fp) < 0) {
                std::cerr << "[pcap] BPF filter error: " << pcap_geterr(handle_) << "\n";
                return false;
            }
        }

        running_ = true;
        capture_thread_ = std::thread([this] { capture_loop(); });
        gc_thread_      = std::thread([this] { gc_loop(); });

        std::cout << "[netwatch] Capture started on " << cfg_.interface << "\n";
        return true;
    }

    void stop() {
        running_ = false;
        if (handle_) {
            pcap_breakloop(handle_);
        }
        if (capture_thread_.joinable()) capture_thread_.join();
        if (gc_thread_.joinable())      gc_thread_.join();
        if (handle_) {
            pcap_close(handle_);
            handle_ = nullptr;
        }
    }

    uint64_t packet_count() const noexcept { return pkt_count_.load(); }
    std::size_t flow_count() const {
        std::lock_guard lock(mu_);
        return flows_.size();
    }

private:
    Config        cfg_;
    pcap_t*       handle_  = nullptr;
    std::atomic<bool> running_;
    std::atomic<uint64_t> pkt_count_{0};
    std::thread   capture_thread_;
    std::thread   gc_thread_;
    mutable std::mutex mu_;
    std::unordered_map<FlowKey, FlowState, FlowKeyHash> flows_;
    FlowCallback  expiry_cb_;

    void capture_loop() {
        pcap_loop(
            handle_,
            /*count=*/-1,
            [](u_char* user, const struct pcap_pkthdr* hdr, const u_char* pkt) {
                auto* self = reinterpret_cast<PacketAnalyzer*>(user);
                self->process_packet(hdr, pkt);
            },
            reinterpret_cast<u_char*>(this)
        );
    }

    void process_packet(const struct pcap_pkthdr* hdr, const u_char* pkt) noexcept {
        ++pkt_count_;
        const auto now = Clock::now();

        // Minimum Ethernet header
        if (hdr->caplen < sizeof(ether_header)) return;
        const auto* eth = reinterpret_cast<const ether_header*>(pkt);
        if (ntohs(eth->ether_type) != ETHERTYPE_IP) return;

        const auto* ip = reinterpret_cast<const iphdr*>(pkt + sizeof(ether_header));
        if (hdr->caplen < sizeof(ether_header) + sizeof(iphdr)) return;

        FlowKey key{};
        key.src_ip = ip->saddr;
        key.dst_ip = ip->daddr;
        key.proto  = ip->protocol;

        const uint8_t* transport = pkt + sizeof(ether_header) + (ip->ihl * 4);
        const uint8_t* payload   = nullptr;
        std::size_t    plen      = 0;

        if (ip->protocol == IPPROTO_TCP) {
            const auto* tcp = reinterpret_cast<const tcphdr*>(transport);
            key.src_port = ntohs(tcp->source);
            key.dst_port = ntohs(tcp->dest);
            payload = transport + tcp->doff * 4;
            plen    = hdr->caplen - (payload - pkt);

            std::lock_guard lock(mu_);
            auto& flow = flows_[key];
            flow.update_iat(now);
            flow.byte_count += hdr->len;
            if (tcp->syn) ++flow.syn_count;
            if (tcp->rst) ++flow.rst_count;
            if (tcp->fin) ++flow.fin_count;
            if (plen > 0) {
                double ep = payload_entropy(payload, std::min(plen, std::size_t{256}));
                // Exponential moving average of entropy
                flow.payload_entropy = 0.9 * flow.payload_entropy + 0.1 * ep;
            }
        } else if (ip->protocol == IPPROTO_UDP) {
            const auto* udp = reinterpret_cast<const udphdr*>(transport);
            key.src_port = ntohs(udp->source);
            key.dst_port = ntohs(udp->dest);

            std::lock_guard lock(mu_);
            auto& flow = flows_[key];
            flow.update_iat(now);
            flow.byte_count += hdr->len;
        }
    }

    /// Garbage-collect idle flows and invoke the expiry callback.
    void gc_loop() {
        using namespace std::chrono_literals;
        while (running_) {
            std::this_thread::sleep_for(30s);

            const auto now     = Clock::now();
            const auto timeout = std::chrono::seconds(cfg_.flow_idle_timeout_s);

            std::lock_guard lock(mu_);
            for (auto it = flows_.begin(); it != flows_.end(); ) {
                if (now - it->second.last_seen > timeout) {
                    if (expiry_cb_) expiry_cb_(it->first, it->second);
                    it = flows_.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }
};

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

static std::atomic<bool> g_running{true};

int main(int argc, char* argv[]) {
    std::signal(SIGINT,  [](int) { g_running = false; });
    std::signal(SIGTERM, [](int) { g_running = false; });

    PacketAnalyzer::Config cfg;
    cfg.interface  = (argc > 1) ? argv[1] : "eth0";
    cfg.bpf_filter = "tcp or udp";

    PacketAnalyzer analyzer(cfg);

    analyzer.on_flow_expired([](const FlowKey& key, const FlowState& state) {
        char src[INET_ADDRSTRLEN], dst[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &key.src_ip, src, sizeof(src));
        inet_ntop(AF_INET, &key.dst_ip, dst, sizeof(dst));

        std::printf(
            "[flow_expired] %s:%u -> %s:%u proto=%u pkts=%lu bytes=%lu "
            "iat_mean_us=%.1f iat_var=%.1f entropy=%.3f syn=%u rst=%u\n",
            src, key.src_port, dst, key.dst_port, key.proto,
            state.packet_count, state.byte_count,
            state.iat_mean_us, std::sqrt(state.iat_variance()),
            state.payload_entropy,
            state.syn_count, state.rst_count
        );
        // In production: serialize to protobuf and send via gRPC streaming stub
    });

    if (!analyzer.start()) return 1;

    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        std::cout << "[netwatch] packets=" << analyzer.packet_count()
                  << " flows="   << analyzer.flow_count() << "\n";
    }

    analyzer.stop();
    return 0;
}
