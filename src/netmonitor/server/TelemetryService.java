package netmonitor.server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import netmonitor.util.FileLogger;

/**
 * Layanan telemetri UDP sisi server: menerima datagram dari Agent, menyimpan metrik
 * per Agent dalam peta thread-safe, mendeteksi status stale, dan meneruskan
 * (fan-out unicast) ke Inspector yang telah berlangganan.
 *
 * Port UDP dan ambang batas stale dapat diatur lewat {@code -Dnetmonitor.udp.port} dan
 * {@code -Dnetmonitor.telemetry.staleMs}.
 */
public class TelemetryService {

    /** Port UDP bawaan untuk telemetri. */
    public static final int DEFAULT_UDP_PORT = 9091;
    /** Ambang batas stale bawaan (milidetik). */
    public static final long DEFAULT_STALE_MS = 30_000L;

    /** Pencatat log server. */
    private final FileLogger log;
    /** Port UDP yang dipakai layanan ini. */
    private final int udpPort;
    /** Ambang batas stale dalam milidetik. */
    private final long staleMs;

    /** Socket UDP untuk menerima datagram. */
    private DatagramSocket socket;
    /** Status apakah layanan masih berjalan. */
    private volatile boolean running;
    /** Thread penerima datagram UDP. */
    private Thread receiverThread;
    /** Scheduler untuk pemeriksaan status stale. */
    private ScheduledExecutorService staleChecker;

    /**
     * Metrik telemetri terakhir dari satu Agent.
     */
    public static final class AgentMetrics {
        /** Identitas Agent (username). */
        public volatile String agentId;
        /** Beban CPU (0.0 - 1.0), -1.0 bila tidak tersedia. */
        public volatile double cpuLoad;
        /** Persentase penggunaan memori (0 - 100). */
        public volatile double memPercent;
        /** Nomor urut terakhir yang diterima. */
        public volatile int seqNum;
        /** Waktu (ms) terakhir metrik diterima. */
        public volatile long lastSeen;
        /** True bila tidak ada telemetri dalam batas staleMs. */
        public volatile boolean stale;

        /** Konstruktor kosong. */
        public AgentMetrics() { }
    }

    /**
     * Tujuan fan-out unicast ke Inspector.
     */
    public static final class InspectorTarget {
        /** Alamat IP Inspector (dari soket TCP). */
        public final InetAddress address;
        /** Port UDP lokal Inspector. */
        public final int udpPort;

        /**
         * Membuat tujuan fan-out.
         *
         * @param address alamat IP Inspector
         * @param udpPort port UDP lokal Inspector
         */
        public InspectorTarget(InetAddress address, int udpPort) {
            this.address = address;
            this.udpPort = udpPort;
        }
    }

    private final ConcurrentHashMap<String, AgentMetrics> metrics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, InspectorTarget> subscribers = new ConcurrentHashMap<>();

    /**
     * Membuat layanan telemetri.
     *
     * @param log pencatat log
     */
    public TelemetryService(FileLogger log) {
        this.log = log;
        this.udpPort = Integer.getInteger("netmonitor.udp.port", DEFAULT_UDP_PORT);
        this.staleMs = Long.getLong("netmonitor.telemetry.staleMs", DEFAULT_STALE_MS);
    }

    /**
     * Memulai socket UDP dan thread penerima.
     *
     * @throws IOException bila port tidak dapat dipakai
     */
    public void start() throws IOException {
        socket = new DatagramSocket(udpPort);
        socket.setSoTimeout(5000);
        running = true;
        receiverThread = new Thread(this::receiveLoop, "TelemetryReceiver");
        receiverThread.setDaemon(true);
        receiverThread.start();
        staleChecker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "TelemetryStaleChecker");
            t.setDaemon(true);
            return t;
        });
        staleChecker.scheduleWithFixedDelay(this::checkStale, staleMs, Math.max(1000L, staleMs / 2), TimeUnit.MILLISECONDS);
        log.info("TelemetryService aktif di port UDP " + udpPort + " (stale " + staleMs + " ms)");
    }

    /**
     * Menghentikan layanan dan menutup socket.
     */
    public void stop() {
        running = false;
        if (staleChecker != null) {
            staleChecker.shutdownNow();
        }
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
        if (receiverThread != null) {
            receiverThread.interrupt();
        }
    }

    /**
     * Loop penerima datagram UDP.
     */
    private void receiveLoop() {
        byte[] buf = new byte[512];
        while (running) {
            DatagramPacket pk = new DatagramPacket(buf, buf.length);
            try {
                socket.receive(pk);
            } catch (SocketTimeoutException e) {
                continue;
            } catch (IOException e) {
                if (running) {
                    log.warn("Telemetry receive gagal: " + e.getMessage());
                }
                continue;
            }
            try {
                processPacket(pk);
            } catch (RuntimeException e) {
                log.warn("Telemetry proses gagal: " + e);
            }
        }
    }

    /**
     * Memproses satu datagram yang masuk.
     *
     * @param pk paket yang diterima
     */
    private void processPacket(DatagramPacket pk) {
        String text = new String(pk.getData(), 0, pk.getLength(), StandardCharsets.UTF_8).trim();
        if (text.isEmpty()) {
            return;
        }
        String[] p = text.split("\\s+");
        String cmd = p[0].toUpperCase();
        if (p.length < 2) {
            return;
        }
        String agentId = p[1];
        if ("HELLO".equals(cmd)) {
            int seq = 0;
            if (p.length >= 3) {
                try {
                    seq = Integer.parseInt(p[2]);
                } catch (NumberFormatException ignored) {
                    seq = 0;
                }
            }
            sendAck(pk.getAddress(), pk.getPort(), seq);
            long now = System.currentTimeMillis();
            metrics.compute(agentId, (k, m) -> {
                if (m == null) {
                    m = new AgentMetrics();
                    m.agentId = agentId;
                }
                m.lastSeen = now;
                m.stale = false;
                return m;
            });
            log.info("Telemetry HELLO dari " + agentId + " (seq " + seq + ")");
        } else if ("TEL".equals(cmd)) {
            if (p.length < 5) {
                return;
            }
            int seq;
            double cpu;
            double mem;
            try {
                seq = Integer.parseInt(p[2]);
                cpu = Double.parseDouble(p[3]);
                mem = Double.parseDouble(p[4]);
            } catch (NumberFormatException e) {
                return;
            }
            AgentMetrics m = metrics.computeIfAbsent(agentId, k -> {
                AgentMetrics nm = new AgentMetrics();
                nm.agentId = agentId;
                return nm;
            });
            if (m.seqNum > 0 && seq > m.seqNum + 1) {
                log.warn("Celah telemetri " + agentId + ": seq " + m.seqNum + " -> " + seq);
            }
            m.seqNum = seq;
            m.cpuLoad = cpu;
            m.memPercent = mem;
            m.lastSeen = System.currentTimeMillis();
            m.stale = false;
            String payload = "TEL " + agentId + " " + seq + " " + cpu + " " + mem;
            fanOut(payload);
        }
    }

    /**
     * Mengirim ACK untuk datagram kontrol.
     *
     * @param addr alamat tujuan
     * @param port port tujuan
     * @param seq  nomor urut
     */
    private void sendAck(InetAddress addr, int port, int seq) {
        byte[] data = ("ACK " + seq).getBytes(StandardCharsets.UTF_8);
        try {
            socket.send(new DatagramPacket(data, data.length, addr, port));
        } catch (IOException e) {
            log.warn("Gagal kirim ACK: " + e.getMessage());
        }
    }

    /**
     * Fan-out unicast ke semua Inspector yang berlangganan.
     *
     * @param payload isi datagram
     */
    private void fanOut(String payload) {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        for (InspectorTarget t : subscribers.values()) {
            try {
                socket.send(new DatagramPacket(data, data.length, t.address, t.udpPort));
            } catch (IOException ex) {
                log.warn("Fan-out gagal ke " + t.address + ":" + t.udpPort + " (" + ex.getMessage() + ")");
            }
        }
    }

    /**
     * Memeriksa dan menandai metrik yang sudah stale (tanpa menghapus dari peta).
     */
    private void checkStale() {
        long now = System.currentTimeMillis();
        for (AgentMetrics m : metrics.values()) {
            if (m.lastSeen > 0 && (now - m.lastSeen) > staleMs) {
                if (!m.stale) {
                    m.stale = true;
                    log.warn("Telemetri stale: " + m.agentId);
                }
            }
        }
    }

    /**
     * Mendaftarkan Inspector untuk menerima fan-out.
     *
     * @param key     kunci (username Inspector)
     * @param addr    alamat IP Inspector (dari soket TCP)
     * @param udpPort port UDP lokal Inspector
     */
    public void subscribe(String key, InetAddress addr, int udpPort) {
        subscribers.put(key, new InspectorTarget(addr, udpPort));
        log.info("Telemetry SUBSCRIBE: " + key + " <- " + addr.getHostAddress() + ":" + udpPort);
    }

    /**
     * Membatalkan langganan Inspector.
     *
     * @param key kunci (username Inspector)
     */
    public void unsubscribe(String key) {
        if (subscribers.remove(key) != null) {
            log.info("Telemetry UNSUBSCRIBE: " + key);
        }
    }

    /**
     * Mengembalikan peta metrik (akses thread-safe via ConcurrentHashMap).
     *
     * @return peta metrik per Agent
     */
    public Map<String, AgentMetrics> getMetrics() {
        return metrics;
    }

    /**
     * Port UDP yang dipakai.
     *
     * @return nomor port
     */
    public int getUdpPort() {
        return udpPort;
    }
}
