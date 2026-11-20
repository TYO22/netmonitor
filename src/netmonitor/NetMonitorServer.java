package netmonitor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Server NetMonitor: ServerSocket multi-threaded (Pertemuan 6).
 *
 * Jalankan: java -cp out netmonitor.NetMonitorServer [port] [berkas-log]
 */
public class NetMonitorServer {

    private static final int MAX_CLIENTS = 50;
    // Heartbeat: server mengirim HB tiap HB_INTERVAL_S; klien yang diam > HB_TIMEOUT_S dianggap putus.
    private static final long HB_INTERVAL_S = Long.getLong("netmonitor.heartbeat.interval", 10L);
    private static final long HB_TIMEOUT_S = Long.getLong("netmonitor.heartbeat.timeout", 30L);
    private static final long INSPECTOR_IDLE_S = Long.getLong("netmonitor.inspector.idle", 600L);

    private final int port;
    private final FileLogger log;
    private final AuthService auth;
    private final GeoIpService geo;
    // Sesi login aktif: username -> handler (thread-safe)
    private final ConcurrentHashMap<String, ClientHandler> sessions = new ConcurrentHashMap<>();
    private final Set<ClientHandler> handlers = ConcurrentHashMap.newKeySet();

    // Pilar 1 (Final): klien dilayani oleh THREAD POOL, bukan satu Thread baru per koneksi.
    // Ukuran pool sama dengan MAX_CLIENTS karena satu ClientHandler hidup selama satu sesi (berumur panjang).
    // Koneksi ke-(MAX_CLIENTS+1) ditolak dengan 503 sebelum sempat masuk pool.
    private final AtomicInteger poolThreadSeq = new AtomicInteger();
    private final ExecutorService clientPool = Executors.newFixedThreadPool(MAX_CLIENTS,
            r -> new Thread(r, "ClientHandler-" + poolThreadSeq.incrementAndGet()));
    // Jumlah klien aktif. Atomik, sehingga "periksa lalu tambah" tidak bisa saling menyalip antar-thread.
    private final AtomicInteger activeClients = new AtomicInteger();

    private final ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "heartbeat");
        t.setDaemon(true);
        return t;
    });

    private final TelemetryService telemetry;

    private ServerSocket serverSocket;
    private volatile boolean running;

    public NetMonitorServer(int port, FileLogger log) {
        this.port = port;
        this.log = log;
        this.auth = new AuthService(System.getProperty("netmonitor.users", "users.properties"), log);
        this.geo = new GeoIpService(log);
        this.telemetry = new TelemetryService(log);
    }

    public FileLogger getLogger() {
        return log;
    }

    public AuthService getAuth() {
        return auth;
    }

    public GeoIpService getGeo() {
        return geo;
    }

    /** Layanan telemetri UDP (menerima dari Agent, fan-out ke Inspector). */
    public TelemetryService getTelemetry() {
        return telemetry;
    }

    public ConcurrentHashMap<String, ClientHandler> getSessions() {
        return sessions;
    }

    /** Dipanggil handler (tepat sekali) saat sesinya berakhir: bebaskan satu slot. */
    void onHandlerFinished(ClientHandler h) {
        if (handlers.remove(h)) {
            activeClients.decrementAndGet();
        }
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);               // SO_REUSEADDR
        serverSocket.bind(new InetSocketAddress(port));
        running = true;
        log.info("Server NetMonitor aktif di port " + port + " (thread pool " + MAX_CLIENTS + " klien, heartbeat "
                + HB_INTERVAL_S + " dtk, batas diam " + HB_TIMEOUT_S + " dtk)");
        heartbeat.scheduleWithFixedDelay(this::checkSessions, HB_INTERVAL_S, HB_INTERVAL_S, TimeUnit.SECONDS);
        telemetry.start();

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "ShutdownHook"));

        while (running) {
            try {
                Socket client = serverSocket.accept();
                // atomik: naikkan penghitung dulu, lalu tolak bila melewati batas
                if (activeClients.incrementAndGet() > MAX_CLIENTS) {
                    activeClients.decrementAndGet();
                    reject(client);
                    continue;
                }
                ClientHandler handler = new ClientHandler(client, this);
                handlers.add(handler);
                try {
                    clientPool.execute(handler);          // ClientHandler adalah Runnable
                } catch (RejectedExecutionException e) {  // pool sudah dimatikan (server berhenti)
                    onHandlerFinished(handler);
                    try {
                        client.close();
                    } catch (IOException ignored) {
                        // abaikan
                    }
                }
            } catch (SocketException e) {
                if (running) {
                    log.error("accept() gagal: " + e.getMessage());
                    pause();
                }
            } catch (IOException e) {
                log.error("accept() gagal: " + e.getMessage());
                pause();
            }
        }
    }

    /** Tugas berkala: kirim heartbeat ke semua sesi dan buang sesi yang tidak lagi menjawab. */
    private void checkSessions() {
        try {
            for (ClientHandler h : sessions.values()) {
                h.heartbeat(HB_TIMEOUT_S * 1000L, INSPECTOR_IDLE_S * 1000L);
            }
        } catch (RuntimeException e) {
            log.error("Pemeriksaan heartbeat gagal: " + e);   // jangan sampai tugas berkala berhenti
        }
    }

    private void reject(Socket client) {
        log.warn("Koneksi ditolak (server penuh): " + client.getRemoteSocketAddress());
        try (Socket s = client;
             BufferedWriter w = new BufferedWriter(
                     new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8))) {
            Protocol.writeLine(w, Protocol.err(503, "Server Penuh"));
        } catch (IOException ignored) {
            // klien sudah pergi
        }
    }

    private void pause() {
        try {
            Thread.sleep(100);   // cegah loop sibuk bila accept() gagal terus-menerus
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        log.info("Server dihentikan, menutup " + handlers.size() + " sesi");
        try {
            serverSocket.close();
        } catch (IOException ignored) {
            // abaikan
        }
        heartbeat.shutdownNow();
        telemetry.stop();
        // beri tahu klien alasan koneksi ditutup, bukan sekadar memutusnya
        for (ClientHandler h : handlers) {
            h.notifyAndClose(Protocol.err(503, "Server Dimatikan"));
        }
        clientPool.shutdownNow();          // hentikan thread pool klien
        pause();
        log.close();
    }

    public static void main(String[] args) {
        int port = Protocol.DEFAULT_PORT;
        String logFile = "netmonitor.log";
        try {
            if (args.length >= 1) {
                port = Integer.parseInt(args[0]);
                if (port < 1 || port > 65535) {
                    throw new NumberFormatException();
                }
            }
            if (args.length >= 2) {
                logFile = args[1];
            }
        } catch (NumberFormatException e) {
            System.err.println("Port tidak valid. Gunakan angka 1-65535.");
            System.exit(1);
        }
        FileLogger logger;
        try {
            logger = new FileLogger(logFile, true);
        } catch (IOException e) {
            System.err.println("Tidak dapat membuka berkas log '" + logFile + "': " + e.getMessage()
                    + ". Periksa izin folder atau gunakan nama berkas lain.");
            System.exit(1);
            return;
        }
        try {
            new NetMonitorServer(port, logger).start();
        } catch (BindException e) {
            String msg = "Server gagal berjalan: port " + port + " tidak dapat dipakai (" + e.getMessage() + "). "
                    + "Kemungkinan server NetMonitor lain masih berjalan di port itu, atau port dipakai program lain. "
                    + "Hentikan program tersebut atau jalankan dengan port lain.";
            logger.error(msg);
            System.err.println(msg);
            System.exit(1);
        } catch (IOException e) {
            logger.error("Server gagal berjalan: " + e.getMessage());
            System.err.println("Server gagal berjalan: " + e.getMessage());
            System.exit(1);
        }
    }
}
