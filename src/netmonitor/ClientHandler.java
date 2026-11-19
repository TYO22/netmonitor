package netmonitor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Satu ClientHandler melayani satu koneksi klien (Pertemuan 6) dan dijalankan oleh thread pool server (Pilar 1 Final).
 * Memproses perintah protokol: LOGIN, LIST, SYSINFO, PING, RESOLVE, QUIT/EXIT.
 *
 * Penanganan kesalahan: setiap kegagalan dijawab dengan kode dan pesan yang menyebut PENYEBAB sebenarnya
 * (mis. internet putus, DNS tidak merespons, Agent terputus), bukan pesan umum yang menyesatkan.
 */
public class ClientHandler implements Runnable {

    private static final int PRELOGIN_TIMEOUT_MS = 30_000;       // batas menunggu LOGIN
    private static final int MAX_LOGIN_ATTEMPTS = 3;
    private static final int AGENT_REPLY_TIMEOUT_MS = 5_000;
    private static final int PING_TIMEOUT_MS = 3_000;
    private static final int DNS_TIMEOUT_MS = 4_000;

    private final Socket socket;
    private final NetMonitorServer server;
    private final FileLogger log;
    private final String remote;
    private final Object writeLock = new Object();
    private final Map<String, CompletableFuture<String>> pending = new ConcurrentHashMap<>();
    private final AtomicLong requestSeq = new AtomicLong();

    private BufferedReader in;
    private BufferedWriter out;
    private volatile String username;
    private volatile String role;
    private volatile boolean closing;
    private volatile long lastReceived = System.currentTimeMillis();   // data apa pun dari klien (termasuk HBACK)
    private volatile long lastActivity = System.currentTimeMillis();   // perintah nyata dari klien
    private int failedLogins;
    private volatile GeoIpService.Result geo;   // lokasi node (Pilar 4: pemetaan node), tampil pada LIST

    /** Kegagalan permintaan ke Agent, lengkap dengan kode respons untuk Inspector. */
    private static final class AgentException extends Exception {
        private static final long serialVersionUID = 1L;
        final int code;

        AgentException(int code, String message) {
            super(message);
            this.code = code;
        }
    }

    public ClientHandler(Socket socket, NetMonitorServer server) {
        this.socket = socket;
        this.server = server;
        this.log = server.getLogger();
        this.remote = String.valueOf(socket.getRemoteSocketAddress());
    }

    public String getUsername() {
        return username;
    }

    public boolean isAgent() {
        return Protocol.ROLE_AGENT.equals(role);
    }

    private boolean isInspector() {
        return Protocol.ROLE_INSPECTOR.equals(role);
    }

    // ------------------------------------------------------------------ siklus hidup

    @Override
    public void run() {
        log.info("Koneksi masuk dari " + remote);
        try {
            socket.setSoTimeout(PRELOGIN_TIMEOUT_MS);
            socket.setKeepAlive(true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));

            String line;
            while ((line = Protocol.readLine(in)) != null) {
                lastReceived = System.currentTimeMillis();
                if (!handleLine(line)) {
                    return;
                }
            }
            if (!closing) {
                log.warn("Klien terputus tanpa QUIT: koneksi ditutup dari sisi klien (" + describe() + ")");
            }
        } catch (SocketTimeoutException e) {
            if (username == null) {
                log.warn("Timeout: belum login dalam " + (PRELOGIN_TIMEOUT_MS / 1000) + " detik (" + describe() + ")");
                trySend(Protocol.err(408, "Sesi Timeout: belum login dalam " + (PRELOGIN_TIMEOUT_MS / 1000) + " detik"));
            } else {
                log.warn("Timeout, sesi diputus (" + describe() + ")");
                trySend(Protocol.err(408, "Sesi Timeout"));
            }
        } catch (Protocol.LineTooLongException e) {
            log.warn("Baris terlalu panjang dari " + describe());
            trySend(Protocol.err(400, "Baris Terlalu Panjang (maksimal " + Protocol.MAX_LINE_LENGTH + " karakter)"));
        } catch (IOException e) {
            if (!closing) {
                log.warn("Koneksi terputus (" + describe() + "): " + describeIo(e));
            }
        } catch (RuntimeException e) {
            // jaring pengaman terakhir: satu klien bermasalah tidak boleh menjatuhkan server
            log.error("Kesalahan tak terduga (" + describe() + "): " + e);
            trySend(Protocol.err(500, "Kesalahan Server"));
        } finally {
            cleanup();
        }
    }

    private void cleanup() {
        String user = username;
        if (user != null) {
            server.getSessions().remove(user, this);
        }
        for (CompletableFuture<String> f : pending.values()) {
            f.cancel(true);
        }
        try {
            socket.shutdownOutput();
        } catch (IOException ignored) {
            // socket mungkin sudah tertutup
        }
        closeSocket();
        log.info("Sesi berakhir (" + describe() + ")");
        server.onHandlerFinished(this);
    }

    private void closeSocket() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // abaikan
        }
    }

    /** Memberi tahu klien alasan penutupan, lalu memutus koneksi (dipakai saat server dimatikan, dsb). */
    public void notifyAndClose(String message) {
        closing = true;
        trySend(message);
        closeSocket();
    }

    private void dropDeadPeer(String why) {
        log.warn("Koneksi dianggap putus (" + describe() + "): " + why + ". Sesi dibersihkan");
        closing = true;
        closeSocket();
    }

    /**
     * Dipanggil berkala oleh server: (1) mengirim HB lalu memeriksa apakah klien masih menjawab, sehingga
     * koneksi yang putus diam-diam (Wi-Fi mati, laptop tidur, kabel dicabut) cepat terdeteksi dan sesinya
     * dibersihkan; (2) menutup sesi Inspector yang tidak aktif terlalu lama.
     */
    void heartbeat(long timeoutMs, long inspectorIdleMs) {
        if (username == null || closing) {
            return;
        }
        long now = System.currentTimeMillis();
        long silent = now - lastReceived;
        if (silent > timeoutMs) {
            dropDeadPeer("tidak ada sinyal dari klien selama " + (silent / 1000) + " detik");
            return;
        }
        if (isInspector() && now - lastActivity > inspectorIdleMs) {
            log.warn("Sesi Inspector tidak aktif " + (inspectorIdleMs / 1000) + " detik, diputus (" + describe() + ")");
            notifyAndClose(Protocol.err(408, "Sesi Timeout: tidak ada aktivitas selama "
                    + (inspectorIdleMs / 1000) + " detik"));
            return;
        }
        try {
            send(Protocol.HEARTBEAT);
        } catch (IOException e) {
            dropDeadPeer("gagal mengirim sinyal: " + describeIo(e));
        }
    }

    private String describe() {
        return username == null ? remote : username + "/" + role + "@" + remote;
    }

    /** Mengubah exception jaringan menjadi kalimat penyebab yang mudah dibaca (untuk log). */
    static String describeIo(IOException e) {
        String m = String.valueOf(e.getMessage()).toLowerCase();
        if (m.contains("connection reset") || m.contains("forcibly closed")) {
            return "koneksi direset klien (klien mati mendadak atau jaringan putus)";
        }
        if (m.contains("broken pipe") || m.contains("connection abort") || m.contains("software caused")) {
            return "koneksi terputus saat mengirim data";
        }
        if (m.contains("socket closed")) {
            return "koneksi ditutup";
        }
        return e.getClass().getSimpleName() + ": " + e.getMessage();
    }

    // ------------------------------------------------------------------ pengiriman

    private void send(String line) throws IOException {
        synchronized (writeLock) {
            Protocol.writeLine(out, line);
        }
        if (!Protocol.HEARTBEAT.equals(line)) {
            log.info((line.startsWith("REQ ") ? "KIRIM " : "RESP ") + describe() + " <- " + Protocol.clean(line, 120));
        }
    }

    private void trySend(String line) {
        try {
            if (out != null) {
                send(line);
            }
        } catch (IOException ignored) {
            // klien sudah pergi
        }
    }

    // ------------------------------------------------------------------ dispatcher

    /** @return false bila sesi harus diakhiri. */
    private boolean handleLine(String raw) throws IOException {
        String line = raw.trim();
        if (line.isEmpty() || line.equals(Protocol.HEARTBEAT_ACK)) {
            return true;      // jawaban heartbeat: cukup memperbarui lastReceived
        }
        lastActivity = System.currentTimeMillis();
        // Jawaban Agent atas permintaan server: "RES <id> <data>"
        if (isAgent() && line.startsWith("RES ")) {
            handleAgentResponse(line);
            return true;
        }
        String[] p = line.split("\\s+");
        String cmd = p[0].toUpperCase();
        log.info("CMD " + describe() + " -> " + (cmd.equals("LOGIN") ? "LOGIN ***" : Protocol.clean(line, 120)));

        switch (cmd) {
            case "LOGIN":
                return doLogin(p);
            case "QUIT":
            case "EXIT":
                send(Protocol.ok("BYE Sesi Berhasil Ditutup"));
                return false;
            case "LIST":
                doList();
                return true;
            case "SYSINFO":
                doSysinfo(p);
                return true;
            case "PING":
                doPing(p);
                return true;
            case "RESOLVE":
                doResolve(p);
                return true;
            default:
                send(Protocol.err(400, "Perintah Tidak Dikenal"));
                return true;
        }
    }

    private boolean doLogin(String[] p) throws IOException {
        if (username != null) {
            send(Protocol.err(400, "Sudah Login"));
            return true;
        }
        if (p.length != 4) {
            send(Protocol.err(400, "Format: LOGIN <peran> <username> <password>"));
            return true;
        }
        String r = p[1].toUpperCase();
        String u = p[2];
        String pw = p[3];
        if (!Protocol.isValidRole(r) || !Protocol.isValidUsername(u)) {
            send(Protocol.err(400, "Peran atau Username Tidak Valid"));
            return true;
        }
        if (!server.getAuth().authenticate(u, pw, r)) {
            failedLogins++;
            log.warn("Login gagal user=" + Protocol.clean(u, 32) + " dari " + remote
                    + " (percobaan " + failedLogins + ")");
            if (failedLogins >= MAX_LOGIN_ATTEMPTS) {
                send(Protocol.err(401, "Kredensial Salah, Terlalu Banyak Percobaan"));
                return false;
            }
            send(Protocol.err(401, "Kredensial Salah"));
            return true;
        }
        if (server.getSessions().containsKey(u)) {
            send(Protocol.err(400, "Username Sudah Ada (sedang dipakai sesi lain; jika sesi lama sebenarnya sudah putus, "
                    + "server membersihkannya dalam beberapa detik)"));
            return true;
        }

        // Web API GeoIP (Pertemuan 4) - dilakukan SEBELUM sesi didaftarkan.
        // Kegagalan API tidak menggagalkan login; alasannya ikut dilaporkan pada teks lokasi.
        GeoIpService.Result geoResult = server.getGeo().lookup(socket.getInetAddress());
        String lokasi = geoResult.getDisplay();
        this.geo = geoResult;

        // Daftarkan sesi dan kirim respons dalam satu blok writeLock supaya
        // permintaan server (REQ) ke Agent tidak mendahului respons LOGIN.
        synchronized (writeLock) {
            if (server.getSessions().putIfAbsent(u, this) != null) {
                send(Protocol.err(400, "Username Sudah Ada"));
                return true;
            }
            username = u;
            role = r;
            send(Protocol.ok("OK Login Berhasil, lokasi: " + lokasi));
        }
        socket.setSoTimeout(0);   // setelah login, kehidupan koneksi dipantau lewat heartbeat
        lastActivity = System.currentTimeMillis();
        log.info("LOGIN berhasil user=" + u + " role=" + r + " ip="
                + socket.getInetAddress().getHostAddress() + " lokasi=" + lokasi);
        return true;
    }

    private boolean requireInspector() throws IOException {
        if (username == null) {
            send(Protocol.err(401, "Belum Login"));
            return false;
        }
        if (!isInspector()) {
            send(Protocol.err(403, "Akses Ditolak (khusus INSPECTOR)"));
            return false;
        }
        return true;
    }

    /**
     * LIST (Pilar 2): daftar semua pengguna aktif, dapat dipakai SEMUA peran yang sudah login.
     * Setiap entri berformat "username | PERAN | lokasi | isp" dan dipisah " ; " (tetap satu baris respons).
     */
    private void doList() throws IOException {
        if (username == null) {
            send(Protocol.err(401, "Belum Login"));
            return;
        }
        List<String> users = server.getSessions().values().stream()
                .sorted(Comparator.comparing(ClientHandler::getUsername))
                .map(ClientHandler::describeForList)
                .collect(Collectors.toList());
        send(Protocol.ok("USERS [" + String.join(" ; ", users) + "]"));
    }

    /** Entri satu pengguna untuk LIST; karakter pemisah dibersihkan agar format tidak rusak. */
    private String describeForList() {
        GeoIpService.Result g = geo;
        String loc = g == null ? "-" : listField(g.getNode());
        String isp = g == null || g.getIsp().isEmpty() ? "-" : listField(g.getIsp());
        return username + " | " + role + " | " + loc + " | " + isp;
    }

    private static String listField(String value) {
        String v = Protocol.clean(value.replaceAll("[|;\\[\\]]", " "), 80);
        return v.isEmpty() ? "-" : v;
    }

    private void doSysinfo(String[] p) throws IOException {
        if (!requireInspector()) {
            return;
        }
        if (p.length != 2) {
            send(Protocol.err(400, "Format: SYSINFO <agentId>"));
            return;
        }
        ClientHandler agent = server.getSessions().get(p[1]);
        if (agent == null || !agent.isAgent()) {
            send(Protocol.err(404, "Agent Tidak Ditemukan (tidak terhubung; periksa daftar dengan LIST)"));
            return;
        }
        try {
            String data = agent.requestSysinfo(AGENT_REPLY_TIMEOUT_MS);
            send(Protocol.ok("SYSINFO " + p[1] + " " + data));
        } catch (AgentException e) {
            send(Protocol.err(e.code, e.getMessage()));
        }
    }

    private void doPing(String[] p) throws IOException {
        if (!requireInspector()) {
            return;
        }
        if (p.length != 2 || !NetUtil.isValidFormat(p[1])) {
            send(Protocol.err(400, "Format Host Tidak Valid"));
            return;
        }
        try {
            NetUtil.PingResult r = NetUtil.ping(p[1], PING_TIMEOUT_MS, DNS_TIMEOUT_MS);
            if (r.reachable) {
                send(Protocol.ok("PING " + p[1] + " REACHABLE " + r.millis + "ms"));
            } else if (!NetUtil.isLocal(r.address) && !NetUtil.isInternetAvailable()) {
                send(Protocol.err(503, "Tidak Ada Koneksi Internet di Server, Host Luar Tidak Dapat Dijangkau"));
            } else {
                send(Protocol.err(408, "Host Tidak Terjangkau (tidak membalas dalam "
                        + (PING_TIMEOUT_MS / 1000) + " detik; bisa juga diblokir firewall)"));
            }
        } catch (NetUtil.DnsTimeoutException e) {
            send(Protocol.err(408, "DNS Tidak Merespons (waktu habis " + (DNS_TIMEOUT_MS / 1000) + " detik)"));
        } catch (UnknownHostException e) {
            send(dnsFailure());
        } catch (IOException e) {
            log.warn("PING gagal (" + describe() + "): " + describeIo(e));
            send(Protocol.err(503, "Jaringan Server Tidak Tersedia (" + Protocol.clean(e.getMessage(), 60) + ")"));
        }
    }

    private void doResolve(String[] p) throws IOException {
        if (!requireInspector()) {
            return;
        }
        if (p.length != 2 || !NetUtil.isValidFormat(p[1])) {
            send(Protocol.err(400, "Format Host Tidak Valid"));
            return;
        }
        try {
            InetAddress a = NetUtil.resolveWithTimeout(p[1], DNS_TIMEOUT_MS);
            send(Protocol.ok("RESOLVE " + p[1] + " " + a.getHostAddress()));
        } catch (NetUtil.DnsTimeoutException e) {
            send(Protocol.err(408, "DNS Tidak Merespons (waktu habis " + (DNS_TIMEOUT_MS / 1000) + " detik)"));
        } catch (UnknownHostException e) {
            send(dnsFailure());
        } catch (IOException e) {
            log.warn("RESOLVE gagal (" + describe() + "): " + describeIo(e));
            send(Protocol.err(503, "Resolusi DNS Gagal (" + Protocol.clean(e.getMessage(), 60) + ")"));
        }
    }

    /** Membedakan "domain memang tidak ada" dari "server sedang tidak punya internet/DNS". */
    private static String dnsFailure() {
        if (NetUtil.isInternetAvailable()) {
            return Protocol.err(404, "Domain Tidak Dikenal");
        }
        return Protocol.err(503, "Tidak Ada Koneksi Internet atau DNS Tidak Tersedia di Server");
    }

    // ------------------------------------------------------------------ komunikasi server -> Agent

    /**
     * Dipanggil dari thread Inspector: meminta info sistem ke Agent ini.
     * Server mengirim "REQ <id> SYSINFO", Agent membalas "RES <id> <data>".
     */
    private String requestSysinfo(long timeoutMs) throws AgentException {
        String id = Long.toString(requestSeq.incrementAndGet());
        CompletableFuture<String> future = new CompletableFuture<>();
        pending.put(id, future);
        try {
            send("REQ " + id + " SYSINFO");
            String reply = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (reply.startsWith("ERR")) {
                throw new AgentException(500, "Agent Gagal Mengumpulkan Data (" + Protocol.clean(reply, 60) + ")");
            }
            return reply;
        } catch (TimeoutException e) {
            throw new AgentException(408, "Agent Tidak Merespons (waktu habis " + (timeoutMs / 1000)
                    + " detik; jaringan Agent mungkin putus)");
        } catch (CancellationException e) {
            throw new AgentException(502, "Agent Terputus Saat Menunggu Respons");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AgentException(500, "Permintaan Dibatalkan");
        } catch (ExecutionException e) {
            throw new AgentException(500, "Kesalahan Saat Menunggu Agent");
        } catch (IOException e) {
            throw new AgentException(502, "Agent Terputus (gagal mengirim permintaan: " + describeIo(e) + ")");
        } finally {
            pending.remove(id);
        }
    }

    private void handleAgentResponse(String line) {
        String[] parts = line.split("\\s+", 3);
        CompletableFuture<String> f = parts.length >= 2 ? pending.get(parts[1]) : null;
        if (f == null) {
            log.warn("RES tak dikenal dari " + describe());
            return;
        }
        f.complete(parts.length == 3 ? Protocol.clean(parts[2], 512) : "");
    }
}
