package netmonitor.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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
import netmonitor.common.Protocol;
import netmonitor.util.FileLogger;
import netmonitor.util.NetUtil;

/**
 * Satu ClientHandler melayani satu koneksi klien (Pertemuan 6) dan dijalankan oleh thread pool server (Pilar 1 Final).
 * Memproses perintah protokol: LOGIN, LIST, SYSINFO, PING, RESOLVE, QUIT/EXIT.
 *
 * Penanganan kesalahan: setiap kegagalan dijawab dengan kode dan pesan yang menyebut PENYEBAB sebenarnya
 * (mis. internet putus, DNS tidak merespons, Agent terputus), bukan pesan umum yang menyesatkan.
 */
public class ClientHandler implements Runnable {

    /** Batas waktu menunggu LOGIN sebelum sesi diputus (ms). */
    private static final int PRELOGIN_TIMEOUT_MS = 30_000;       // batas menunggu LOGIN
    /** Jumlah maksimal percobaan login gagal sebelum sesi diputus. */
    private static final int MAX_LOGIN_ATTEMPTS = 3;
    /** Batas tunggu balasan Agent atas permintaan server (ms). */
    private static final int AGENT_REPLY_TIMEOUT_MS = 5_000;
    /** Batas waktu ping host (ms). */
    private static final int PING_TIMEOUT_MS = 3_000;
    /** Batas waktu resolusi DNS (ms). */
    private static final int DNS_TIMEOUT_MS = 4_000;
    /** Panjang maksimum teks BROADCAST. */
    private static final int MAX_BROADCAST_TEXT_LENGTH = 500;   // panjang maksimal teks BROADCAST
    /** Panjang maksimum teks MSG. */
    private static final int MAX_MSG_TEXT_LENGTH = 500;        // panjang maksimal teks MSG
    /** Batas waktu keseluruhan transfer berkas di kanal data (ms). */
    private static final long TRANSFER_TIMEOUT_MS = Long.getLong("netmonitor.transfer.timeout", 300_000L);

    /** Soket koneksi klien. */
    private final Socket socket;
    /** Server yang memiliki handler ini. */
    private final NetMonitorServer server;
    /** Pencatat log server. */
    private final FileLogger log;
    /** Alamat remote klien untuk keperluan log. */
    private final String remote;
    /** Kunci penulisan: menjamin urutan baris respons tidak tercampur. */
    private final Object writeLock = new Object();
    /** Permintaan ke Agent yang sedang menunggu balasan (id -> future). */
    private final Map<String, CompletableFuture<String>> pending = new ConcurrentHashMap<>();
    /** Penghasil id permintaan yang unik dan monoton naik. */
    private final AtomicLong requestSeq = new AtomicLong();

    /** Pembaca baris masuk dari klien. */
    private BufferedReader in;
    /** Penulis baris keluar ke klien. */
    private BufferedWriter out;
    /** Username klien yang sedang login (null bila belum login). */
    private volatile String username;
    /** Peran klien (AGENT/INSPECTOR). */
    private volatile String role;
    /** Flag apakah sesi sedang/sudah ditutup. */
    private volatile boolean closing;
    /** Waktu data terakhir dari klien (termasuk HBACK). */
    private volatile long lastReceived = System.currentTimeMillis();   // data apa pun dari klien (termasuk HBACK)
    /** Waktu perintah nyata terakhir dari klien. */
    private volatile long lastActivity = System.currentTimeMillis();   // perintah nyata dari klien
    /** Flag sedang menunggu transfer berkas di kanal data. */
    private volatile boolean transferring;   // menunggu transfer berkas di kanal data
    /** Jumlah percobaan login gagal beruntun. */
    private int failedLogins;
    /** Hasil GeoIP klien, tampil pada perintah LIST. */
    private volatile GeoIpService.Result geo;   // lokasi node (Pilar 4: pemetaan node), tampil pada LIST

    /** Kegagalan permintaan ke Agent, lengkap dengan kode respons untuk Inspector. */
    private static final class AgentException extends Exception {
        private static final long serialVersionUID = 1L;
        /** Kode respons yang akan dikirimkan ke Inspector. */
        final int code;

        /**
         * @param code    kode respons
         * @param message pesan kegagalan
         */
        AgentException(int code, String message) {
            super(message);
            this.code = code;
        }
    }

    /**
     * Membuat handler untuk satu koneksi klien.
     *
     * @param socket soket koneksi klien
     * @param server server yang menjalankan handler ini
     */
    public ClientHandler(Socket socket, NetMonitorServer server) {
        this.socket = socket;
        this.server = server;
        this.log = server.getLogger();
        this.remote = String.valueOf(socket.getRemoteSocketAddress());
    }

    /** @return username klien yang sedang login, atau null. */
    public String getUsername() {
        return username;
    }

    /** @return true bila klien berperan AGENT. */
    public boolean isAgent() {
        return Protocol.ROLE_AGENT.equals(role);
    }

    /** @return true bila klien berperan INSPECTOR. */
    private boolean isInspector() {
        return Protocol.ROLE_INSPECTOR.equals(role);
    }

    // ------------------------------------------------------------------ siklus hidup

    /** 
     * Siklus hidup handler: membaca perintah dari klien hingga koneksi terputus atau QUIT.
     * Setiap kegagalan dihandle dengan kode respons yang mencantumkan penyebabnya.
     * Jalankan oleh thread pool server (tidak terbentuk per-thread untuk koneksi).
     */
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

    /** Melepaskan sumber daya sesi: hapus dari peta sesi, batalkan permintaan tertunda, tutup soket. */
    private void cleanup() {
        String user = username;
        if (user != null) {
            server.getSessions().remove(user, this);
            server.getTelemetry().unsubscribe(user);
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

    /** Menutup soket dengan aman, mengabaikan IOException. */
    private void closeSocket() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // abaikan
        }
    }

    /**
     * Memberi tahu klien alasan penutupan, lalu memutus koneksi (dipakai saat server dimatikan, dsb).
     *
     * @param message pesan yang akan dikirim ke klien
     */
    public void notifyAndClose(String message) {
        closing = true;
        trySend(message);
        closeSocket();
    }

    /** Menandai klien mati dan menutup soket dengan alasan yang diberikan. */
    private void dropDeadPeer(String why) {
        log.warn("Koneksi dianggap putus (" + describe() + "): " + why + ". Sesi dibersihkan");
        closing = true;
        closeSocket();
    }

    /**
     * Dipanggil berkala oleh server: (1) mengirim HB lalu memeriksa apakah klien masih menjawab, sehingga
     * koneksi yang putus diam-diam (Wi-Fi mati, laptop tidur, kabel dicabut) cepat terdeteksi dan sesinya
     * dibersihkan; (2) menutup sesi Inspector yang tidak aktif terlalu lama.
     *
     * @param timeoutMs       batas waktu no-signal untuk dianggap putus (ms)
     * @param inspectorIdleMs batas waktu aktivitas tidak terhadap Inspector (ms)
     */
    void heartbeat(long timeoutMs, long inspectorIdleMs) {
        if (username == null || closing) {
            return;
        }
        if (transferring) {
            // Selama transfer berkas berlangsung, klien tidak mengirim baris perintah;
            // tetap kirim sinyal hidup tetapi jangan dianggap putus hanya karena diam.
            try {
                send(Protocol.HEARTBEAT);
            } catch (IOException e) {
                dropDeadPeer("gagal mengirim sinyal: " + describeIo(e));
            }
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

    /** @return deskripsi singkat sesi untuk keperluan log. */
    private String describe() {
        return username == null ? remote : username + "/" + role + "@" + remote;
    }

    /**
     * Mengubah exception jaringan menjadi kalimat penyebab yang mudah dibaca (untuk log).
     *
     * @param e exception jaringan
     * @return deskripsi penyebab kegagalan
     */
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

    /**
     * Mengirim satu baris ke klien dengan penguncian penulisan.
     *
     * @param line baris yang akan dikirim
     * @throws IOException bila terjadi kesalahan I/O
     */
    private void send(String line) throws IOException {
        synchronized (writeLock) {
            Protocol.writeLine(out, line);
        }
        if (!Protocol.HEARTBEAT.equals(line)) {
            log.info((line.startsWith("REQ ") ? "KIRIM " : "RESP ") + describe() + " <- " + Protocol.clean(line, 120));
        }
    }

    /**
     * Mencoba mengirim baris ke klien, menelan IOException bila klien sudah pergi.
     *
     * @param line baris yang akan dikirim
     */
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

    /**
     * Memproses satu baris perintah dari klien.
     *
     * @param raw baris mentah dari klien
     * @return {@code false} bila sesi harus diakhiri, {@code true} bila sesi berlanjut
     * @throws IOException bila terjadi kesalahan I/O saat menulis respons
     */
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
            case "BROADCAST":
                doBroadcast(p);
                return true;
            case "MSG":
                doMsg(p);
                return true;
            case "EXEC":
                doExec(p);
                return true;
            case "SUBSCRIBE":
                doSubscribe(p);
                return true;
            case "UNSUBSCRIBE":
                doUnsubscribe();
                return true;
            case "WATCH":
                doWatch();
                return true;
            case "UPLOAD":
                return doUpload(p);
            case "PUSH":
                return doPush(p);
            default:
                send(Protocol.err(400, "Perintah Tidak Dikenal"));
                return true;
        }
    }

    /**
     * Memproses perintah LOGIN: validasi kredensial, lookup GeoIP, daftarkan sesi.
     *
     * @param p token perintah, p[1] peran, p[2] username, p[3] password
     * @return {@code true} bila sesi berlanjut, {@code false} bila sesi ditutup (login gagal total)
     * @throws IOException bila terjadi kesalahan I/O saat menulis respons
     */
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

    /**
     * Memeriksa apakah klien sudah login dan berperan INSPECTOR.
     *
     * @return {@code true} bila klien INSPECTOR yang terautentikasi
     * @throws IOException bila terjadi kesalahan I/O saat menulis respons
     */
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
     *
     * @throws IOException bila terjadi kesalahan I/O saat menulis respons
     */
    private void doList() throws IOException {
        if (!requireInspector()) {
            return;
        }
        List<String> users = server.getSessions().values().stream()
                .sorted(Comparator.comparing(ClientHandler::getUsername))
                .map(ClientHandler::describeForList)
                .collect(Collectors.toList());
        send(Protocol.ok("USERS [" + String.join(" ; ", users) + "]"));
    }

    /**
     * Entri satu pengguna untuk LIST; karakter pemisah dibersihkan agar format tidak rusak.
     *
     * @return string deskripsi pengguna untuk LIST
     */
    private String describeForList() {
        GeoIpService.Result g = geo;
        String loc = g == null ? "-" : listField(g.getNode());
        String isp = g == null || g.getIsp().isEmpty() ? "-" : listField(g.getIsp());
        return username + " | " + role + " | " + loc + " | " + isp;
    }

    /**
     * Membersihkan sebuah kolom untuk perintah LIST.
     *
     * @param value nilai kolom mentah
     * @return nilai aman maksimal 80 karakter, atau "-" bila kosong
     */
    private static String listField(String value) {
        String v = Protocol.clean(value.replaceAll("[|;\\[\\]]", " "), 80);
        return v.isEmpty() ? "-" : v;
    }

    /**
     * Memproses perintah SYSINFO: meminta data sistem dari Agent tujuan.
     *
     * @param p token perintah, p[1] agentId
     * @throws IOException bila terjadi kesalahan I/O saat menulis respons
     */
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

    /**
     * Memproses perintah PING: menguji jangkauan host dari sisi server.
     *
     * @param p token perintah, p[1] host tujuan
     * @throws IOException bila terjadi kesalahan I/O saat menulis respons
     */
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

    /**
     * Memproses perintah RESOLVE: resolusi DNS atas host yang diminta.
     *
     * @param p token perintah, p[1] host/domain
     * @throws IOException bila terjadi kesalahan I/O saat menulis respons
     */
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
     * Server mengirim "REQ &lt;id&gt; SYSINFO", Agent membalas "RES &lt;id&gt; &lt;data&gt;".
     *
     * @param timeoutMs batas waktu menunggu balasan dari Agent (ms)
     * @return data sistem yang dilaporkan Agent
     * @throws AgentException bila Agent gagal, timeout, atau terputus
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

    /**
     * Meminta Agent menjalankan perintah EXEC (dipanggil dari thread Inspector).
     * Server mengirim "REQ <id> EXEC <namaPerintah>", Agent membalas "RES <id> <data>".
     *
     * @param command   nama perintah yang sudah divalidasi whitelist
     * @param timeoutMs batas waktu tunggu balasan Agent
     * @return hasil eksekusi dari Agent
     * @throws AgentException jika Agent gagal, timeout, atau terputus
     */
    private String requestExec(String command, long timeoutMs) throws AgentException {
        String id = Long.toString(requestSeq.incrementAndGet());
        CompletableFuture<String> future = new CompletableFuture<>();
        pending.put(id, future);
        try {
            send("REQ " + id + " EXEC " + command);
            String reply = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (reply.startsWith("ERR")) {
                throw new AgentException(500, "Agent Gagal Menjalankan Perintah (" + Protocol.clean(reply, 60) + ")");
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

    /**
     * Memproses balasan Agent (RES &lt;id&gt; &lt;data&gt;) dan menyelesaikan future yang menunggu.
     *
     * @param line baris respons mentah dari Agent
     */
    private void handleAgentResponse(String line) {
        String[] parts = line.split("\\s+", 3);
        CompletableFuture<String> f = parts.length >= 2 ? pending.get(parts[1]) : null;
        if (f == null) {
            log.warn("RES tak dikenal dari " + describe());
            return;
        }
        f.complete(parts.length == 3 ? Protocol.clean(parts[2], 512) : "");
    }

    /**
     * Menangani perintah BROADCAST: kirim pesan ke semua klien terhubung.
     * Pengirim menerima 200 OK; tidak ada respons 404 pada broadcast.
     *
     * @param p token perintah, p[1..] adalah isi pesan
     * @throws IOException jika gagal menulis respons ke pengirim
     */
    private void doBroadcast(String[] p) throws IOException {
        if (!requireInspector()) {
            return;
        }
        String text = joinFrom(p, 1);
        if (text.length() > MAX_BROADCAST_TEXT_LENGTH) {
            send(Protocol.err(400, "Pesan Terlalu Panjang (maksimal " + MAX_BROADCAST_TEXT_LENGTH + " karakter)"));
            return;
        }
        String safeText = Protocol.clean(text, MAX_BROADCAST_TEXT_LENGTH);
        String push = "BCAST " + username + " " + safeText;
        for (ClientHandler h : server.getSessions().values()) {
            if (h != this) {
                h.trySend(push);
            }
        }
        log.info("BROADCAST dari " + username + ": " + Protocol.clean(safeText, 80));
        send(Protocol.ok("Pesan Dikirim"));
    }

    /**
     * Menangani perintah MSG: kirim pesan privat ke satu klien.
     * Diizinkan untuk peran AGENT maupun INSPECTOR (tidak memerlukan requireInspector).
     * Jika target tidak terhubung, balas 404 ERR Pengguna Tidak Terhubung.
     *
     * @param p token perintah, p[1] target username, p[2..] isi pesan
     * @throws IOException jika gagal menulis respons ke pengirim
     */
    private void doMsg(String[] p) throws IOException {
        if (username == null) {
            send(Protocol.err(401, "Belum Login"));
            return;
        }
        if (p.length < 3) {
            send(Protocol.err(400, "Format: MSG <username> <teks>"));
            return;
        }
        String target = p[1];
        String text = joinFrom(p, 2);
        if (text.length() > MAX_MSG_TEXT_LENGTH) {
            send(Protocol.err(400, "Pesan Terlalu Panjang (maksimal " + MAX_MSG_TEXT_LENGTH + " karakter)"));
            return;
        }
        ClientHandler dest = server.getSessions().get(target);
        if (dest == null) {
            send(Protocol.err(404, "Pengguna Tidak Terhubung"));
            return;
        }
        String safeText = Protocol.clean(text, MAX_MSG_TEXT_LENGTH);
        dest.trySend("MSG " + username + " " + safeText);
        log.info("MSG " + username + " -> " + target + ": " + Protocol.clean(safeText, 80));
        send(Protocol.ok("Pesan Terkirim"));
    }

    /**
     * Menangani perintah EXEC (khusus Inspector): memvalidasi whitelist dan meneruskan ke Agent.
     * Server menunggu balasan Agent paling lama 12 detik.
     *
     * @param p token perintah, p[1] agentId, p[2] nama perintah
     * @throws IOException jika gagal menulis respons
     */
    private void doExec(String[] p) throws IOException {
        if (!requireInspector()) {
            return;
        }
        if (p.length != 3) {
            send(Protocol.err(400, "Format: EXEC <agentId> <namaPerintah>"));
            return;
        }
        String agentId = p[1];
        String command = p[2].toLowerCase();
        if (!isAllowedExecCommand(command)) {
            send(Protocol.err(403, "Perintah Tidak Diizinkan (di luar whitelist)"));
            return;
        }
        ClientHandler agent = server.getSessions().get(agentId);
        if (agent == null || !agent.isAgent()) {
            send(Protocol.err(404, "Agent Tidak Ditemukan (tidak terhubung; periksa daftar dengan LIST)"));
            return;
        }
        try {
            String data = agent.requestExec(command, 12_000);
            send(Protocol.ok("EXEC " + agentId + " " + data));
        } catch (AgentException e) {
            send(Protocol.err(e.code, e.getMessage()));
        }
    }

    /**
     * Memeriksa apakah nama perintah termasuk whitelist eksekusi jarak jauh.
     *
     * @param command nama perintah (huruf kecil)
     * @return true jika diizinkan
     */
    private static boolean isAllowedExecCommand(String command) {
        return "hostname".equals(command) || "whoami".equals(command) || "ipconfig".equals(command)
                || "ifconfig".equals(command) || "tasklist".equals(command) || "ps".equals(command);
    }

    /**
     * Menangani perintah UPLOAD dari Inspector.
     * Server membuat token transfer, menginformasikan kanal data kepada klien,
     * lalu menunggu penerimaan berkas melalui {@link DataTransferService}.
     *
     * @param p token perintah, p[1] adalah nama berkas
     * @throws IOException jika gagal menulis respons
     */
    private boolean doUpload(String[] p) throws IOException {
        if (!requireInspector()) {
            return true;
        }
        if (p.length != 2) {
            send(Protocol.err(400, "Format: UPLOAD <namaBerkas>"));
            return true;
        }
        String filename = p[1];
        if (!Protocol.isSafeFilename(filename)) {
            send(Protocol.err(400, "Nama Berkas Tidak Valid"));
            return true;
        }
        String token = server.getDataTransfer().createUploadToken(filename);
        transferring = true;
        try {
            send("200 UPLOAD READY token=" + token + " dataPort=" + server.getDataPort()
                    + " filename=" + filename);
            CompletableFuture<String> f = server.getDataTransfer().getResult(token);
            String result = f.get(TRANSFER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if ("OK".equals(result)) {
                send(Protocol.ok("UPLOAD Selesai " + filename));
            } else {
                send(Protocol.err(400, "Upload Gagal (" + result + ")"));
            }
        } catch (TimeoutException e) {
            send(Protocol.err(408, "Transfer Timeout: berkas gagal diterima dalam waktu yang ditentukan"));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            send(Protocol.err(500, "Transfer Dibatalkan"));
        } catch (ExecutionException e) {
            send(Protocol.err(500, "Kesalahan Internal Transfer: " + e.getMessage()));
        } finally {
            transferring = false;
            server.getDataTransfer().removeToken(token);
        }
        return true;
    }

    /**
     * Menangani perintah PUSH dari Inspector: meminta Agent mengunduh berkas patch
     * dari kanal data server. Menggunakan pola {@link CompletableFuture} seperti
     * SYSINFO dan EXEC.
     *
     * @param p token perintah, p[1] agentId, p[2] nama berkas
     * @throws IOException jika gagal menulis respons
     */
    private boolean doPush(String[] p) throws IOException {
        if (!requireInspector()) {
            return true;
        }
        if (p.length != 3) {
            send(Protocol.err(400, "Format: PUSH <agentId> <namaBerkas>"));
            return true;
        }
        String agentId = p[1];
        String filename = p[2];
        if (!Protocol.isSafeFilename(filename)) {
            send(Protocol.err(400, "Nama Berkas Tidak Valid"));
            return true;
        }
        File uploaded = new File(server.getUploadsDir(), filename);
        if (!uploaded.isFile()) {
            send(Protocol.err(404, "Berkas Patch Tidak Ada di Server"));
            return true;
        }
        ClientHandler agent = server.getSessions().get(agentId);
        if (agent == null || !agent.isAgent()) {
            send(Protocol.err(404, "Agent Tidak Ditemukan (tidak terhubung; periksa daftar dengan LIST)"));
            return true;
        }
        String token = server.getDataTransfer().createPushToken(filename, uploaded.length());
        transferring = true;
        try {
            String result = agent.requestPush(token, server.getDataPort(), TRANSFER_TIMEOUT_MS);
            if ("OK".equals(result) || "PUSH OK".equals(result)) {
                send(Protocol.ok("PUSH Selesai " + agentId + " " + filename));
            } else {
                send(Protocol.err(500, "Push Gagal (" + Protocol.clean(result, 60) + ")"));
            }
        } catch (AgentException e) {
            send(Protocol.err(e.code, e.getMessage()));
        } finally {
            transferring = false;
            server.getDataTransfer().removeToken(token);
        }
        return true;
    }

    /**
     * Meminta Agent mengunduh berkas patch melalui kanal data.
     * Server mengirim {@code REQ <id> PUSH <token> <dataPort>} ke Agent,
     * lalu menunggu balasan {@code RES <id> PUSH OK} atau error.
     *
     * @param token     token transfer yang dibuat oleh server
     * @param dataPort  port kanal data TCP
     * @param timeoutMs batas waktu tunggu balasan Agent
     * @return "OK" jika Agent berhasil mengunduh berkas
     * @throws AgentException jika Agent gagal, timeout, atau terputus
     */
    private String requestPush(String token, int dataPort, long timeoutMs) throws AgentException {
        String id = Long.toString(requestSeq.incrementAndGet());
        CompletableFuture<String> future = new CompletableFuture<>();
        pending.put(id, future);
        try {
            send("REQ " + id + " PUSH " + token + " " + dataPort);
            String reply = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (reply.startsWith("ERR")) {
                throw new AgentException(500, "Agent Gagal Menerima Berkas (" + Protocol.clean(reply, 60) + ")");
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

    /**
     * Mendaftarkan Inspector untuk menerima fan-out telemetri UDP.
     * Inspector mengirim port UDP lokalnya; server mengirim datagram ke alamat IP klien TCP pada port tersebut.
     *
     * @param p token perintah, p[1] adalah port UDP lokal Inspector
     * @throws IOException jika gagal menulis respons
     */
    private void doSubscribe(String[] p) throws IOException {
        if (!requireInspector()) {
            return;
        }
        if (p.length != 2) {
            send(Protocol.err(400, "Format: SUBSCRIBE <udpPort>"));
            return;
        }
        int udpPort;
        try {
            udpPort = Integer.parseInt(p[1]);
            if (udpPort < 1 || udpPort > 65535) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            send(Protocol.err(400, "Port UDP Tidak Valid"));
            return;
        }
        server.getTelemetry().subscribe(username, socket.getInetAddress(), udpPort);
        send(Protocol.ok("SUBSCRIBED " + udpPort));
    }

    /**
     * Membatalkan langganan telemetri UDP Inspector.
     *
     * @throws IOException jika gagal menulis respons
     */
    private void doUnsubscribe() throws IOException {
        if (!requireInspector()) {
            return;
        }
        server.getTelemetry().unsubscribe(username);
        send(Protocol.ok("UNSUBSCRIBED"));
    }

    /**
     * Menampilkan metrik telemetri terakhir untuk semua Agent.
     * Agent yang melewati ambang batas stale ditandai STALE.
     *
     * @throws IOException jika gagal menulis respons
     */
    private void doWatch() throws IOException {
        if (!requireInspector()) {
            return;
        }
        java.util.Map<String, TelemetryService.AgentMetrics> map = server.getTelemetry().getMetrics();
        if (map.isEmpty()) {
            send(Protocol.ok("WATCH []"));
            return;
        }
        StringBuilder sb = new StringBuilder();
        java.util.List<TelemetryService.AgentMetrics> list = new java.util.ArrayList<>(map.values());
        list.sort(java.util.Comparator.comparing(m -> m.agentId == null ? "" : m.agentId));
        for (int i = 0; i < list.size(); i++) {
            TelemetryService.AgentMetrics m = list.get(i);
            if (i > 0) {
                sb.append(" ; ");
            }
            String status = m.stale ? "STALE" : "OK";
            sb.append(m.agentId).append(" cpu=").append(String.format(java.util.Locale.US, "%.2f", m.cpuLoad * 100.0))
              .append("% mem=").append(String.format(java.util.Locale.US, "%.2f", m.memPercent))
              .append("% seq=").append(m.seqNum).append(" status=").append(status);
        }
        send(Protocol.ok("WATCH [" + sb.toString() + "]"));
    }

    /**
     * Menggabungkan token mulai indeks tertentu menjadi satu teks.
     *
     * @param p     token hasil split
     * @param start indeks awal
     * @return teks gabungan (dipangkas spasi di tepi)
     */
    private static String joinFrom(String[] p, int start) {
        if (p == null || start >= p.length) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < p.length; i++) {
            if (i > start) {
                sb.append(' ');
            }
            sb.append(p[i]);
        }
        return sb.toString().trim();
    }
}
