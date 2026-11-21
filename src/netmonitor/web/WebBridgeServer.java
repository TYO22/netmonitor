package netmonitor.web;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import netmonitor.client.ClientConfig;
import netmonitor.client.ClientEngine;
import netmonitor.client.ClientListener;
import netmonitor.common.Protocol;

/**
 * Jembatan HTTP lokal untuk Web GUI NetMonitor.
 * Menyediakan berkas statis dari folder {@code web/}, REST endpoint, dan Server-Sent Events (SSE)
 * di atas satu {@link ClientEngine} per sesi browser. Tidak ada kode soket di handler HTTP;
 * semua I/O jaringan tetap di {@link ClientEngine}.
 */
public class WebBridgeServer {

    private static final int MAX_PORT_TRIES = 25;
    private static final long AGENT_LOGIN_TIMEOUT_MS = 20_000L;

    private final HttpServer server;
    private final int port;
    private final Gson gson = new Gson();
    private final Map<String, WebSession> sessions = new ConcurrentHashMap<>();
    private final Path webRoot;

    /**
     * Membuat server HTTP pada port yang diminta (auto-increment bila terpakai).
     *
     * @param preferredPort port awal yang diinginkan
     * @throws IOException bila server tidak dapat dibuat
     */
    public WebBridgeServer(int preferredPort) throws IOException {
        int p = preferredPort;
        HttpServer created = null;
        for (int i = 0; i < MAX_PORT_TRIES; i++) {
            try {
                created = HttpServer.create(new InetSocketAddress("127.0.0.1", p), 0);
                break;
            } catch (IOException e) {
                p++;
            }
        }
        if (created == null) {
            throw new IOException("Tidak dapat menemukan port kosong mulai dari " + preferredPort);
        }
        this.server = created;
        this.port = p;
        this.webRoot = Paths.get("web").toAbsolutePath().normalize();
        this.server.setExecutor(Executors.newFixedThreadPool(32));
        registerContexts();
        this.server.start();
    }

    /** @return port HTTP yang benar-benar dipakai. */
    public int getPort() {
        return port;
    }

    /** Menghentikan server HTTP dan menutup semua sesi. */
    public void stop() {
        for (WebSession s : new ArrayList<>(sessions.values())) {
            closeSession(s, true);
        }
        server.stop(1);
    }

    private void registerContexts() {
        server.createContext("/", this::handleStatic);
        server.createContext("/api/state", this::handleState);
        server.createContext("/api/login", this::handleLogin);
        server.createContext("/api/logout", this::handleLogout);
        server.createContext("/api/nodes", this::handleNodes);
        server.createContext("/api/telemetry/subscribe", this::handleTelemetrySubscribe);
        server.createContext("/api/telemetry/unsubscribe", this::handleTelemetryUnsubscribe);
        server.createContext("/api/stream", this::handleStream);
    }

    // ------------------------------------------------------------------ static files

    private void handleStatic(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendJson(ex, 405, mapOf("ok", false, "message", "Method not allowed"));
            return;
        }
        String rawPath = ex.getRequestURI().getPath();
        if (rawPath == null || rawPath.isEmpty() || "/".equals(rawPath)) {
            rawPath = "/index.html";
        }
        Path requested = webRoot.resolve(rawPath.substring(1)).normalize();
        if (!requested.startsWith(webRoot) || !Files.isRegularFile(requested)) {
            sendText(ex, 404, "Not Found", "text/plain; charset=utf-8");
            return;
        }
        String mime = guessMime(requested.toString());
        byte[] data = Files.readAllBytes(requested);
        Headers h = ex.getResponseHeaders();
        h.set("Content-Type", mime);
        h.set("Cache-Control", "no-cache");
        ex.sendResponseHeaders(200, data.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(data);
        }
    }

    private static String guessMime(String name) {
        String lower = name.toLowerCase();
        if (lower.endsWith(".html") || lower.endsWith(".htm")) {
            return "text/html; charset=utf-8";
        }
        if (lower.endsWith(".css")) {
            return "text/css; charset=utf-8";
        }
        if (lower.endsWith(".js")) {
            return "application/javascript; charset=utf-8";
        }
        if (lower.endsWith(".json")) {
            return "application/json; charset=utf-8";
        }
        if (lower.endsWith(".svg")) {
            return "image/svg+xml";
        }
        if (lower.endsWith(".png")) {
            return "image/png";
        }
        return "application/octet-stream";
    }

    // ------------------------------------------------------------------ helpers

    private void sendJson(HttpExchange ex, int status, Object body) throws IOException {
        byte[] data = gson.toJson(body).getBytes(StandardCharsets.UTF_8);
        Headers h = ex.getResponseHeaders();
        h.set("Content-Type", "application/json; charset=utf-8");
        h.set("Cache-Control", "no-cache");
        ex.sendResponseHeaders(status, data.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(data);
        }
    }

    private void sendText(HttpExchange ex, int status, String text, String contentType) throws IOException {
        byte[] data = text.getBytes(StandardCharsets.UTF_8);
        Headers h = ex.getResponseHeaders();
        h.set("Content-Type", contentType);
        ex.sendResponseHeaders(status, data.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(data);
        }
    }

    private Map<String, Object> mapOf(Object... kv) {
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return m;
    }

    private String readBody(HttpExchange ex) throws IOException {
        try (InputStream is = ex.getRequestBody();
             BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                if (sb.length() > 0) {
                    sb.append('\n');
                }
                sb.append(line);
            }
            return sb.toString();
        }
    }

    private String getCookieSid(HttpExchange ex) {
        List<String> cookies = ex.getRequestHeaders().get("Cookie");
        if (cookies == null) {
            return null;
        }
        for (String c : cookies) {
            for (String part : c.split(";")) {
                String t = part.trim();
                if (t.startsWith("sid=")) {
                    return t.substring(4);
                }
            }
        }
        return null;
    }

    private WebSession sessionFromRequest(HttpExchange ex) {
        String sid = getCookieSid(ex);
        if (sid == null) {
            return null;
        }
        return sessions.get(sid);
    }

    private void setSessionCookie(HttpExchange ex, String sid, boolean clear) {
        String value = clear
                ? "sid=; Path=/; Max-Age=0; SameSite=Lax"
                : "sid=" + sid + "; Path=/; SameSite=Lax";
        ex.getResponseHeaders().add("Set-Cookie", value);
    }

    // ------------------------------------------------------------------ /api/state

    private void handleState(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendJson(ex, 405, mapOf("ok", false, "message", "Method not allowed"));
            return;
        }
        WebSession s = sessionFromRequest(ex);
        if (s == null) {
            sendJson(ex, 200, mapOf(
                    "connected", false,
                    "role", null,
                    "username", null,
                    "host", null,
                    "port", 0,
                    "subscribed", false,
                    "udpPort", 0,
                    "nodes", new ArrayList<>(),
                    "stagedFiles", new ArrayList<>(),
                    "receivedFiles", new ArrayList<>()
            ));
            return;
        }
        sendJson(ex, 200, mapOf(
                "connected", s.connected,
                "role", s.cfg.getRole(),
                "username", s.cfg.getUsername(),
                "host", s.cfg.getHost(),
                "port", s.cfg.getPort(),
                "subscribed", s.subscribed,
                "udpPort", s.udpPort,
                "nodes", nodesJson(s),
                "stagedFiles", new ArrayList<>(s.stagedFiles),
                "receivedFiles", new ArrayList<>(s.receivedFiles)
        ));
    }

    // ------------------------------------------------------------------ /api/login

    private void handleLogin(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            sendJson(ex, 405, mapOf("ok", false, "message", "Method not allowed"));
            return;
        }
        JsonObject body;
        try {
            body = gson.fromJson(readBody(ex), JsonObject.class);
        } catch (Exception e) {
            sendJson(ex, 400, mapOf("ok", false, "message", "Body JSON tidak valid"));
            return;
        }
        if (body == null) {
            sendJson(ex, 400, mapOf("ok", false, "message", "Body kosong"));
            return;
        }
        String host = str(body, "host", "localhost");
        int port;
        try {
            port = body.has("port") ? body.get("port").getAsInt() : 9090;
        } catch (Exception e) {
            sendJson(ex, 400, mapOf("ok", false, "message", "Port tidak valid"));
            return;
        }
        String role = str(body, "role", "INSPECTOR").toUpperCase();
        String username = str(body, "username", "");
        String password = str(body, "password", "");
        if (!Protocol.isValidRole(role)) {
            sendJson(ex, 400, mapOf("ok", false, "message", "Peran harus AGENT atau INSPECTOR"));
            return;
        }
        if (username.isEmpty()) {
            sendJson(ex, 400, mapOf("ok", false, "message", "Username tidak boleh kosong"));
            return;
        }

        ClientConfig cfg = new ClientConfig(host, port, role, username, password);
        WebListener listener = new WebListener();
        ClientEngine engine = new ClientEngine(cfg, listener);
        WebSession session = new WebSession(UUID.randomUUID().toString(), cfg, engine, listener);

        boolean ok;
        String message;
        if (cfg.isAgent()) {
            CountDownLatch latch = new CountDownLatch(1);
            listener.setLoginLatch(latch, session);
            session.agentThread = new Thread(() -> engine.runAgent(), "web-agent-" + username);
            session.agentThread.setDaemon(true);
            session.agentThread.start();
            try {
                latch.await(AGENT_LOGIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            ok = listener.loginSucceeded;
            message = listener.lastMessage;
            if (!ok) {
                try {
                    engine.requestQuit();
                } catch (Exception ignored) {
                    // abaikan
                }
                try {
                    session.agentThread.join(2000);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        } else {
            listener.setLoginLatch(null, session);
            ok = engine.connect();
            message = ok ? listener.lastMessage : listener.lastError;
            if (message == null || message.isEmpty()) {
                message = ok ? "Login berhasil" : "Login gagal";
            }
        }

        if (!ok) {
            sendJson(ex, 401, mapOf("ok", false, "message", message == null ? "Login gagal" : message));
            return;
        }
        session.connected = true;
        if (message == null || message.isEmpty()) {
            message = "Login berhasil";
        }
        sessions.put(session.id, session);
        setSessionCookie(ex, session.id, false);
        sendJson(ex, 200, mapOf(
                "ok", true,
                "message", message,
                "role", cfg.getRole(),
                "username", cfg.getUsername()
        ));
    }

    // ------------------------------------------------------------------ /api/logout

    private void handleLogout(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            sendJson(ex, 405, mapOf("ok", false, "message", "Method not allowed"));
            return;
        }
        WebSession s = sessionFromRequest(ex);
        if (s != null) {
            closeSession(s, false);
        }
        setSessionCookie(ex, "", true);
        sendJson(ex, 200, mapOf("ok", true, "message", "Logout berhasil"));
    }

    private void closeSession(WebSession s, boolean silent) {
        sessions.remove(s.id);
        s.connected = false;
        try {
            if (s.cfg.isAgent()) {
                s.engine.requestQuit();
            } else {
                try {
                    s.engine.request("QUIT");
                } catch (Exception ignored) {
                    // abaikan
                }
            }
        } catch (Exception ignored) {
            // abaikan
        }
        try {
            s.engine.close();
        } catch (Exception ignored) {
            // abaikan
        }
        if (s.agentThread != null) {
            try {
                s.agentThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // tutup koneksi SSE yang masih terbuka
        for (OutputStream os : new ArrayList<>(s.sseClients)) {
            try {
                os.close();
            } catch (IOException ignored) {
                // abaikan
            }
            s.sseClients.remove(os);
        }
    }

    // ------------------------------------------------------------------ /api/nodes

    private void handleNodes(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendJson(ex, 405, mapOf("ok", false, "message", "Method not allowed"));
            return;
        }
        WebSession s = sessionFromRequest(ex);
        if (s == null) {
            sendJson(ex, 401, mapOf("ok", false, "message", "Sesi tidak ditemukan"));
            return;
        }
        if (!"INSPECTOR".equals(s.cfg.getRole())) {
            sendJson(ex, 403, mapOf("ok", false, "message", "Hanya INSPECTOR yang dapat melihat node"));
            return;
        }
        ClientEngine.Result r = s.engine.request("LIST");
        if (r.status != ClientEngine.Result.Status.OK || r.line == null) {
            sendJson(ex, 200, mapOf("ok", false, "message", "Gagal mengambil LIST", "nodes", new ArrayList<>()));
            return;
        }
        List<Map<String, Object>> nodes = parseUsersList(r.line);
        List<String> order = new ArrayList<>();
        for (Map<String, Object> n : nodes) {
            if (!"AGENT".equals(String.valueOf(n.get("role")))) {
                continue;
            }
            String id = String.valueOf(n.get("agentId"));
            order.add(id);
            s.nodes.put(id, n);
        }
        s.nodeOrder = order;
        sendJson(ex, 200, mapOf("ok", r.line.startsWith("200"), "line", r.line, "nodes", nodesJson(s)));
    }

    /**
     * Mengurai respons LIST "200 USERS [name | ROLE | location | isp ; ...]" menjadi daftar objek node.
     */
    private static List<Map<String, Object>> parseUsersList(String line) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (line == null) {
            return out;
        }
        int open = line.indexOf('[');
        int close = line.lastIndexOf(']');
        if (open < 0 || close <= open) {
            return out;
        }
        String inner = line.substring(open + 1, close).trim();
        if (inner.isEmpty()) {
            return out;
        }
        String[] entries = inner.split("\\s*;\\s*");
        for (String e : entries) {
            String[] parts = e.split("\\s*\\|\\s*");
            if (parts.length < 2) {
                continue;
            }
            String id = parts[0].trim();
            String role = parts[1].trim();
            String location = parts.length > 2 ? parts[2].trim() : "-";
            String isp = parts.length > 3 ? parts[3].trim() : "-";
            Map<String, Object> node = new java.util.LinkedHashMap<>();
            node.put("agentId", id);
            node.put("role", role);
            node.put("status", "ONLINE");
            node.put("location", location);
            node.put("isp", isp);
            out.add(node);
        }
        return out;
    }

    /**
     * Mengembalikan daftar node sesi dalam urutan LIST terakhir, dilengkapi telemetri terakhir.
     */
    private static List<Map<String, Object>> nodesJson(WebSession s) {
        List<Map<String, Object>> list = new ArrayList<>();
        for (String id : s.nodeOrder) {
            Map<String, Object> n = s.nodes.get(id);
            if (n != null) {
                list.add(n);
            }
        }
        return list;
    }

    // ------------------------------------------------------------------ /api/telemetry/subscribe

    private void handleTelemetrySubscribe(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            sendJson(ex, 405, mapOf("ok", false, "message", "Method not allowed"));
            return;
        }
        WebSession s = sessionFromRequest(ex);
        if (s == null) {
            sendJson(ex, 401, mapOf("ok", false, "message", "Sesi tidak ditemukan"));
            return;
        }
        if (!"INSPECTOR".equals(s.cfg.getRole())) {
            sendJson(ex, 403, mapOf("ok", false, "message", "Hanya INSPECTOR"));
            return;
        }
        JsonObject body = readJsonObject(ex);
        if (body == null) {
            sendJson(ex, 400, mapOf("ok", false, "message", "Body JSON tidak valid"));
            return;
        }
        int udpPort;
        try {
            udpPort = body.has("udpPort") ? body.get("udpPort").getAsInt() : 9095;
        } catch (Exception e) {
            sendJson(ex, 400, mapOf("ok", false, "message", "udpPort tidak valid"));
            return;
        }
        if (udpPort < 1 || udpPort > 65535) {
            sendJson(ex, 400, mapOf("ok", false, "message", "udpPort di luar rentang"));
            return;
        }
        // mulai penerima UDP lokal dulu, lalu SUBSCRIBE ke server
        if (!s.engine.subscribeTelemetry(udpPort)) {
            sendJson(ex, 500, mapOf("ok", false, "message", "Gagal membuka socket UDP lokal"));
            return;
        }
        ClientEngine.Result r = s.engine.request("SUBSCRIBE " + udpPort);
        if (r.status != ClientEngine.Result.Status.OK || r.line == null || !r.line.startsWith("200")) {
            s.engine.unsubscribeTelemetry();
            sendJson(ex, 200, mapOf("ok", false, "line", r.line, "message", "SUBSCRIBE ditolak server"));
            return;
        }
        s.subscribed = true;
        s.udpPort = udpPort;
        sendJson(ex, 200, mapOf("ok", true, "line", r.line, "udpPort", udpPort));
    }

    // ------------------------------------------------------------------ /api/telemetry/unsubscribe

    private void handleTelemetryUnsubscribe(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            sendJson(ex, 405, mapOf("ok", false, "message", "Method not allowed"));
            return;
        }
        WebSession s = sessionFromRequest(ex);
        if (s == null) {
            sendJson(ex, 401, mapOf("ok", false, "message", "Sesi tidak ditemukan"));
            return;
        }
        ClientEngine.Result r = s.engine.request("UNSUBSCRIBE");
        s.engine.unsubscribeTelemetry();
        s.subscribed = false;
        s.udpPort = 0;
        sendJson(ex, 200, mapOf("ok", r.line != null && r.line.startsWith("200"), "line", r.line));
    }

    private JsonObject readJsonObject(HttpExchange ex) {
        try {
            return gson.fromJson(readBody(ex), JsonObject.class);
        } catch (Exception e) {
            return null;
        }
    }

    // ------------------------------------------------------------------ /api/stream (SSE)

    private void handleStream(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendJson(ex, 405, mapOf("ok", false, "message", "Method not allowed"));
            return;
        }
        WebSession s = sessionFromRequest(ex);
        if (s == null) {
            sendJson(ex, 401, mapOf("ok", false, "message", "Sesi tidak ditemukan"));
            return;
        }
        Headers h = ex.getResponseHeaders();
        h.set("Content-Type", "text/event-stream; charset=utf-8");
        h.set("Cache-Control", "no-cache");
        h.set("Connection", "keep-alive");
        ex.sendResponseHeaders(200, 0);
        OutputStream os = ex.getResponseBody();
        s.sseClients.add(os);
        // kirim satu event awal agar klien tahu stream aktif
        writeSse(os, "log", mapOf("level", "INFO", "line", "SSE stream connected", "timestamp", now()));
        try {
            while (true) {
                // heartbeat komentar agar koneksi tetap hidup dan putus terdeteksi
                try {
                    os.write(": ping\n\n".getBytes(StandardCharsets.UTF_8));
                    os.flush();
                } catch (IOException e) {
                    break;
                }
                Thread.sleep(15000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            s.sseClients.remove(os);
            try {
                os.close();
            } catch (IOException ignored) {
                // abaikan
            }
        }
    }

    // ------------------------------------------------------------------ WebListener callback bridge

    /**
     * Listener yang meneruskan kejadian ClientEngine ke SSE. Method dipanggil dari thread mesin jaringan.
     */
    public static final class WebListener implements ClientListener {
        volatile CountDownLatch loginLatch;
        volatile WebSession session;
        volatile boolean loginSucceeded;
        volatile String lastError;
        volatile String lastMessage;
        volatile String lastMessageLevel;

        void setLoginLatch(CountDownLatch latch, WebSession s) {
            this.loginLatch = latch;
            this.session = s;
            this.loginSucceeded = false;
            this.lastError = null;
            this.lastMessage = null;
        }

        private void broadcast(String event, Object data) {
            if (session == null) {
                return;
            }
            String json;
            try {
                json = new Gson().toJson(data);
            } catch (Exception e) {
                json = "{\"error\":\"serialization\"}";
            }
            for (OutputStream os : new ArrayList<>(session.sseClients)) {
                try {
                    writeSse(os, event, json);
                } catch (IOException e) {
                    session.sseClients.remove(os);
                    try {
                        os.close();
                    } catch (IOException ignored) {
                        // abaikan
                    }
                }
            }
        }

        private static void writeSse(OutputStream os, String event, Object data) throws IOException {
            WebBridgeServer.writeSse(os, event, data);
        }

        @Override
        public void onInfo(String message) {
            lastMessage = message;
            if (loginLatch != null && !loginSucceeded && session != null && !session.connected) {
                // pesan info saat login inspector tidak menandakan gagal; biarkan
            }
            broadcast("log", mapStatic("level", "INFO", "line", message, "timestamp", now()));
        }

        @Override
        public void onError(String message) {
            lastError = message;
            lastMessage = message;
            broadcast("log", mapStatic("level", "ERROR", "line", message, "timestamp", now()));
            CountDownLatch l = loginLatch;
            if (l != null && !loginSucceeded && session != null && !session.connected) {
                // jangan langsung gagal; login inspector memanggil onError hanya pada kegagalan
                // Agent: onError sebelum connected berarti gagal
                if (session.cfg.isAgent()) {
                    loginSucceeded = false;
                    l.countDown();
                    loginLatch = null;
                }
            }
        }

        @Override
        public void onConnected(String loginResponse, boolean reconnected) {
            lastMessage = loginResponse;
            loginSucceeded = true;
            if (session != null) {
                session.connected = true;
            }
            CountDownLatch l = loginLatch;
            if (l != null) {
                l.countDown();
                loginLatch = null;
            }
            broadcast("log", mapStatic("level", "INFO", "line", loginResponse, "timestamp", now()));
        }

        @Override
        public void onServerMessage(String line) {
            broadcast("log", mapStatic("level", "INFO", "line", line, "timestamp", now()));
        }

        @Override
        public void onChatMessage(String line) {
            broadcast("message", mapStatic("type", "chat", "raw", line, "timestamp", now()));
        }

        @Override
        public void onTelemetry(String agentId, double cpuLoad, double memPercent, int seqNum) {
            broadcast("telemetry", mapStatic("agentId", agentId, "cpu", cpuLoad, "ram", memPercent, "seq", seqNum, "timestamp", now()));
            WebSession s = session;
            if (s == null) {
                return;
            }
            Map<String, Object> node = s.nodes.get(agentId);
            if (node != null) {
                node.put("cpu", cpuLoad);
                node.put("ram", memPercent);
                node.put("seq", seqNum);
                node.put("status", "ONLINE");
            } else {
                Map<String, Object> created = new java.util.LinkedHashMap<>();
                created.put("agentId", agentId);
                created.put("role", "AGENT");
                created.put("status", "ONLINE");
                created.put("location", "-");
                created.put("isp", "-");
                created.put("cpu", cpuLoad);
                created.put("ram", memPercent);
                created.put("seq", seqNum);
                Map<String, Object> prev = s.nodes.putIfAbsent(agentId, created);
                if (prev != null && prev != created) {
                    prev.put("cpu", cpuLoad);
                    prev.put("ram", memPercent);
                    prev.put("seq", seqNum);
                    prev.put("status", "ONLINE");
                }
            }
        }

        @Override
        public void onTransferProgress(String direction, long bytesTransferred, long totalExpected) {
            broadcast("transfer", mapStatic("direction", direction, "filename", null, "bytesTransferred", bytesTransferred, "totalBytes", totalExpected, "status", "progress", "timestamp", now()));
        }

        @Override
        public void onConnectionLost(String reason, boolean willReconnect, boolean byServer) {
            if (session != null) {
                session.connected = false;
            }
            broadcast("disconnect", mapStatic("reason", reason, "willReconnect", willReconnect, "byServer", byServer, "timestamp", now()));
            CountDownLatch l = loginLatch;
            if (l != null && !loginSucceeded) {
                lastError = reason;
                loginSucceeded = false;
                l.countDown();
                loginLatch = null;
            }
        }

        @Override
        public void onReconnecting(int attempt, int maxAttempts, long delayMs) {
            broadcast("log", mapStatic("level", "INFO", "line", "Mencoba menyambung ulang " + attempt + "/" + maxAttempts, "timestamp", now()));
        }

        private static Map<String, Object> mapStatic(Object... kv) {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            for (int i = 0; i + 1 < kv.length; i += 2) {
                m.put(String.valueOf(kv[i]), kv[i + 1]);
            }
            return m;
        }
    }

    // ------------------------------------------------------------------ WebSession

    /** Satu sesi browser: satu ClientEngine + metadata. */
    public static final class WebSession {
        public final String id;
        public final ClientConfig cfg;
        public final ClientEngine engine;
        public final WebListener listener;
        public final List<String> stagedFiles = new CopyOnWriteArrayList<>();
        public final List<String> receivedFiles = new CopyOnWriteArrayList<>();
        public final CopyOnWriteArrayList<OutputStream> sseClients = new CopyOnWriteArrayList<>();
        public volatile boolean connected;
        public volatile boolean subscribed;
        public volatile int udpPort;
        public volatile Thread agentThread;
        /**
         * Peta agentId -> info node (role, location, isp, status, cpu, ram).
         * Diperbarui oleh handleNodes dan onTelemetry.
         */
        public final ConcurrentHashMap<String, Map<String, Object>> nodes = new ConcurrentHashMap<>();
        /**
         * Urutan nama agent dari LIST terakhir, untuk menjaga urutan tampilan tabel.
         */
        public volatile List<String> nodeOrder = new ArrayList<>();

        WebSession(String id, ClientConfig cfg, ClientEngine engine, WebListener listener) {
            this.id = id;
            this.cfg = cfg;
            this.engine = engine;
            this.listener = listener;
            this.listener.session = this;
        }
    }

    // ------------------------------------------------------------------ static SSE writer

    static void writeSse(OutputStream os, String event, Object data) throws IOException {
        Gson g = new Gson();
        String payload = (data instanceof String) ? (String) data : g.toJson(data);
        String frame = "event: " + event + "\ndata: " + payload + "\n\n";
        os.write(frame.getBytes(StandardCharsets.UTF_8));
        os.flush();
    }

    private static String str(JsonObject o, String key, String def) {
        if (o == null || !o.has(key) || o.get(key).isJsonNull()) {
            return def;
        }
        try {
            return o.get(key).getAsString();
        } catch (Exception e) {
            return def;
        }
    }

    private static String now() {
        return java.time.Instant.now().toString();
    }
}