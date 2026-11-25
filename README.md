# NetMonitor Enterprise — Kelompok 2
**Pemrograman Jaringan [TIS31263]** — Final Project Pemrograman Jaringan

**Anggota Kelompok:**
- Muhamad Radityo Pratama
- Marsha Harliani Azzahra
- Raditya Yhori Pratama

---

## Gambaran Umum

NetMonitor Enterprise adalah aplikasi client-server berbasis Java (JDK 21, pustaka Google Gson 2.15.0) untuk memantau dan mengelola sekumpulan node (Agent) dari satu titik kendali (Inspector). Skenario proyek adalah *Remote Server Cluster & Resource Inspector*, di mana Inspector dapat mengelola banyak node Agent yang tersebar melalui protokol TCP kustom, transfer berkas biner, telemetri UDP real-time, dan operasi jarak jauh.

Aplikasi terdiri dari dua peran utama:
- **INSPECTOR** — Administrator yang memantau node, mengirim perintah, upload berkas, dan berkomunikasi tim.
- **AGENT** — Node yang dipantau, menjawab permintaan info sistem, menerima perintah remote, dan menerima file patch.

---

## Arsitektur & Fitur Utama Sistem

### 1. Lapisan Server (`src/netmonitor/server/`)
- **NetMonitorServer** — Titik masuk server. Membuka `ServerSocket` TCP port 9090, thread pool tetap 50 (`Executors.newFixedThreadPool`), penghitung atomik `activeClients`, scheduler heartbeat, GeoIP lookup, dan layanan data-channel port 9092.
- **ClientHandler** (`implements Runnable`) — Melayani satu klien per thread: dispatcher perintah (`LOGIN`, `LIST`, `SYSINFO`, `PING`, `RESOLVE`, `BROADCAST`, `MSG`, `EXEC`, `SUBSCRIBE`, `UNSUBSCRIBE`, `WATCH`, `UPLOAD`, `PUSH`, `QUIT`), validasi, dan delegasi ke service terkait.
- **AuthService** — Otentikasi kredensial dari `users.properties`, pengecekan batas 3 kali login gagal, timeout pre-login 30 detik.
- **GeoIpService** — REST API client ke ip-api.com melalui `HttpURLConnection`. JSON hasil dideserialisasi ke `GeoLocationDTO` menggunakan Google Gson. Field `region` dipetakan dari kunci JSON `regionName`. Untuk status 4xx/5xx, body kesalahan dibaca dari `getErrorStream()`.
- **TelemetryService** — `DatagramSocket` port UDP 9091 (konfigurasi via `-Dnetmonitor.udp.port`). Menerima datagram metrik dari Agent, deteksi stale via ambang `-Dnetmonitor.telemetry.staleMs` (default 30000 ms), fan-out unik ke Inspector yang `SUBSCRIBE`.
- **DataTransferService** — Kanal TCP terpisah port 9092 (konfigurasi via `-Dnetmonitor.data.port`). Transfer berkas biner dengan chunking 8192 byte (8 KB), header metadata (`writeUTF nama` + `writeLong ukuran`), dan `shutdownOutput()` tanpa menutup soket kontrol. Otorisasi lewat token sekali pakai (`ConcurrentHashMap` + `AtomicBoolean`).

### 2. Mesin Klien (`src/netmonitor/client/`)
- **ClientEngine** — Mesin jaringan tanpa UI. Dipakai bersama `ConsoleClientUI` (CLI) dan `WebBridgeServer` (Web GUI). Menjalankan Background Receiver Thread untuk pesan push (`BCAST`/`MSG`) secara asinkron.
- **ClientListener** — Antarmuka kejadian mesin-tampilan: `onInfo`, `onError`, `onConnected`, `onServerMessage`, `onConnectionLost`, `onTelemetry`, `onTransferProgress`. Semua keluaran log lewat antarmuka ini agar siap digunakan CLI maupun Web GUI.
- **NetMonitorClient** — `main` tipis: `ConsoleClientUI.promptConfig` → `ConsoleClientUI.run`.

### 3. Util & DTO
- **Protocol** — Konstanta `DEFAULT_PORT = 9090`, `MAX_LINE_LENGTH = 4096`, `MAX_BROADCAST_TEXT_LENGTH = 500`, `MAX_MSG_TEXT_LENGTH = 500`. Utilitas `isValidUsername`, `isValidRole`, `isSafeFilename` (anti path-traversal), `ok()/err/clean`. Penanganan `LineTooLongException` jika satu baris melebihi `MAX_LINE_LENGTH`.
- **GeoLocationDTO** — POJO Gson dengan `@SerializedName` di seluruh field. Field `region` dipetakan dari kunci JSON `regionName`. Method `isSuccess()` untuk status "success"/"fail". Field `message` untuk alasan gagal.
- **FileLogger** — Menulis log server ke berkas log dengan `synchronized`, juga tercetak ke konsol.

### 4. Telemetri UDP & Heartbeat
- Agent mengirim paket metrik CPU/Memori berkala (±2 detik) melalui `DatagramPacket` ke port 9091.
- Inspector `SUBSCRIBE` ke port UDP lokal; server meneruskan fan-out melalui socket UDP utama (unicast, bukan socket baru).
- Mekanisme retransmisi hanya untuk datagram kontrol `HELLO` (maks 3x, ACK timeout 500 ms); telemetri toleran hilang paket (deteksi celah lewat nomor urut).
- Perintah `WATCH` menampilkan metrik terakhir seluruh Agent, menandai status `STALE` bila melewati batas staleMs.

### 5. Web GUI (`web/` & `src/netmonitor/web/`)
- **NetMonitorWebClient** — Entry point peluncur Web GUI. Menjalankan `WebBridgeServer` pada port lokal (bawaan 8080, auto-increment bila port sudah dipakai) lalu mencetak URL browser.
- **WebBridgeServer** — Jembatan HTTP (`com.sun.net.httpserver.HttpServer` JDK) + Gson + SSE (Server-Sent Events). Semua operasi I/O soket dijalankan di background thread terpisah dari HTTP handler, sehingga UI browser tidak freeze. Server tetap mencetak log aktivitas di konsol terminal.
- **Endpoint REST & SSE:**
  - `GET /api/state` — Status sesi (terhubung, peran, username, nodes, stagedFiles, receivedFiles, receivedFilesDetail).
  - `POST /api/login` — Login (host, port, username, password) → buat `ClientEngine`.
  - `POST /api/logout` — `QUIT` dan tutup seluruh sesi.
  - `GET /api/nodes` — Daftar node (auto-polling 5 detik saat sesi INSPECTOR aktif).
  - `GET /api/telemetry/subscribe` / `unsubscribe` — Daftar/batalkan terima telemetri UDP.
  - `GET /api/stream` — **SSE**: `connected`, `lost`, `chat`, `telemetry`, `transfer-progress`, `info`, `error`.
  - `POST /api/diagnostics/sysinfo` — Body `{agentId}` → `SYSINFO <agentId>`, respons memuat `os`, `cores`, `ram`, `agentUptime`.
  - `POST /api/diagnostics/exec` — Body `{agentId, command}` → `EXEC <agentId> <command>` (whitelist server: `hostname`, `whoami`, `ipconfig`, `ifconfig`, `tasklist`, `ps`).
  - `POST /api/diagnostics/ping` — Body `{host}` → `PING <host>`.
  - `POST /api/diagnostics/resolve` — Body `{domain}` → `RESOLVE <domain>`.
  - `POST /api/files/upload` — Body mentah + header `X-Filename` → sanitasi `Paths.get(rawName).getFileName()`, tulis sementara, panggil `ClientEngine.requestUpload`, siarannya SSE `event: transfer`.
  - `POST /api/files/push` — Body `{agentId, filename}` → `PUSH <agentId> <filename>`, siaran SSE `event: log`.
  - `POST /api/chat/send` — Body `{target, message}` → `BROADCAST` bila target `"ALL"`, selain itu `MSG <target> <message>`. Siaran SSE `event: log`.
  - `GET /api/recipients` — Daftar username tujuan unik (selalu `"admin"` + seluruh username sesi aktif, kecuali pemanggil).

---

## Struktur Direktori Repositori (berkas terlacak Git)

Berikut pohon direktori berkas yang terlacak di Git (output `git ls-files`):

```
.gitignore
lib/
  gson-2.15.0.jar
README.md
src/
  netmonitor/
    client/
      ClientConfig.java
      ClientEngine.java
      ClientListener.java
      ConsoleClientUI.java
      NetMonitorClient.java
    common/
      Protocol.java
    dto/
      GeoLocationDTO.java
    server/
      AuthService.java
      ClientHandler.java
      DataTransferService.java
      GeoIpService.java
      NetMonitorServer.java
      TelemetryService.java
    util/
      FileLogger.java
      NetUtil.java
    web/
      NetMonitorWebClient.java
      WebBridgeServer.java
web/
  app.js
  index.html
  styles.css
users.properties
```

**Catatan:**
- Berkas konfigurasi akun (`users.properties`) bersifat opsional; jika tidak ada, akun bawaan digunakan (`admin/admin123` INSPECTOR, `agent01..03/agent123` AGENT).

---

## Matriks Perintah Protokol & Endpoint Web Bridge

| Perintah | Pengirim | Fungsi | Eksekutor | Catatan |
|---|---|---|---|---|
| `LOGIN <peran> <user> <pass>` | semua | Autentikasi & registrasi sesi; memicu lookup GeoIP | Server (`AuthService` + `GeoIpService`) | 3 kali gagal → diputus |
| `LIST` | INSPECTOR | Daftar pengguna aktif dengan peran, lokasi, ISP | Server (`ClientHandler`, `requireInspector`) | Format: `USERS [nama\|PERAN\|lokasi\|ISP ; ...]` |
| `SYSINFO <agentId>` | INSPECTOR | Minta info sistem dari Agent tertentu | Server → Agent (`REQ <id> SYSINFO` → `RES <id> <data>`) | Agent mengeksekusi via `ProcessBuilder` |
| `PING <host>` | INSPECTOR | Cek keterjangkauan host dari sisi server | Server (`NetUtil.ping`) | Timeout 3 detik, kode 408/503 |
| `RESOLVE <domain>` | INSPECTOR | Resolusi DNS | Server (`NetUtil.resolveWithTimeout`) | Timeout 4 detik |
| `BROADCAST <teks>` | INSPECTOR | Kirim pesan ke seluruh klien | Server (fan-out ke semua sesi) | Maks 500 karakter |
| `MSG <username> <teks>` | INSPECTOR & AGENT | Pesan privat ke satu pengguna | Server (rute privat ke `ClientListener.onChatMessage`) | Maks 500 karakter |
| `EXEC <agentId> <perintah>` | INSPECTOR | Eksekusi perintah whitelist di Agent | Server → Agent (`REQ <id> EXEC <nama>`) → Agent `ProcessBuilder` | Whitelist: `hostname`, `whoami`, `ipconfig`, `ifconfig`, `tasklist`, `ps` |
| `SUBSCRIBE <udpPort>` | INSPECTOR | Mulai terima fan-out telemetri UDP | Server (`TelemetryService.subscribe`) | Inspector buka `DatagramSocket` lokal |
| `UNSUBSCRIBE` | INSPECTOR | Berhenti terima telemetri UDP | Server (`TelemetryService.unsubscribe`) | — |
| `WATCH` | INSPECTOR | Tampilkan metrik terakhir seluruh Agent | Server (`TelemetryService.getMetrics`) | Status `STALE` bila melewati batas |
| `UPLOAD <path>` | INSPECTOR | Unggah berkas patch ke staging server | Server (`DataTransferService`, kanal 9092) | Header `X-Filename`, sanitasi nama |
| `PUSH <agentId> <file>` | INSPECTOR | Push berkas dari server ke Agent | Server → Agent (token sekali pakai, data channel 9092) | Agent simpan ke folder tujuan agent (path lokal) |
| `QUIT` / `EXIT` | semua | Tutup sesi dengan rapi | Server (hentikan pool, tutup soket) | — |

**Endpoint Web Bridge (SSE & REST):**
- `GET /api/state`, `POST /api/login`, `POST /api/logout`, `GET /api/stream` (SSE)
- `POST /api/diagnostics/*`, `POST /api/files/*`, `POST /api/chat/send`, `GET /api/recipients`
- Lihat `WebBridgeServer.java` untuk detail implementasi.

---

## Panduan Kompilasi & Cara Menjalankan Aplikasi

### Kompilasi (Windows PowerShell)
```powershell
# 1. Kompilasi semua modul (urutan dependency)
javac -Xlint:all -cp lib\gson-2.15.0.jar -d out src\netmonitor\common\*.java
javac -Xlint:all -cp lib\gson-2.15.0.jar -d out src\netmonitor\dto\*.java
javac -Xlint:all -cp lib\gson-2.15.0.jar -d out src\netmonitor\util\*.java
javac -Xlint:all -cp lib\gson-2.15.0.jar -d out src\netmonitor\server\*.java
javac -Xlint:all -cp lib\gson-2.15.0.jar -d out src\netmonitor\client\*.java
javac -Xlint:all -d out -cp lib\gson-2.15.0.jar src\netmonitor\web\*.java
```

### Menjalankan Server
```powershell
java -cp "out;lib\gson-2.15.0.jar" netmonitor.server.NetMonitorServer 9090
```

### Menjalankan Web GUI
```powershell
java -cp "out;lib\gson-2.15.0.jar" netmonitor.web.NetMonitorWebClient 8080
```
Buka `http://localhost:8080` di browser.

### Menjalankan Klien CLI
```powershell
# Inspector
java -cp out netmonitor.client.NetMonitorClient localhost 9090 INSPECTOR admin admin123

# Agent
java -cp out netmonitor.client.NetMonitorClient localhost 9090 AGENT agent01 agent123
```

### Verifikasi Manual Alur Kerja

1. **Login multi-peran:** Jalankan server, lalu buka tiga terminal: Inspector (`INSPECTOR admin admin123`), Agent 1 (`AGENT agent01 agent123`), Agent 2 (`AGENT agent02 agent123`). Ketik `LIST` di Inspector untuk melihat daftar node.

2. **Pemantauan telemetri UDP:** Inspector klik **SUBSCRIBE** di Web GUI (port 9095 default). Grafik sparkline CPU/Memory di dashboard akan memperbarui data real-time setiap ~2 detik. Klik **UNSUBSCRIBE** untuk berhenti.

3. **Diagnostik & Remote Command:**
   - Dari tab **Diagnostics & Remote Exec** Web GUI: isi host `8.8.8.8` lalu klik **PING**; isi domain `example.com` lalu **RESOLVE**.
   - Pilih target Agent di dropdown, klik **Fetch SysInfo** (`SYSINFO`), lalu **Run** EXEC (`hostname`/`ipconfig`/`tasklist`). Whitelist hanya mengizinkan `hostname`, `whoami`, `ipconfig`, `ifconfig`, `tasklist`, `ps`.

4. **Transfer file patch (PUSH):** Dari tab **Patch & File Transfer**: pilih berkas → **Upload** ke staging. Pilih berkas staged, pilih target Agent, klik **Push to Agent**. Berkas tiba di folder tujuan agent dengan ukuran verifikasi.

5. **Komunikasi tim (Team Comms):** Dari tab **Team Comms**: kirim **BROADCAST** yang diterima semua klien (indikator lingkaran hijau unread dot muncul). Kirim pesan privat ke `admin` dari Agent; respons muncul di INSPECTOR.

6. **Fault Tolerance:** Tutup tab Agent secara mendadak. Status di tabel **Systems** berubah menjadi `OFFLINE`. Coba akses `SYSINFO` ke Agent OFFLINE → pesan error informatif: `Agent "agent02" sedang OFFLINE atau tidak terhubung ke server`. Login ulang Agent → status kembali `ONLINE`.

---

## Catatan Konfigurasi

Berkas konfigurasi akun (`users.properties`) bersifat opsional; jika tidak ada, akun bawaan digunakan (`admin/admin123` INSPECTOR, `agent01..03/agent123` AGENT).

Hanya komponen berikut yang merupakan referensi inti repositori:
- **`src/netmonitor/...`** — Kode sumber Java (tiga lapisan utama: client, server, web).
- **`web/`** — Antarmuka Web GUI (HTML5, CSS3, ES6+ JavaScript murni; tanpa Node.js, npm, bundler, atau CDN).
- **`lib/gson-2.15.0.jar`** — Satu-satunya pustaka eksternal. Semua kode Java murni JDK SE.
- **`README.md`** — Dokumentasi ini.

Komponen yang tidak terlacak tidak ditampilkan di dokumentasi ini.

---
*Final Project — Kelompok 2, Pemrograman Jaringan [TIS31263]*
