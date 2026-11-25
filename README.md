# NetMonitor Enterprise — Kelompok 2, Pemrograman Jaringan (TIS31263)

Aplikasi client-server TCP/UDP berbasis Java untuk memantau node (Agent) dari satu konsol (Inspector).
Status: **CLI lengkap; Web GUI (INSPECTOR + AGENT) selesai** (lihat `docs/PRD_NetMonitor_Final.md` untuk rencana, keputusan desain, dan to-do).

## Struktur

```
src/netmonitor/common/   Protocol.java
src/netmonitor/dto/      GeoLocationDTO.java
src/netmonitor/util/     NetUtil.java, FileLogger.java
src/netmonitor/server/   NetMonitorServer.java, ClientHandler.java, AuthService.java, TelemetryService.java, DataTransferService.java, GeoIpService.java
src/netmonitor/client/   ClientEngine.java, ConsoleClientUI.java, ClientListener.java, ClientConfig.java, NetMonitorClient.java
src/netmonitor/web/      NetMonitorWebClient.java, WebBridgeServer.java
web/                     index.html, styles.css, app.js (frontend Web GUI, Vanilla JS)
lib/                     gson-2.15.0.jar (WAJIB di classpath server)
docs/                    PRD_NetMonitor_Final.md
DESIGN.md                Aturan tampilan Web GUI (wajib dibaca sebelum mengubah frontend)
AGENTS.md                Panduan kerja AI agent (aturan arsitektur & build)
tests/                   alat uji otomatis (Python 3; Linux/macOS dan Windows)
users.properties         akun (opsional; ada akun bawaan jika tidak ada)
```

## Kompilasi

Windows (PowerShell, per sub-package):

```powershell
javac -Xlint:all -cp lib\gson-2.15.0.jar -d out src\netmonitor\common\*.java
javac -Xlint:all -cp lib\gson-2.15.0.jar -d out src\netmonitor\dto\*.java
javac -Xlint:all -cp lib\gson-2.15.0.jar -d out src\netmonitor\util\*.java
javac -Xlint:all -cp lib\gson-2.15.0.jar -d out src\netmonitor\server\*.java
javac -Xlint:all -cp lib\gson-2.15.0.jar -d out src\netmonitor\client\*.java
javac -Xlint:all -d out -cp lib\gson-2.15.0.jar src\netmonitor\web\*.java
```

Linux/macOS (per sub-package):

```bash
javac -Xlint:all -cp lib/gson-2.15.0.jar -d out src/netmonitor/common/*.java
javac -Xlint:all -cp lib/gson-2.15.0.jar -d out src/netmonitor/dto/*.java
javac -Xlint:all -cp lib/gson-2.15.0.jar -d out src/netmonitor/util/*.java
javac -Xlint:all -cp lib/gson-2.15.0.jar -d out src/netmonitor/server/*.java
javac -Xlint:all -cp lib/gson-2.15.0.jar -d out src/netmonitor/client/*.java
javac -Xlint:all -d out -cp lib/gson-2.15.0.jar src/netmonitor/web/*.java
```

## Menjalankan (tiga terminal, dari folder utama proyek)

Windows:

```powershell
java -cp "out;lib\gson-2.15.0.jar" netmonitor.server.NetMonitorServer 9090
java -cp out netmonitor.client.NetMonitorClient localhost 9090 AGENT agent01 agent123
java -cp out netmonitor.client.NetMonitorClient localhost 9090 INSPECTOR admin admin123
```

Linux/macOS:

```bash
java -cp "out:lib/gson-2.15.0.jar" netmonitor.server.NetMonitorServer 9090
java -cp out netmonitor.client.NetMonitorClient localhost 9090 AGENT agent01 agent123
java -cp out netmonitor.client.NetMonitorClient localhost 9090 INSPECTOR admin admin123
```

Perintah Inspector (input langsung, satu baris per perintah):

- `LIST` — daftar pengguna aktif dengan peran, lokasi, dan ISP.
- `SYSINFO <agentId>` — informasi sistem dari Agent tertentu.
- `PING <host>` — cek keterjangkauan host.
- `RESOLVE <domain>` — resolusi DNS.
- `BROADCAST <teks>` — pesan ke semua klien (maks 500 karakter).
- `MSG <username> <teks>` — pesan privat antar pengguna (berfungsi untuk semua peran, maks 500 karakter).
- `EXEC <agentId> <namaPerintah>` — eksekusi perintah whitelist di Agent (`hostname`, `whoami`, `ipconfig`, `ifconfig`, `tasklist`, `ps`).
- `UPLOAD <pathLokal>` — unggah berkas patch ke server.
- `PUSH <agentId> <namaBerkas>` — kirim patch dari server ke Agent.
- `SUBSCRIBE <udpPort>` — mulai terima telemetri UDP.
- `UNSUBSCRIBE` — berhenti terima telemetri UDP.
- `WATCH` — tampilkan metrik terakhir semua Agent.
- `HELP` — daftar perintah.
- `QUIT` / `EXIT` — keluar.

Contoh:

```text
netmonitor> LIST
netmonitor> SYSINFO agent01
netmonitor> PING 8.8.8.8
netmonitor> RESOLVE example.com
netmonitor> BROADCAST halo semuanya
netmonitor> MSG agent01 pesan rahasia
netmonitor> EXEC agent01 hostname
netmonitor> UPLOAD C:\path\patch.bin
netmonitor> PUSH agent01 patch.bin
netmonitor> SUBSCRIBE 9091
netmonitor> WATCH
netmonitor> QUIT
```

Agent: ketik `quit` atau `exit` lalu Enter untuk keluar. Akun bawaan: `admin/admin123` (INSPECTOR), `agent01..03/agent123` (AGENT). Di VS Code gunakan tombol **Run Java** (bukan Code Runner).

## Web GUI (bonus)

Kompilasi paket `web` (lihat atas), lalu jalankan server dan jembatan web di dua terminal terpisah:

Windows:

```powershell
java -cp "out;lib\gson-2.15.0.jar" netmonitor.server.NetMonitorServer 9090
java -cp "out;lib\gson-2.15.0.jar" netmonitor.web.NetMonitorWebClient 8080
```

Linux/macOS:

```bash
java -cp "out:lib/gson-2.15.0.jar" netmonitor.server.NetMonitorServer 9090
java -cp "out:lib/gson-2.15.0.jar" netmonitor.web.NetMonitorWebClient 8080
```

Buka `http://localhost:8080` di browser. Login sebagai `INSPECTOR` atau `AGENT`. Untuk demo side-by-side, buka dua tab browser: satu login INSPECTOR dan satu login AGENT; Inspector dapat mengirim patch dan pesan, Agent menampilkan berkas yang diterima dan membalas pesan.

**Penting:** Frontend Web GUI mengikuti `DESIGN.md` — 15 aturan Anti‑AI Slop (tanpa gradient, bayangan, emoji, grid 4‑point, font `IBM Plex Sans`/`JetBrains Mono`, angka tabular‑nums, aksen hijau `#10B981` satu‑satunya). Kode soket berada hanya di `WebBridgeServer.java`, antarmuka dibuat dengan HTML5/CSS3/ES6+ tanpa Node.js, npm, bundler, maupun CDN eksternal. Tidak ada dependensi eksternal selain `lib/gson-2.15.0.jar` dan kelas bawaan JDK. JVM shutdown hook (`Ctrl+C`) menutup seluruh sesi dengan rapi.

## Fitur yang telah diimplementasikan

- Server memakai **thread pool** (`Executors.newFixedThreadPool(50)`), sesi di `ConcurrentHashMap`, penghitung klien `AtomicInteger`.
- **REST API + Gson**: lokasi IP (ip-api.com) dipetakan ke `GeoLocationDTO` dengan `@SerializedName`; lokasi node tampil di `LIST`.
- `LIST` untuk semua peran: `200 USERS [nama | PERAN | lokasi | isp ; ...]`.
- Heartbeat TCP (`HB`/`HBACK`), sambung ulang Agent, pesan error yang menyebut penyebab.
- Klien dipisah: `ClientEngine` (mesin jaringan, tanpa UI) + `ConsoleClientUI` (tampilan) agar GUI web dapat memakai mesin yang sama.
- `BROADCAST` dan `MSG` (Pilar 2, D8): pesan ke semua klien dan privat antar pengguna, batas 500 karakter, push `BCAST`/`MSG` diteruskan ke listener tanpa tertukar dengan jawaban perintah.
- `EXEC` (D9): remote command khusus Inspector; server memvalidasi whitelist dan meneruskan ke Agent (timeout server 12 dtk); Agent menjalankan perintah whitelist via `ProcessBuilder` tanpa shell (timeout eksekusi 10 dtk, output 2.800 karakter satu baris, penanda `... [output terpotong]` bila lebih panjang).
- Telemetri UDP (D10/D11): `TelemetryService` di server menerima datagram UDP dari Agent, menyimpan metrik per Agent, fan-out ke Inspector lewat `SUBSCRIBE <udpPort>`, deteksi stale dan perintah `WATCH`.
- Transfer berkas biner (Pilar 3): kanal data TCP, token sekali pakai, chunking 8 KB, `shutdownOutput()` tanpa menutup soket perintah.
- Tampilan CLI: pemisah visual dan ringkasan menu bernomor saat klien siap menerima perintah, input perintah langsung tetap didukung, pewarnaan ANSI untuk status dan header yang otomatis nonaktif dengan `-Dnetmonitor.cli.ansi=false`.

## Pengujian otomatis

Windows:

```powershell
cd tests\windows
python w1.py
python w2.py
python w3.py
python w4.py
python w5.py
python w6.py
python w7.py
python w8.py
python w9.py
python w10.py
```

Linux/macOS:

```bash
cd tests && python3 s1.py && python3 s1b.py && python3 s2.py && python3 c1.py && python3 r1.py
```

Skrip Windows di atas menguji fitur chat (`BROADCAST`/`MSG`), `EXEC`, batas panjang pesan, telemetri UDP, transfer berkas, sanitasi nama, dan batas ukuran. Skrip Linux/macOS menguji fondasi (thread pool, ketahanan, klien). Pengujian di beberapa laptop dan jaringan nyata **belum** dilakukan.