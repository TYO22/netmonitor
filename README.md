# NetMonitor Enterprise — Kelompok 2, Pemrograman Jaringan (TIS31263)

Aplikasi client-server TCP berbasis Java untuk memantau node (Agent) dari satu konsol (Inspector).
Status: **tahap komunikasi, remote command, dan telemetri UDP selesai** (lihat `docs/PRD_NetMonitor_Final.md` untuk rencana, keputusan desain, dan to-do).

## Struktur
```
src/netmonitor/   kode sumber (package netmonitor)
lib/              gson-2.15.0.jar (WAJIB di classpath server)
docs/             PRD_NetMonitor_Final.md
tests/            alat uji otomatis (Python 3, untuk Linux/macOS)
users.properties  akun (opsional; ada akun bawaan jika tidak ada)
```

## Kompilasi
```
javac -cp lib/gson-2.15.0.jar -d out src/netmonitor/*.java
```
(Windows: `lib\gson-2.15.0.jar` dan `src\netmonitor\*.java`.)

## Menjalankan (tiga terminal, dari folder utama proyek)
```
java -cp "out;lib/gson-2.15.0.jar" netmonitor.NetMonitorServer 9090       (Linux/macOS: ganti ; dengan :)
java -cp out netmonitor.NetMonitorClient localhost 9090 AGENT agent01 agent123
java -cp out netmonitor.NetMonitorClient localhost 9090 INSPECTOR admin admin123
```
Perintah Inspector: `LIST`, `SYSINFO <agent>`, `PING <host>`, `RESOLVE <domain>`, `BROADCAST <teks>`, `MSG <username> <teks>`, `EXEC <agentId> <namaPerintah>`, `QUIT`. Agent: ketik `quit` atau `exit`. Ketik `HELP` untuk daftar perintah. Akun bawaan: `admin/admin123` (INSPECTOR), `agent01..03/agent123` (AGENT).
Di VS Code gunakan tombol **Run Java** (bukan Code Runner). Akun bawaan: `admin/admin123` (INSPECTOR), `agent01..03/agent123` (AGENT).

## Fitur yang telah diimplementasikan
- Server memakai **thread pool** (`Executors.newFixedThreadPool(50)`), sesi di `ConcurrentHashMap`, penghitung klien `AtomicInteger`.
- **REST API + Gson**: lokasi IP (ip-api.com) dipetakan ke `GeoLocationDTO` dengan `@SerializedName`; lokasi node tampil di `LIST`.
- `LIST` untuk semua peran: `200 USERS [nama | PERAN | lokasi | isp ; ...]`.
- Heartbeat TCP (`HB`/`HBACK`), sambung ulang Agent, pesan error yang menyebut penyebab.
- Klien dipisah: `ClientEngine` (mesin jaringan, tanpa UI) + `ConsoleClientUI` (tampilan) agar GUI web dapat memakai mesin yang sama.
- `BROADCAST` dan `MSG` (Pilar 2, D8): pesan ke semua klien dan privat antar pengguna, batas 500 karakter, push `BCAST`/`MSG` diteruskan ke listener tanpa tertukar dengan jawaban perintah.
- `EXEC` (D9): remote command khusus Inspector; server memvalidasi whitelist dan meneruskan ke Agent (timeout server 12 dtk); Agent menjalankan perintah whitelist via `ProcessBuilder` tanpa shell (timeout eksekusi 10 dtk, output 4 KB satu baris).
- Telemetri UDP (D10/D11): `TelemetryService` di server menerima datagram UDP dari Agent, menyimpan metrik per Agent, fan-out ke Inspector lewat `SUBSCRIBE <udpPort>`, deteksi stale dan perintah `WATCH`.

## Pengujian otomatis
```
javac -cp lib/gson-2.15.0.jar -d out src/netmonitor/*.java
cd tests && python3 s1.py && python3 s1b.py && python3 s2.py && python3 c1.py && python3 r1.py
```
Skrip Linux/macOS di atas menguji fondasi (thread pool, ketahanan, klien). Skrip Windows `tests/windows/w1.py`–`w5.py` menguji fitur chat (`BROADCAST`/`MSG`) dan `EXEC` serta batas panjang pesan; semuanya lolos (44 pemeriksaan) di Windows. Pengujian di beberapa laptop dan jaringan nyata **belum** dilakukan.
