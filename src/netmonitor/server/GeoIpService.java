package netmonitor.server;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import netmonitor.common.Protocol;
import netmonitor.dto.GeoLocationDTO;
import netmonitor.util.FileLogger;
import netmonitor.util.NetUtil;

/**
 * Integrasi Web API publik GeoIP lewat HttpURLConnection (Pertemuan 4).
 * Dipanggil server saat LOGIN berhasil untuk mengetahui lokasi IP klien.
 *
 * Kegagalan API (internet putus, DNS mati, timeout, HTTP error, JSON tak terduga) TIDAK menggagalkan
 * login. Hasilnya tetap berupa teks lokasi, disertai ALASAN yang jelas, mis.
 *   "tidak diketahui (tidak ada koneksi internet atau DNS tidak tersedia)".
 */
public final class GeoIpService {

    public static final String UNKNOWN = "tidak diketahui";
    public static final String LOCAL = "Jaringan Lokal";

    private static final String DEFAULT_URL =
            "http://ip-api.com/json/%s?fields=status,message,country,countryCode,regionName,city,isp";
    private static final int TIMEOUT_MS = 3000;       // timeout sambung dan baca (masing-masing)
    private static final int DEADLINE_MS = 4000;      // batas TOTAL satu pencarian lokasi (termasuk DNS)
    private static final int MAX_BODY_BYTES = 8192;
    private static final Gson GSON = new Gson();   // pemetaan JSON -> POJO

    private final String urlTemplate;
    private final boolean lookupPrivate;
    private final FileLogger log;

    /**
     * Hasil satu pencarian lokasi: lokasi (jika berhasil) atau alasan kegagalan.
     * Disimpan sebagai Outcome karena kegagalan tidak menghentikan sesi login.
     */
    private static final class Outcome {
        /** Lokasi hasil pencarian (null bila gagal). */
        final String location;
        /** DTO hasil pencarian (null bila gagal). */
        final GeoLocationDTO dto;
        /** Alasan kegagalan (null bila berhasil). */
        final String reason;

        /**
         * @param location hasil lokasi atau null
         * @param dto      DTO hasil pencarian atau null
         * @param reason   alasan kegagalan atau null
         */
        private Outcome(String location, GeoLocationDTO dto, String reason) {
            this.location = location;
            this.dto = dto;
            this.reason = reason;
        }

        /**
         * Membuat hasil pencarian yang berhasil.
         *
         * @param location lokasi yang berhasil ditemukan
         * @param dto      DTO hasil pencarian
         * @return Outcome hasil berhasil
         */
        static Outcome ok(String location, GeoLocationDTO dto) {
            return new Outcome(location, dto, null);
        }

        /**
         * Membuat hasil pencarian yang gagal.
         *
         * @param reason alasan kegagalan
         * @return Outcome hasil gagal
         */
        static Outcome fail(String reason) {
            return new Outcome(null, null, reason);
        }
    }

    /**
     * Hasil pencarian lokasi yang dibawa sepanjang sesi klien.
     * display = teks untuk respons LOGIN; node/region/isp = keterangan "node" yang tampil pada perintah LIST.
     */
    public static final class Result {
        /** Teks ringkas untuk respons LOGIN. */
        private final String display;
        /** Nama region atau lokasi untuk ditampilkan pada LIST. */
        private final String node;
        /** Penyedia internet node. */
        private final String isp;

        /**
         * @param display teks ringkas untuk respons LOGIN
         * @param node    nama region/lokasi untuk LIST
         * @param isp     penyedia internet node
         */
        Result(String display, String node, String isp) {
            this.display = display;
            this.node = node;
            this.isp = isp;
        }

        /** @return teks ringkas untuk respons LOGIN. */
        public String getDisplay() {
            return display;
        }

        /** @return nama region atau lokasi untuk LIST. */
        public String getNode() {
            return node;
        }

        /** @return penyedia internet node. */
        public String getIsp() {
            return isp;
        }
    }

    /**
     * Membuat layanan GeoIP.
     *
     * @param log pencatat log server
     */
    public GeoIpService(FileLogger log) {
        this.log = log;
        // dapat diganti saat pengujian: -Dnetmonitor.geoip.url=http://localhost:8000/%s
        this.urlTemplate = System.getProperty("netmonitor.geoip.url", DEFAULT_URL);
        this.lookupPrivate = Boolean.getBoolean("netmonitor.geoip.lookupPrivate");
    }

    /**
     * Mencari lokasi klien. Hasil memuat teks untuk LOGIN dan keterangan node untuk LIST.
     * Tidak pernah melempar exception; kegagalan dilaporkan sebagai teks alasan.
     *
     * @param address alamat IP klien
     * @return hasil pencarian lokasi (berhasil atau gagal dengan alasan)
     */
    public Result lookup(InetAddress address) {
        boolean local = NetUtil.isLocal(address);
        String realIp = address.getHostAddress().split("%")[0];

        // Klien lokal (127.0.0.1 / 192.168.x.x) tidak punya lokasi publik. Kirim IP kosong agar API
        // membaca IP publik jaringan ini; kecuali lookupPrivate=true (mode uji: kirim IP aslinya).
        String ipToQuery = local ? (lookupPrivate ? realIp : "") : realIp;

        Outcome o = queryWithDeadline(ipToQuery);
        if (o.location != null) {
            String display = (local && !lookupPrivate) ? o.location + " (via IP publik jaringan)" : o.location;
            String region = Protocol.clean(o.dto.getRegion(), 64);
            String node = region.isEmpty() ? o.location
                    : o.location.replaceFirst(", ([^,]*)$", ", " + java.util.regex.Matcher.quoteReplacement(region) + ", $1");
            return new Result(display, node, Protocol.clean(o.dto.getIsp(), 64));
        }
        log.warn("GeoIP gagal untuk " + (ipToQuery.isEmpty() ? "IP publik jaringan" : ipToQuery) + ": " + o.reason);
        String display = local ? LOCAL + " (lokasi publik tidak tersedia: " + o.reason + ")"
                               : UNKNOWN + " (" + o.reason + ")";
        return new Result(display, local ? LOCAL : UNKNOWN, "");
    }

    /**
     * Membatasi total waktu pencarian, karena DNS bisa macet di luar kendali timeout HTTP.
     *
     * @param ip alamat IP yang akan dicari (boleh kosong)
     * @return hasil pencarian (berhasil atau gagal)
     */
    private Outcome queryWithDeadline(String ip) {
        try {
            return NetUtil.callWithTimeout(() -> query(ip), DEADLINE_MS);
        } catch (TimeoutException e) {
            return Outcome.fail("layanan lokasi tidak merespons dalam " + (DEADLINE_MS / 1000)
                    + " detik (internet putus atau DNS lambat)");
        } catch (Exception e) {
            return Outcome.fail("kesalahan tak terduga: " + e.getClass().getSimpleName());
        }
    }

    /**
     * Menangani proses HTTP, membaca JSON, dan mengklasifikasikan kegagalan.
     *
     * @param ip alamat IP yang akan dikirim ke layanan GeoIP
     * @return hasil pencarian (berhasil atau gagal dengan alasan yang jelas)
     */
    private Outcome query(String ip) {
        HttpURLConnection conn = null;
        try {
            URL url = URI.create(String.format(urlTemplate, ip)).toURL();
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(TIMEOUT_MS);
            conn.setReadTimeout(TIMEOUT_MS);
            conn.setRequestProperty("User-Agent", "NetMonitor/1.0");
            conn.setRequestProperty("Accept", "application/json");

            int status = conn.getResponseCode();
            if (status != HttpURLConnection.HTTP_OK) {
                // Baca respons kesalahan dari getErrorStream() untuk detail lebih lanjut
                String errorBody = null;
                try (InputStream errIn = conn.getErrorStream()) {
                    if (errIn != null) {
                        errorBody = new String(errIn.readNBytes(MAX_BODY_BYTES), StandardCharsets.UTF_8);
                    }
                } catch (IOException ignored) {
                }
                String detail = errorBody != null && errorBody.length() > 0
                        ? (" respons: " + errorBody.substring(0, Math.min(errorBody.length(), 200)))
                        : "";
                return Outcome.fail(describeHttp(status) + detail);
            }
            String body;
            try (InputStream in = conn.getInputStream()) {
                body = new String(in.readNBytes(MAX_BODY_BYTES), StandardCharsets.UTF_8);
            }
            GeoLocationDTO geo;
            try {
                geo = GSON.fromJson(body, GeoLocationDTO.class);   // JSON -> objek POJO
            } catch (JsonParseException e) {
                geo = null;                                     // bukan JSON yang valid
            }
            if (geo == null || geo.getStatus() == null) {
                return Outcome.fail("respons layanan lokasi tidak valid (bukan format JSON yang dikenal)");
            }
            if (!geo.isSuccess()) {
                String msg = Protocol.clean(geo.getMessage(), 60);
                return Outcome.fail("layanan lokasi tidak dapat menentukan lokasi" + (msg.isEmpty() ? "" : " (" + msg + ")"));
            }
            String city = Protocol.clean(geo.getCity(), 64);
            String code = Protocol.clean(geo.getCountryCode(), 8);
            String country = Protocol.clean(geo.getCountry(), 64);
            String place = !city.isEmpty() ? city : country;
            String result = !place.isEmpty() && !code.isEmpty() ? place + ", " + code : place;
            return result.isEmpty() ? Outcome.fail("respons layanan lokasi tidak berisi lokasi") : Outcome.ok(result, geo);
        } catch (UnknownHostException e) {
            return Outcome.fail("tidak ada koneksi internet atau DNS tidak tersedia");
        } catch (SocketTimeoutException e) {
            return Outcome.fail("layanan lokasi tidak merespons (timeout " + (TIMEOUT_MS / 1000) + " detik)");
        } catch (NoRouteToHostException e) {
            return Outcome.fail("tidak ada rute ke layanan lokasi (internet putus)");
        } catch (ConnectException e) {
            String m = String.valueOf(e.getMessage()).toLowerCase();
            if (m.contains("refused")) {
                return Outcome.fail("layanan lokasi menolak koneksi");
            }
            return Outcome.fail("tidak dapat menyambung ke layanan lokasi (internet putus atau diblokir)");
        } catch (SocketException e) {
            String m = String.valueOf(e.getMessage()).toLowerCase();
            if (m.contains("unreachable") || m.contains("network is down")) {
                return Outcome.fail("jaringan tidak tersedia (Wi-Fi/Ethernet terputus)");
            }
            return Outcome.fail("koneksi ke layanan lokasi terputus (" + e.getClass().getSimpleName() + ")");
        } catch (IOException e) {
            return Outcome.fail("gangguan jaringan saat menghubungi layanan lokasi (" + e.getClass().getSimpleName() + ")");
        } catch (IllegalArgumentException e) {
            return Outcome.fail("alamat layanan lokasi tidak valid");
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * Mendeskripsikan kode respons HTTP dari layanan GeoIP.
     *
     * @param status kode respons HTTP
     * @return deskripsi singkat kode tersebut
     */
    private static String describeHttp(int status) {
        if (status == 429) {
            return "batas permintaan layanan lokasi terlampaui (HTTP 429)";
        }
        if (status == 403) {
            return "layanan lokasi menolak atau memblokir akses (HTTP 403)";
        }
        if (status >= 500) {
            return "layanan lokasi sedang bermasalah (HTTP " + status + ")";
        }
        return "layanan lokasi menjawab HTTP " + status;
    }
}
