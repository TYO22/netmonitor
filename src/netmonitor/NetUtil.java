package netmonitor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilitas jaringan berbasis InetAddress (Pertemuan 3):
 * validasi format host, resolusi nama domain, uji jangkauan (isReachable),
 * serta diagnosis "apakah internet tersedia?" untuk membedakan penyebab kegagalan.
 */
public final class NetUtil {

    private NetUtil() { }

    private static final Pattern IPV4 = Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$");
    private static final Pattern IPV6 = Pattern.compile("^[0-9A-Fa-f:.]{2,45}$");
    private static final Pattern LABEL = Pattern.compile("^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$");

    /** Pekerja latar belakang (daemon) untuk operasi yang bisa macet tanpa batas waktu, mis. DNS. */
    private static final ThreadPoolExecutor POOL = new ThreadPoolExecutor(0, 32, 30, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(), r -> {
                Thread t = new Thread(r, "netutil-worker");
                t.setDaemon(true);
                return t;
            });

    private static final long NET_CACHE_MS = 5_000;
    private static final int NET_CHECK_TIMEOUT_MS = 1_200;
    private static volatile long netCheckedAt;
    private static volatile boolean netAvailable;

    /** Resolusi DNS melewati batas waktu (DNS macet, jaringan putus, atau server DNS tidak menjawab). */
    public static class DnsTimeoutException extends IOException {
        private static final long serialVersionUID = 1L;

        public DnsTimeoutException(String host, int timeoutMs) {
            super("DNS tidak menjawab dalam " + (timeoutMs / 1000) + " detik untuk " + host);
        }
    }

    /** Hasil uji jangkauan host. */
    public static final class PingResult {
        public final InetAddress address;
        public final boolean reachable;
        public final long millis;

        PingResult(InetAddress address, boolean reachable, long millis) {
            this.address = address;
            this.reachable = reachable;
            this.millis = millis;
        }
    }

    /**
     * Memeriksa FORMAT host saja (tanpa DNS): IPv4, literal IPv6, atau nama host RFC 1123.
     * Dicek lebih dulu agar input aneh tidak sampai ke resolver DNS.
     */
    public static boolean isValidFormat(String host) {
        if (host == null || host.isEmpty() || host.length() > 253) {
            return false;
        }
        Matcher m = IPV4.matcher(host);
        if (m.matches()) {
            for (int i = 1; i <= 4; i++) {
                if (Integer.parseInt(m.group(i)) > 255) {
                    return false;
                }
            }
            return true;
        }
        if (host.indexOf(':') >= 0) {
            return IPV6.matcher(host).matches();
        }
        String[] labels = host.split("\\.", -1);
        for (String label : labels) {
            if (!LABEL.matcher(label).matches()) {
                return false;
            }
        }
        // label terakhir tidak boleh angka semua (mencegah "1.2.3" yang ditafsirkan sebagai IP)
        return !labels[labels.length - 1].matches("\\d+");
    }

    /** Resolusi IP/domain menjadi InetAddress (tanpa batas waktu dari aplikasi). */
    public static InetAddress resolve(String host) throws UnknownHostException {
        return InetAddress.getByName(host);
    }

    /** Menjalankan tugas di pekerja latar belakang dengan batas waktu. Tugas yang macet tidak menahan pemanggil. */
    public static <T> T callWithTimeout(Callable<T> task, long timeoutMs) throws Exception {
        Future<T> future;
        try {
            future = POOL.submit(task);
        } catch (RejectedExecutionException e) {
            throw new TimeoutException("pekerja jaringan sedang penuh");
        }
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }

    /**
     * Resolusi DNS dengan batas waktu.
     *
     * @throws UnknownHostException nama tidak dikenal (atau DNS menjawab "tidak ada")
     * @throws DnsTimeoutException  DNS tidak menjawab tepat waktu
     */
    public static InetAddress resolveWithTimeout(String host, int timeoutMs) throws IOException {
        try {
            return callWithTimeout(() -> InetAddress.getByName(host), timeoutMs);
        } catch (TimeoutException e) {
            throw new DnsTimeoutException(host, timeoutMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DnsTimeoutException(host, timeoutMs);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Resolusi DNS gagal: " + e.getMessage(), e);
        }
    }

    /** Menguji jangkauan host dengan InetAddress.isReachable(); resolusi DNS dibatasi dnsTimeoutMs. */
    public static PingResult ping(String host, int pingTimeoutMs, int dnsTimeoutMs) throws IOException {
        InetAddress address = resolveWithTimeout(host, dnsTimeoutMs);
        long start = System.nanoTime();
        boolean reachable = address.isReachable(pingTimeoutMs);
        long millis = (System.nanoTime() - start) / 1_000_000L;
        return new PingResult(address, reachable, millis);
    }

    public static PingResult ping(String host, int timeoutMs) throws IOException {
        return ping(host, timeoutMs, 4_000);
    }

    /** True untuk alamat loopback / jaringan privat yang tidak dikenal oleh layanan GeoIP publik. */
    public static boolean isLocal(InetAddress a) {
        return a.isLoopbackAddress() || a.isSiteLocalAddress()
                || a.isLinkLocalAddress() || a.isAnyLocalAddress();
    }

    /**
     * Diagnosis sederhana: apakah komputer ini punya akses internet? Mencoba membuka koneksi TCP ke
     * beberapa alamat IP publik (tanpa DNS). Hasilnya disimpan 5 detik agar tidak membebani jaringan.
     * Daftar target dapat diganti dengan properti -Dnetmonitor.internet.check=host:port,host:port
     */
    public static boolean isInternetAvailable() {
        long now = System.currentTimeMillis();
        if (now - netCheckedAt < NET_CACHE_MS) {
            return netAvailable;
        }
        boolean ok = false;
        String targets = System.getProperty("netmonitor.internet.check", "1.1.1.1:443,8.8.8.8:53");
        for (String t : targets.split(",")) {
            String[] hp = t.trim().split(":");
            if (hp.length != 2) {
                continue;
            }
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(hp[0], Integer.parseInt(hp[1])), NET_CHECK_TIMEOUT_MS);
                ok = true;
                break;
            } catch (IOException | RuntimeException e) {
                // coba target berikutnya
            }
        }
        netAvailable = ok;
        netCheckedAt = System.currentTimeMillis();
        return ok;
    }
}
