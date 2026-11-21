package netmonitor.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Custom Application Protocol NetMonitor (Bab III proposal).
 *
 * Aturan pesan:
 *  - Encoding   : UTF-8
 *  - Delimiter  : CR LF ("\r\n") di setiap akhir baris
 *  - Perintah   : COMMAND ARG1 ARG2 ...
 *  - Respons    : KODE [ERR] pesan   (2xx sukses, 4xx salah klien, 5xx salah server)
 */
public final class Protocol {

    /** Konstruktor privat; kelas ini hanya berisi konstanta dan method statis. */
    private Protocol() { }

    /** Port TCP bawaan server NetMonitor. */
    public static final int DEFAULT_PORT = 9090;
    /** Batas maksimum panjang satu baris protokol untuk melindungi memori dari input berlebihan. */
    public static final int MAX_LINE_LENGTH = 1024;
    /** Penanda akhir baris protokol (CR LF). */
    public static final String EOL = "\r\n";

    /** Sinyal "masih hidup": server mengirim HB, klien membalas HBACK (deteksi koneksi putus diam-diam). */
    public static final String HEARTBEAT = "HB";
    /** Balasan atas sinyal {@link #HEARTBEAT} yang dikirim klien ke server. */
    public static final String HEARTBEAT_ACK = "HBACK";

    /** Nilai peran untuk node Agent (pemantau jarak jauh). */
    public static final String ROLE_AGENT = "AGENT";
    /** Nilai peran untuk Inspector (pengguna yang mengawasi Agent). */
    public static final String ROLE_INSPECTOR = "INSPECTOR";

    /** Pola validasi username: 1-32 karakter dari huruf, angka, titik, garis bawah, atau strip. */
    private static final Pattern USERNAME = Pattern.compile("^[A-Za-z0-9_.-]{1,32}$");

    /** Exception yang dilempar ketika satu baris melebihi {@link #MAX_LINE_LENGTH}. */
    public static class LineTooLongException extends IOException {
        private static final long serialVersionUID = 1L;

        /** Konstruktor yang menetapkan pesan bawaan berdasarkan batas panjang baris. */
        public LineTooLongException() {
            super("Baris melebihi " + MAX_LINE_LENGTH + " karakter");
        }
    }

    /**
     * Memeriksa apakah username memenuhi pola {@code ^[A-Za-z0-9_.-]{1,32}$}.
     *
     * @param username username yang akan divalidasi
     * @return {@code true} bila username valid, {@code false} jika tidak
     */
    public static boolean isValidUsername(String username) {
        return username != null && USERNAME.matcher(username).matches();
    }

    /**
     * Memeriksa apakah peran termasuk AGENT atau INSPECTOR.
     *
     * @param role peran yang akan divalidasi
     * @return {@code true} bila peran valid, {@code false} jika tidak
     */
    public static boolean isValidRole(String role) {
        return ROLE_AGENT.equals(role) || ROLE_INSPECTOR.equals(role);
    }

    /**
     * Memeriksa apakah sebuah nama berkas aman dipakai pada transfer berkas.
     * Nama ditolak bila kosong, mengandung pemisah direktori ({@code /} atau {@code \}),
     * atau mengandung rangkaian dua titik ({@code ..}) yang dapat dipakai untuk path traversal.
     *
     * @param name nama berkas yang akan diperiksa
     * @return true bila nama berkas aman
     */
    public static boolean isSafeFilename(String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        if (name.contains("/") || name.contains("\\")) {
            return false;
        }
        return !name.contains("..");
    }

    /**
     * Membangun respons sukses dengan kode 200.
     *
     * @param text isi pesan, misalnya {@code "BYE Sesi Berhasil Ditutup"}
     * @return baris respons berbentuk {@code 200 <text>}
     */
    public static String ok(String text) {
        return "200 " + text;
    }

    /**
     * Membangun respons gagal dengan kode tertentu dan awalan ERR.
     *
     * @param code    kode respons (mis. 404, 500)
     * @param message pesan penjelas kegagalan
     * @return baris respons berbentuk {@code <code> ERR <message>}
     */
    public static String err(int code, String message) {
        return code + " ERR " + message;
    }

    /**
     * Membersihkan karakter kontrol (termasuk CR/LF) dan memotong panjang teks dari sumber luar.
     *
     * @param text      teks mentah yang akan dibersihkan
     * @param maxLength panjang maksimum hasil setelah dibersihkan
     * @return teks bersih dengan panjang maksimal {@code maxLength}, atau string kosong bila input null
     */
    public static String clean(String text, int maxLength) {
        if (text == null) {
            return "";
        }
        String cleaned = text.replaceAll("\\p{Cntrl}", " ").trim();
        return cleaned.length() > maxLength ? cleaned.substring(0, maxLength) : cleaned;
    }

    /**
     * Membaca satu baris protokol. CR diabaikan sehingga "\r\n" maupun "\n" diterima.
     * Mengembalikan null bila stream sudah ditutup lawan (EOF).
     *
     * @param in pembaca sumber karakter
     * @return baris yang dibaca tanpa terminator, atau {@code null} jika EOF tanpa data
     * @throws IOException            bila terjadi kesalahan I/O
     * @throws LineTooLongException   bila panjang baris melebihi {@link #MAX_LINE_LENGTH}
     */
    public static String readLine(BufferedReader in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\n') {
                return sb.toString();
            }
            if (c == '\r') {
                continue;
            }
            if (sb.length() >= MAX_LINE_LENGTH) {
                throw new LineTooLongException();
            }
            sb.append((char) c);
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    /**
     * Pembaca baris yang menyimpan sisa baris di antara pemanggilan. Berbeda dengan {@link #readLine(BufferedReader)} statis,
     * sebuah {@link java.net.SocketTimeoutException} TIDAK membuang data setengah baris yang sudah terbaca, sehingga
     * soket boleh diberi timeout pendek (untuk pemeriksaan berkala) tanpa merusak pesan.
     */
    public static final class LineReader {
        /** Pembaca karakter sumber. */
        private final BufferedReader in;
        /** Penampung sisa karakter yang sudah dibaca namun belum membentuk satu baris penuh. */
        private final StringBuilder buf = new StringBuilder();

        /**
         * Membuat pembaca baris berbasis buffer.
         *
         * @param in pembaca karakter sumber (misalnya dari soket)
         */
        public LineReader(BufferedReader in) {
            this.in = in;
        }

        /**
         * Membaca satu baris lengkap dari sumber.
         *
         * @return satu baris tanpa terminator, atau {@code null} bila koneksi ditutup lawan (EOF)
         * @throws IOException          bila terjadi kesalahan I/O
         * @throws LineTooLongException bila panjang baris melebihi {@link #MAX_LINE_LENGTH}
         */
        public String readLine() throws IOException {
            int c;
            while ((c = in.read()) != -1) {
                if (c == '\n') {
                    String line = buf.toString();
                    buf.setLength(0);
                    return line;
                }
                if (c == '\r') {
                    continue;
                }
                if (buf.length() >= MAX_LINE_LENGTH) {
                    buf.setLength(0);
                    throw new LineTooLongException();
                }
                buf.append((char) c);
            }
            if (buf.length() == 0) {
                return null;
            }
            String line = buf.toString();
            buf.setLength(0);
            return line;
        }
    }

    /**
     * Menulis satu baris diakhiri "\r\n" lalu flush.
     *
     * @param out  penulis tujuan
     * @param line isi baris; karakter CR/LF di dalamnya diganti spasi agar protokol tidak rusak
     * @throws IOException bila terjadi kesalahan I/O
     */
    public static void writeLine(BufferedWriter out, String line) throws IOException {
        out.write(line.replaceAll("[\\r\\n]", " "));
        out.write(EOL);
        out.flush();
    }
}
