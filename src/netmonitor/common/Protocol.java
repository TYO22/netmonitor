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

    private Protocol() { }

    public static final int DEFAULT_PORT = 9090;
    public static final int MAX_LINE_LENGTH = 1024;
    public static final String EOL = "\r\n";

    /** Sinyal "masih hidup": server mengirim HB, klien membalas HBACK (deteksi koneksi putus diam-diam). */
    public static final String HEARTBEAT = "HB";
    public static final String HEARTBEAT_ACK = "HBACK";

    public static final String ROLE_AGENT = "AGENT";
    public static final String ROLE_INSPECTOR = "INSPECTOR";

    private static final Pattern USERNAME = Pattern.compile("^[A-Za-z0-9_.-]{1,32}$");

    /** Dilempar bila satu baris melebihi MAX_LINE_LENGTH (perlindungan memori/serangan). */
    public static class LineTooLongException extends IOException {
        private static final long serialVersionUID = 1L;

        public LineTooLongException() {
            super("Baris melebihi " + MAX_LINE_LENGTH + " karakter");
        }
    }

    public static boolean isValidUsername(String username) {
        return username != null && USERNAME.matcher(username).matches();
    }

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

    /** Respons sukses, contoh: ok("BYE Sesi Berhasil Ditutup") -> "200 BYE Sesi Berhasil Ditutup". */
    public static String ok(String text) {
        return "200 " + text;
    }

    /** Respons gagal, contoh: err(404, "Agent Tidak Ditemukan") -> "404 ERR Agent Tidak Ditemukan". */
    public static String err(int code, String message) {
        return code + " ERR " + message;
    }

    /** Membersihkan karakter kontrol (termasuk CR/LF) dan memotong panjang teks dari sumber luar. */
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
     * Pembaca baris yang menyimpan sisa baris di antara pemanggilan. Berbeda dengan readLine() statis,
     * sebuah SocketTimeoutException TIDAK membuang data setengah baris yang sudah terbaca, sehingga
     * soket boleh diberi timeout pendek (untuk pemeriksaan berkala) tanpa merusak pesan.
     */
    public static final class LineReader {
        private final BufferedReader in;
        private final StringBuilder buf = new StringBuilder();

        public LineReader(BufferedReader in) {
            this.in = in;
        }

        /** @return satu baris, atau null jika koneksi ditutup lawan. */
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

    /** Menulis satu baris diakhiri "\r\n" lalu flush. */
    public static void writeLine(BufferedWriter out, String line) throws IOException {
        out.write(line.replaceAll("[\\r\\n]", " "));
        out.write(EOL);
        out.flush();
    }
}
