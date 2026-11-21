package netmonitor.dto;

import com.google.gson.annotations.SerializedName;

/**
 * DTO (Data Transfer Object / POJO) yang memetakan JSON balasan REST API GeoIP (ip-api.com).
 *
 * Pilar 4 (Final): JSON dideserialisasi otomatis oleh Gson lewat {@code gson.fromJson(json, GeoLocationDTO.class)}.
 * Anotasi {@link SerializedName} menghubungkan KUNCI JSON dengan field Java. Anotasi ini wajib bila nama
 * field Java berbeda dari kunci JSON (contoh: kunci "regionName" dipetakan ke field {@code region}), dan
 * dipasang pada semua field agar pemetaannya eksplisit. Kunci JSON yang tidak ada di kelas ini diabaikan.
 */
public class GeoLocationDTO {

    /** Status respons dari API GeoIP: "success" atau "fail". */
    @SerializedName("status")
    private String status;        // "success" atau "fail"

    /** Pesan penjelasan bila status = "fail". */
    @SerializedName("message")
    private String message;       // alasan bila status = "fail"

    /** Kode nama negara (mis. "ID" untuk Indonesia). */
    @SerializedName("country")
    private String country;

    /** Kode negara dua huruf (mis. "ID"). */
    @SerializedName("countryCode")
    private String countryCode;

    /** Nama region atau provinsi (contoh: "Jawa Barat"). */
    @SerializedName("regionName")
    private String region;        // nama field Java berbeda dari kunci JSON

    /** Nama kota. */
    @SerializedName("city")
    private String city;

    /** Penyedia layanan internet (contoh: "Telkomsel"). */
    @SerializedName("isp")
    private String isp;           // penyedia internet (membantu menjelaskan mengapa lokasi IP bisa meleset)

    /**
     * Konstruktor kosong.
     * Dibutuhkan Gson untuk melakukan deserialisasi JSON menjadi objek ini.
     */
    public GeoLocationDTO() {
        // konstruktor kosong dibutuhkan Gson
    }

    /** @return status respons API GeoIP. */
    public String getStatus() { return status; }

    /** @param status status respons yang akan diset. */
    public void setStatus(String status) { this.status = status; }

    /** @return pesan penjelas bila status = "fail". */
    public String getMessage() { return message; }

    /** @param message pesan penjelas yang akan diset. */
    public void setMessage(String message) { this.message = message; }

    /** @return nama negara. */
    public String getCountry() { return country; }

    /** @param country nama negara yang akan diset. */
    public void setCountry(String country) { this.country = country; }

    /** @return kode negara dua huruf. */
    public String getCountryCode() { return countryCode; }

    /** @param countryCode kode negara yang akan diset. */
    public void setCountryCode(String countryCode) { this.countryCode = countryCode; }

    /** @return nama region atau provinsi. */
    public String getRegion() { return region; }

    /** @param region nama region yang akan diset. */
    public void setRegion(String region) { this.region = region; }

    /** @return nama kota. */
    public String getCity() { return city; }

    /** @param city nama kota yang akan diset. */
    public void setCity(String city) { this.city = city; }

    /** @return penyedia layanan internet. */
    public String getIsp() { return isp; }

    /** @param isp penyedia layanan yang akan diset. */
    public void setIsp(String isp) { this.isp = isp; }

    /** @return true bila API menjawab status "success". */
    public boolean isSuccess() {
        return "success".equals(status);
    }

    /** @return representasi teks dari objek ini. */
    @Override
    public String toString() {
        return "GeoLocationDTO{status=" + status + ", city=" + city + ", region=" + region
                + ", countryCode=" + countryCode + ", isp=" + isp + "}";
    }
}
