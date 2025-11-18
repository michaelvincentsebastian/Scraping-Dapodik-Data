import os
import time

# ==========================================================
# IMPORT FUNGSI DARI MODUL KHUSUS (dapodik_utils.py)
# ==========================================================
# Ambil semua konstanta dan fungsi yang dibutuhkan dari file dapodik_utils.py
from dapodik_utils import (
    SEMESTER_ID, 
    request_api, 
    create_csv_header, 
    load_processed_ids, 
    parse_html, 
    append_to_csv
)
# Anda tidak perlu mengimport requests, bs4, csv, dll., lagi karena 
# sudah diurus di dalam dapodik_utils.py

# =========================
# MAIN
# =========================
def main():
    # --- PENGATURAN TARGET (KOTA BEKASI) ---
    target_kota_keywords = ["BEKASI"] 

    # Nama file CSV (SUDAH DIPASTIKAN KE DATA KOTA BEKASI)
    csv_filename = os.path.join('result', 'data_Bekasi.csv')
    # --------------------------------------

    # PANGGIL FUNGSI DARI dapodik_utils.py
    create_csv_header(csv_filename)

    # PANGGIL FUNGSI DARI dapodik_utils.py
    processed_ids = load_processed_ids(csv_filename)
    print(f"File target: {csv_filename}")
    print(f"Sudah ada {len(processed_ids)} sekolah di CSV, akan di-skip.")

    # ambil semua provinsi (level 0)
    print("=== AMBIL DAFTAR PROVINSI ===")
    # PANGGIL FUNGSI DARI dapodik_utils.py
    provinsi_list = request_api(backoff=3) # level 0

    for province in provinsi_list:
        # Langsung target Jawa Barat, tempat Bekasi berada, untuk efisiensi
        if "JAWA BARAT" not in province['nama'].upper():
            print(f"\nPROVINSI: {province['nama']} (Skip)")
            continue

        print(f"\nPROVINSI: {province['nama']} (Target)")
        level_wilayah_prov = province['id_level_wilayah']
        kode_wilayah_prov = province['kode_wilayah'].strip()

        # ambil semua kota/kab di provinsi ini
        # PANGGIL FUNGSI DARI dapodik_utils.py
        response_kota = request_api(
            level_wilayah=level_wilayah_prov,
            kode_wilayah=kode_wilayah_prov,
            semester_id=SEMESTER_ID, # Menggunakan SEMESTER_ID dari dapodik_utils
            backoff=3
        )

        for kota in response_kota:
            nama_kota = kota["nama"]

            # Mencocokkan dengan keyword "BEKASI"
            if not any(keyword.lower() in nama_kota.lower() for keyword in target_kota_keywords):
                print(f"  >>> Skip {nama_kota}")
                continue
            
            # Filter untuk memastikan hanya KOTA BEKASI (menghindari KABUPATEN BEKASI)
            if "KOTA BEKASI" not in nama_kota.upper():
                print(f"  >>> Skip {nama_kota} (Bukan Kota Bekasi)")
                continue

            print(f"\n  Kota/Kab MATCH: {nama_kota} ✨")
            level_wilayah_kota = kota['id_level_wilayah']
            kode_wilayah_kota = kota['kode_wilayah'].strip()

            # ambil kecamatan di kota tsb
            # PANGGIL FUNGSI DARI dapodik_utils.py
            response_kecamatan = request_api(
                level_wilayah=level_wilayah_kota,
                kode_wilayah=kode_wilayah_kota,
                semester_id=SEMESTER_ID,
                backoff=3
            )

            for kecamatan in response_kecamatan:
                print(f"    Kecamatan: {kecamatan['nama']}")
                level_wilayah_kec = kecamatan['id_level_wilayah']
                kode_wilayah_kec = kecamatan['kode_wilayah'].strip()

                # ambil daftar sekolah di kecamatan
                print("    [DELAY] Jeda 8 detik sebelum memproses sekolah...")
                time.sleep(8)

                # PANGGIL FUNGSI DARI dapodik_utils.py
                response_sekolah = request_api(
                    level_wilayah=level_wilayah_kec,
                    kode_wilayah=kode_wilayah_kec,
                    semester_id=SEMESTER_ID,
                    backoff=5
                )

                if not response_sekolah:
                    print("    ❌ GAGAL mengambil daftar sekolah. Melanjutkan.")
                    time.sleep(5)
                    continue

                jenjang = ['SD', 'SMP', 'SMA', 'SMK']
                jenis = ['Negeri', 'Swasta']

                for sekolah in response_sekolah:
                    if (sekolah['bentuk_pendidikan'] in jenjang and
                            sekolah['status_sekolah'] in jenis):

                        sekolah_id_enkrip = sekolah['sekolah_id_enkrip'].strip()

                        if sekolah_id_enkrip in processed_ids:
                            print(f"      Sekolah (SKIP, sudah di CSV): {sekolah['nama']}")
                            continue

                        print(f"      Sekolah: {sekolah['nama']}")
                        try:
                            school_url = f"https://dapo.dikdasmen.go.id/sekolah/{sekolah_id_enkrip}"
                            
                            # PANGGIL FUNGSI DARI dapodik_utils.py
                            school_data = parse_html(school_url)

                            # Jeda setelah ambil detail (HTML + API Rekap)
                            time.sleep(3)

                            # PANGGIL FUNGSI DARI dapodik_utils.py
                            write_successful = append_to_csv(
                                csv_filename,
                                sekolah_id_enkrip,
                                school_data,
                                sekolah['nama'],
                                province['nama'],
                                kota['nama'],
                                kecamatan['nama']
                            )
                            
                            # Logika sukses/gagal di sini tetap sama
                            if write_successful:
                                print(f"      ✅ SUCCESS: {sekolah['nama']} berhasil disimpan.")
                                processed_ids.add(sekolah_id_enkrip)
                            else:
                                print(f"      ⚠️ GAGAL DISIMPAN ke CSV (Cek error di atas). Melanjutkan.")
                                
                        except Exception as e:
                            print(f"      ❌ Error processing school {sekolah['nama']}: {e}")
                            time.sleep(5) # Jeda panjang jika terjadi error
                            continue
    
    # ==================================================
    # PESAN BERHASIL 
    # ==================================================
    print("\n" + "="*50)
    print("SEMUA DATA BERHASIL DI SCRAPE! ✨✅")
    print("Proses selesai dan file CSV Anda sudah final.")
    print("="*50)


if __name__ == '__main__':
    main()