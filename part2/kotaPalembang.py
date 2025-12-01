import os
import time

# ==========================================================
# IMPORT FUNGSI DARI MODUL KHUSUS (dapodik_utils.py)
# Pastikan file dapodik_utils.py ada di direktori yang sama
# ==========================================================
from dapodik_utils import (
    SEMESTER_ID, 
    request_api, 
    create_csv_header, 
    load_processed_ids, 
    parse_html, 
    append_to_csv
)

# =========================
# PENGATURAN TARGET
# =========================
TARGET_CITY_NAME = "KOTA PALEMBANG"
TARGET_PROVINCE_NAME = "SUMATERA SELATAN"
CSV_FILE = os.path.join('result', 'data_Palembang.csv')


def main():
    print(f"=========================================")
    print(f"|  START SCRAPING: {TARGET_CITY_NAME}  |")
    print(f"=========================================")
    
    create_csv_header(CSV_FILE)
    processed_ids = load_processed_ids(CSV_FILE)
    print(f"File target: {CSV_FILE}. Skip {len(processed_ids)} ID yang sudah ada.")

    # ---------------------------------------------------------------------
    # PERULANGAN 1: PROVINSI (PERBAIKAN NORMALISASI DITERAPKAN DI SINI)
    # ---------------------------------------------------------------------
    print("=== AMBIL DAFTAR PROVINSI ===")
    provinsi_list = request_api(backoff=3) 

    for province in provinsi_list:
        # --- PERBAIKAN: Normalisasi Nama Provinsi dari API ---
        nama_prov_api = province['nama'].upper().strip()
        
        # Hapus "PROV. " atau "PROV." jika ada di awal string API
        if nama_prov_api.startswith("PROV. "):
            nama_prov_upper = nama_prov_api[6:].strip()
        elif nama_prov_api.startswith("PROV."):
            nama_prov_upper = nama_prov_api[5:].strip()
        else:
            nama_prov_upper = nama_prov_api
        
        # Pencocokan
        if nama_prov_upper != TARGET_PROVINCE_NAME:
            continue

        print(f"\nPROVINSI MATCH: {province['nama']} (TARGET PROV) ✨")
        level_wilayah_prov = province['id_level_wilayah']
        kode_wilayah_prov = province['kode_wilayah'].strip()

        # Ambil semua kota/kab di provinsi ini
        response_kota = request_api(
            level_wilayah=level_wilayah_prov,
            kode_wilayah=kode_wilayah_prov,
            semester_id=SEMESTER_ID,
            backoff=3
        )

        # ---------------------------------------------------------------------
        # PERULANGAN 2: KOTA/KABUPATEN
        # ---------------------------------------------------------------------
        for kota in response_kota:
            nama_kota = kota["nama"]
            nama_kota_upper = nama_kota.upper().strip()

            if nama_kota_upper != TARGET_CITY_NAME:
                print(f"  >>> Skip {nama_kota}")
                continue
            
            # --- MULAI PROSES KOTA TARGET ---
            print(f"\n  Kota/Kab MATCH: {nama_kota} (TARGET KOTA) 🎯")
            level_wilayah_kota = kota['id_level_wilayah']
            kode_wilayah_kota = kota['kode_wilayah'].strip()

            # Ambil kecamatan di kota tsb
            response_kecamatan = request_api(
                level_wilayah=level_wilayah_kota,
                kode_wilayah=kode_wilayah_kota,
                semester_id=SEMESTER_ID,
                backoff=3
            )

            # ---------------------------------------------------------------------
            # PERULANGAN 3 & 4: KECAMATAN & SEKOLAH
            # ---------------------------------------------------------------------
            for kecamatan in response_kecamatan:
                print(f"    Kecamatan: {kecamatan['nama']}")
                level_wilayah_kec = kecamatan['id_level_wilayah']
                kode_wilayah_kec = kecamatan['kode_wilayah'].strip()

                print("    [DELAY] Jeda 8 detik sebelum memproses sekolah...")
                time.sleep(8)

                response_sekolah = request_api(
                    level_wilayah=level_wilayah_kec,
                    kode_wilayah=kode_wilayah_kec,
                    semester_id=SEMESTER_ID,
                    backoff=5
                )

                if not response_sekolah:
                    print("    ❌ GAGAL mengambil daftar sekolah. Melanjutkan.")
                    time.sleep(5)
                    continue

                jenjang = ['SD', 'SMP', 'SMA', 'SMK']; jenis = ['Negeri', 'Swasta']
                for sekolah in response_sekolah:
                    if (sekolah['bentuk_pendidikan'] in jenjang and sekolah['status_sekolah'] in jenis):

                        sekolah_id_enkrip = sekolah['sekolah_id_enkrip'].strip()
                        if sekolah_id_enkrip in processed_ids:
                            print(f"      Sekolah (SKIP, sudah di CSV): {sekolah['nama']}")
                            continue

                        print(f"      Sekolah: {sekolah['nama']}")
                        try:
                            school_url = f"https://dapo.dikdasmen.go.id/sekolah/{sekolah_id_enkrip}" 
                            school_data = parse_html(school_url) 
                            time.sleep(3)

                            write_successful = append_to_csv(
                                CSV_FILE, sekolah_id_enkrip, school_data, sekolah['nama'],
                                province['nama'], kota['nama'], kecamatan['nama']
                            )
                            
                            if write_successful:
                                print(f"      ✅ SUCCESS: {sekolah['nama']} berhasil disimpan.")
                                processed_ids.add(sekolah_id_enkrip)
                            else:
                                print(f"      ⚠️ GAGAL DISIMPAN ke CSV (Cek error di atas). Melanjutkan.")
                                
                        except Exception as e:
                            print(f"      ❌ Error processing school {sekolah['nama']}: {e}")
                            time.sleep(5) 
                            continue

    print("\n" + "="*50)
    print(f"SCRAPE {TARGET_CITY_NAME} SELESAI! ✅")
    print("="*50)


if __name__ == '__main__':
    main()