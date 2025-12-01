import os
import time
import concurrent.futures # <-- MODUL BARU

# ==========================================================
# IMPORT FUNGSI DARI MODUL KHUSUS (dapodik_utils.py)
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
TARGET_CITY_NAME = "KOTA BALIKPAPAN"
TARGET_PROVINCE_NAME = "KALIMANTAN TIMUR"
CSV_FILE = os.path.join('result', 'data_Balikpapan.csv')

# Konstanta untuk batas thread/request
MAX_WORKERS = 5 # Maksimal 5 thread berjalan bersamaan
SCHOOL_DETAIL_DELAY = 3 # Jeda per sekolah tetap dipertahankan

# --- FUNGSI BARU UNTUK MEMPROSES SEKOLAH SECARA INDIVIDUAL ---
def process_school(sekolah, processed_ids, province_name, city_name, kecamatan_name):
    """Memproses detail satu sekolah dan menyimpannya ke CSV."""
    
    sekolah_id_enkrip = sekolah['sekolah_id_enkrip'].strip()
    
    if sekolah_id_enkrip in processed_ids:
        print(f"      Sekolah (SKIP, sudah di CSV): {sekolah['nama']}")
        return True # Mengembalikan True agar ID tetap ada
    
    print(f"      Sekolah: {sekolah['nama']}")
    try:
        school_url = f"https://dapo.dikdasmen.go.id/sekolah/{sekolah_id_enkrip}" 
        
        # NOTE: Tidak ada time.sleep() di sini, karena delay dilakukan 
        # di luar fungsi ini atau ditangani oleh delay ThreadPool
        school_data = parse_html(school_url) 
        time.sleep(SCHOOL_DETAIL_DELAY) # <-- JEDA PENTING UNTUK MENGHINDARI RATE LIMITING

        write_successful = append_to_csv(
            CSV_FILE, sekolah_id_enkrip, school_data, sekolah['nama'],
            province_name, city_name, kecamatan_name
        )
        
        if write_successful:
            print(f"      ✅ SUCCESS: {sekolah['nama']} berhasil disimpan.")
            return sekolah_id_enkrip # Mengembalikan ID jika sukses
        else:
            print(f"      ⚠️ GAGAL DISIMPAN ke CSV (Cek error di atas).")
            return None
            
    except Exception as e:
        print(f"      ❌ Error processing school {sekolah['nama']}: {e}")
        return None


def main():
    print(f"=========================================")
    print(f"|  START SCRAPING: {TARGET_CITY_NAME}  |")
    print(f"=========================================")
    
    create_csv_header(CSV_FILE)
    processed_ids = load_processed_ids(CSV_FILE)
    print(f"File target: {CSV_FILE}. Skip {len(processed_ids)} ID yang sudah ada.")

    print("=== AMBIL DAFTAR PROVINSI ===")
    provinsi_list = request_api(backoff=3) 

    for province in provinsi_list:
        # Normalisasi Nama Provinsi
        nama_prov_api = province['nama'].upper().strip()
        if nama_prov_api.startswith("PROV. "):
            nama_prov_upper = nama_prov_api[6:].strip()
        elif nama_prov_api.startswith("PROV."):
            nama_prov_upper = nama_prov_api[5:].strip()
        else:
            nama_prov_upper = nama_prov_api
        
        if nama_prov_upper != TARGET_PROVINCE_NAME:
            continue

        print(f"\nPROVINSI MATCH: {province['nama']} (TARGET PROV) ✨")
        level_wilayah_prov = province['id_level_wilayah']
        kode_wilayah_prov = province['kode_wilayah'].strip()

        response_kota = request_api(level_wilayah=level_wilayah_prov, kode_wilayah=kode_wilayah_prov, semester_id=SEMESTER_ID, backoff=3)

        # ---------------------------------------------------------------------
        # PERULANGAN 2: KOTA/KABUPATEN
        # ---------------------------------------------------------------------
        for kota in response_kota:
            nama_kota = kota["nama"]
            nama_kota_upper = nama_kota.upper().strip()

            if nama_kota_upper != TARGET_CITY_NAME:
                print(f"  >>> Skip {nama_kota}")
                continue
            
            print(f"\n  Kota/Kab MATCH: {nama_kota} (TARGET KOTA) 🎯")
            level_wilayah_kota = kota['id_level_wilayah']
            kode_wilayah_kota = kota['kode_wilayah'].strip()

            response_kecamatan = request_api(level_wilayah=level_wilayah_kota, kode_wilayah=kode_wilayah_kota, semester_id=SEMESTER_ID, backoff=3)

            # ---------------------------------------------------------------------
            # PERULANGAN 3: KECAMATAN (Memproses sekolah di dalam executor)
            # ---------------------------------------------------------------------
            for kecamatan in response_kecamatan:
                print(f"    Kecamatan: {kecamatan['nama']}")
                level_wilayah_kec = kecamatan['id_level_wilayah']
                kode_wilayah_kec = kecamatan['kode_wilayah'].strip()

                # Mengambil daftar sekolah (request API ini tetap harus sinkron/sequential)
                print("    [DELAY] Jeda 8 detik sebelum memproses sekolah...")
                time.sleep(8)
                response_sekolah = request_api(level_wilayah=level_wilayah_kec, kode_wilayah=kode_wilayah_kec, semester_id=SEMESTER_ID, backoff=5)

                if not response_sekolah:
                    print("    ❌ GAGAL mengambil daftar sekolah. Melanjutkan.")
                    time.sleep(5)
                    continue

                
                # --- APLIKASI MULTITHREADING DI SINI (Loop 4) ---
                sekolah_to_process = []
                jenjang = ['SD', 'SMP', 'SMA', 'SMK']; jenis = ['Negeri', 'Swasta']

                for sekolah in response_sekolah:
                    if (sekolah['bentuk_pendidikan'] in jenjang and sekolah['status_sekolah'] in jenis):
                        sekolah_to_process.append(sekolah)

                if not sekolah_to_process:
                    print("    Tidak ada sekolah yang memenuhi kriteria di kecamatan ini.")
                    continue

                print(f"    Memulai {len(sekolah_to_process)} sekolah menggunakan ThreadPoolExecutor (max {MAX_WORKERS} threads)...")

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = []
                    for sekolah in sekolah_to_process:
                        future = executor.submit(process_school, sekolah, processed_ids, province['nama'], kota['nama'], kecamatan['nama'])
                        futures.append(future)

                    # Kumpulkan hasil dan perbarui processed_ids
                    for future in concurrent.futures.as_completed(futures):
                        result_id = future.result()
                        if result_id:
                            processed_ids.add(result_id)
                # --- AKHIR MULTITHREADING ---


    print("\n" + "="*50)
    print(f"SCRAPE {TARGET_CITY_NAME} SELESAI! ✅")
    print("="*50)


if __name__ == '__main__':
    # Pastikan direktori result ada
    os.makedirs('result', exist_ok=True) 
    main()