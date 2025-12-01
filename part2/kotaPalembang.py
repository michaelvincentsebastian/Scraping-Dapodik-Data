import os
import time
import concurrent.futures 

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
TARGET_CITY_NAME = "KOTA PALEMBANG" # <-- TARGET KOTA
TARGET_PROVINCE_NAME = "SUMATERA SELATAN" # <-- TARGET PROVINSI
CSV_FILE = os.path.join('result', 'data_Palembang.csv') # <-- NAMA FILE

# Konstanta baru untuk kontrol kecepatan dan retry
MAX_WORKERS = 5            # Maksimal 5 thread berjalan bersamaan
RETRY_DELAY = 10           # Jeda awal detik sebelum mencoba ulang request yang gagal
SCHOOL_DETAIL_DELAY = 3    # Digunakan untuk mengontrol kecepatan peluncuran thread di main()


# --- FUNGSI DENGAN MEKANISME RETRY TANPA BATAS (LOOP WHILE TRUE) ---
def process_school(sekolah, processed_ids, province_name, city_name, kecamatan_name):
    """
    Memproses detail satu sekolah dan menyimpannya ke CSV.
    Menerapkan mekanisme retry (coba ulang) tanpa batas hingga berhasil.
    """
    
    sekolah_id_enkrip = sekolah['sekolah_id_enkrip'].strip()
    
    # 1. Pengecekan ID yang sudah diproses (Mekanisme Resume)
    if sekolah_id_enkrip in processed_ids:
        print(f"      Sekolah (SKIP, sudah di CSV): {sekolah['nama']}")
        return True 
    
    school_url = f"https://dapo.dikdasmen.go.id/sekolah/{sekolah_id_enkrip}"
    
    # 2. Loop Retry Tanpa Batas (while True)
    retry_count = 0
    
    while True:
        retry_count += 1
        print(f"      Sekolah: {sekolah['nama']} (Percobaan ke-{retry_count})")
        
        try:
            school_data = parse_html(school_url) # <-- Request detail sekolah
            
            # Jika berhasil mendapatkan data, keluar dari loop retry dan simpan
            write_successful = append_to_csv(
                CSV_FILE, sekolah_id_enkrip, school_data, sekolah['nama'],
                province_name, city_name, kecamatan_name
            )
            
            if write_successful:
                print(f"      ✅ SUCCESS: {sekolah['nama']} berhasil disimpan.")
                return sekolah_id_enkrip # KELUAR DARI LOOP jika sukses
            else:
                # Gagal menyimpan ke CSV (masalah internal), coba lagi setelah jeda
                print(f"      ⚠️ GAGAL DISIMPAN ke CSV. Mencoba lagi.")
                time.sleep(5) 
                # Lanjut ke awal loop (percobaan berikutnya)
                
        except Exception as e:
            # Jika terjadi error (Timeout, 404, 429)
            print(f"      ❌ Error processing school {sekolah['nama']}: {e}")
            
            # --- MEKANISME EXPONENTIAL BACKOFF (Jeda bertambah) ---
            delay_time = RETRY_DELAY * (2 ** (retry_count - 1))
            if delay_time > 120: delay_time = 120 # Batasi jeda maksimal 120 detik (2 menit)

            print(f"      ⏳ Menunggu {delay_time:.1f} detik sebelum mencoba ulang...")
            time.sleep(delay_time)
            # Lanjut ke awal loop (percobaan berikutnya)
                
    # Tidak akan mencapai di sini


def main():
    print(f"=========================================")
    print(f"|  START SCRAPING: {TARGET_CITY_NAME}  |")
    print(f"=========================================")
    
    # Pastikan direktori result ada
    os.makedirs('result', exist_ok=True) 
    
    create_csv_header(CSV_FILE)
    processed_ids = load_processed_ids(CSV_FILE)
    print(f"File target: {CSV_FILE}. Skip {len(processed_ids)} ID yang sudah ada.")

    # ---------------------------------------------------------------------
    # PERULANGAN 1: PROVINSI
    # ---------------------------------------------------------------------
    print("=== AMBIL DAFTAR PROVINSI ===")
    provinsi_list = request_api(backoff=3) 

    for province in provinsi_list:
        # --- Normalisasi Nama Provinsi ---
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
            # PERULANGAN 3: KECAMATAN (Memproses sekolah di dalam executor)
            # ---------------------------------------------------------------------
            for kecamatan in response_kecamatan:
                print(f"    Kecamatan: {kecamatan['nama']}")
                level_wilayah_kec = kecamatan['id_level_wilayah']
                kode_wilayah_kec = kecamatan['kode_wilayah'].strip()

                # Mengambil daftar sekolah (request API ini tetap harus sinkron/sequential)
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

                
                # --- APLIKASI MULTITHREADING DI SINI (Loop 4) ---
                sekolah_to_process = []
                jenjang = ['SD', 'SMP', 'SMA', 'SMK']; jenis = ['Negeri', 'Swasta']

                for sekolah in response_sekolah:
                    if (sekolah['bentuk_pendidikan'] in jenjang and sekolah['status_sekolah'] in jenis):
                        sekolah_to_process.append(sekolah)

                if not sekolah_to_process:
                    print("    Tidak ada sekolah yang memenuhi kriteria di kecamatan ini.")
                    continue

                print(f"    Memulai {len(sekolah_to_process)} sekolah menggunakan ThreadPoolExecutor (max {MAX_WORKERS} threads)...")

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = []
                    for sekolah in sekolah_to_process:
                        future = executor.submit(process_school, sekolah, processed_ids, province['nama'], kota['nama'], kecamatan['nama'])
                        futures.append(future)
                        
                        # 💡 PENYESUAIAN PENTING: Menambahkan delay di sini untuk mengontrol kecepatan peluncuran thread
                        time.sleep(SCHOOL_DETAIL_DELAY / MAX_WORKERS)
                        

                    # Kumpulkan hasil dan perbarui processed_ids
                    for future in concurrent.futures.as_completed(futures):
                        result_id = future.result()
                        if result_id and result_id is not True: 
                            processed_ids.add(result_id)
                # --- AKHIR MULTITHREADING ---


    print("\n" + "="*50)
    print(f"SCRAPE {TARGET_CITY_NAME} SELESAI! ✅")
    print("="*50)


if __name__ == '__main__':
    main()