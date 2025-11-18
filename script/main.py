import requests
from bs4 import BeautifulSoup
import re
import json
import csv
import time
import urllib3
import os
import sys
from concurrent.futures import ThreadPoolExecutor # <--- Modul baru untuk kecepatan

# Menonaktifkan peringatan SSL (penting untuk Dapodik)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- KONSTANTA GLOBAL ---
BASE_URL = 'https://dapo.kemendikdasmen.go.id'
# SEMESTER_ID: 20251 = Semester Genap TA 2024/2025
SEMESTER_ID = '20251' 

PROVINSI_TARGET = 'Prov. Jawa Barat' 
KOTA_TARGETS = ['Kota Bogor', 'Kab. Bogor'] 

# Konfigurasi Threading untuk Kecepatan
MAX_WORKERS = 10 # Batasan jumlah permintaan paralel
# Catatan: time.sleep(1) telah dihapus dari loop pemrosesan.

# --- HELPER FUNCTIONS (API & HTML REQUESTS) ---
# (Semua helper functions di atas (request_api, get_wilayah_data, dll.) tetap sama)

def request_api(url: str) -> list:
    # ... (fungsi ini tetap sama seperti sebelumnya) ...
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        req = requests.get(url, timeout=15, headers=headers)
        if req.status_code == 200:
            if not req.text:
                return []
            return req.json()
        else:
            print(f"    [API ERROR] Status {req.status_code} untuk URL: {url}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"    [NETWORK ERROR] Gagal request API ({e}). URL: {url}")
        return []

def get_wilayah_data(id_level_wilayah: str, kode_wilayah: str, semester_id: str) -> list:
    # ... (fungsi ini tetap sama seperti sebelumnya) ...
    url = f'{BASE_URL}/rekap/dataSekolah?id_level_wilayah={id_level_wilayah}&kode_wilayah={kode_wilayah}&semester_id={semester_id}'
        
    return request_api(url)

def get_recapitulation_data(sekolah_id: str, semester_id: str) -> dict:
    # ... (fungsi ini tetap sama seperti sebelumnya) ...
    url = f'{BASE_URL}/rekap/sekolahDetail?semester_id={semester_id}&sekolah_id={sekolah_id}'
    recap_list = request_api(url)
    
    if recap_list and isinstance(recap_list, list) and len(recap_list) > 0:
        return recap_list[0]
    return {}

def request_html(url: str, max_retries: int = 3) -> str:
    """Mengambil konten HTML dengan mekanisme retry sederhana."""
    backoff = 2.0
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    for attempt in range(max_retries):
        try:
            # Menggunakan verify=False karena Dapodik sering memiliki isu SSL
            res = requests.get(url, verify=False, timeout=15, headers=headers) 
            if res.status_code == 200:
                if "User validation required" in res.text or "Checking your browser" in res.text:
                    print(f"        [WARNING] Validasi Pengguna Ditemukan. Retrying ({attempt+1}/{max_retries})...")
                    time.sleep(backoff)
                    backoff *= 2 
                    continue
                return res.text
            elif res.status_code == 404:
                return ""
            else:
                 print(f"        [HTML ERROR] Status {res.status_code} untuk URL: {url}")
                 return ""
        except requests.exceptions.RequestException as e:
            print(f"        [NETWORK ERROR] {e}, retrying ({attempt+1}/{max_retries}) in {backoff}s...")
            time.sleep(backoff)
            backoff *= 2
    return ""

def lat_lon_parse(url: str) -> tuple[str, str]:
    # ... (fungsi ini tetap sama seperti sebelumnya) ...
    req = request_html(url)
    if not req:
        return None, None
        
    soup = BeautifulSoup(req, 'html.parser')
    latitude = None
    longitude = None
    scripts = soup.find_all('script')

    for script in scripts:
        if script.string and "L.map('maps').setView" in script.string: 
            lat_search = re.search(r"lat:\s*(-?\d+\.\d+)", script.string)
            lon_search = re.search(r"lon:\s*(-?\d+\.\d+)", script.string)

            if lat_search:
                latitude = lat_search.group(1)
            if lon_search:
                longitude = lon_search.group(1)
            break
    return latitude, longitude

def parse_html(url: str) -> dict:
    # ... (fungsi ini tetap sama seperti sebelumnya) ...
    req = request_html(url)
    school_data = {"profile": {}, "recapitulation": {}, "contact": {}}
    
    if not req:
        return school_data
        
    soup = BeautifulSoup(req, 'html.parser')
    
    # --- 1. Ambil Data Profil & Sidebar ---
    sidebar_data = {} 
    
    profile_panels = soup.select('#profil .panel-info')
    for panel in profile_panels:
        heading = panel.find(class_='panel-heading').get_text(strip=True)
        body = panel.find(class_='panel-body')
        section_data = {}
        if body: 
            for p in body.find_all('p'):
                if p.find('strong'):
                    key = p.find('strong').get_text(strip=True).replace(':', '').strip()
                    value = p.strong.next_sibling.strip() if p.strong.next_sibling else '' 
                    section_data[key] = value
            
            if "Identitas" in heading:
                school_data["profile"]["identitas_sekolah"] = section_data
            elif "Pelengkap" in heading:
                school_data["profile"]["data_pelengkap"] = section_data
            elif "Rinci" in heading:
                school_data["profile"]["data_rinci"] = section_data

    sidebar_menu = soup.find(class_='profile-usermenu')
    if sidebar_menu:
        for item in sidebar_menu.find_all('li'):
            text = item.get_text(strip=True)
            if ':' in text:
                key, value = text.split(':', 1)
                sidebar_data[key.strip()] = value.strip()
    school_data["profile"]["sidebar_info"] = sidebar_data 

    # --- 2. Ambil Data Kontak ---
    contact_panel = soup.select_one('#kontak .panel-info')
    if contact_panel:
        contact_info = {}
        for p in contact_panel.find_all('p'):
            if p.find('strong'):
                key = p.find('strong').get_text(strip=True).replace(':', '').strip()
                value = p.strong.next_sibling.strip() if p.strong.next_sibling else ''
                contact_info[key] = value
        school_data["contact"] = contact_info
        
    # --- 3. Ambil Koordinat (Lintang/Bujur) ---
    npsn = school_data["profile"].get("identitas_sekolah", {}).get("NPSN", "")
    if npsn:
        urls_latlon = f'https://referensi.data.kemdikbud.go.id/pendidikan/npsn/{npsn}'
        latitude, longitude = lat_lon_parse(urls_latlon)

        if latitude or longitude:
            school_data["contact"]["Lintang"] = latitude
            school_data["contact"]["Bujur"] = longitude

    # --- 4. Ambil Data Rekapitulasi (API Call) ---
    sekolah_id = url.split('/')[-1].strip()
    school_data["recapitulation"] = get_recapitulation_data(sekolah_id, SEMESTER_ID)

    return school_data

# --- CSV HANDLER FUNCTIONS ---

def create_csv_header(filename):
    # ... (fungsi ini tetap sama seperti sebelumnya) ...
    headers = [
        'Nama_Sekolah', 'Provinsi', 'Kota', 'Kecamatan',
        'NPSN', 'Status', 'Bentuk_Pendidikan', 'Status_Kepemilikan',
        'SK_Pendirian_Sekolah', 'Tanggal_SK_Pendirian', 'SK_Izin_Operasional',
        'Tanggal_SK_Izin_Operasional', 'Kebutuhan_Khusus_Dilayani', 'Nama_Bank',
        'Cabang_KCP_Unit', 'Rekening_Atas_Nama', 'Status_BOS',
        'Waku_Penyelenggaraan', 'Sertifikasi_ISO', 'Sumber_Listrik',
        'Daya_Listrik', 'Kecepatan_Internet', 'Kepsek', 'Operator',
        'Akreditasi', 'Kurikulum', 'Waktu', 'Alamat', 'RT_RW',
        'Dusun', 'Desa_Kelurahan', 'Kecamatan_Detail', 'Kabupaten',
        'Provinsi_Detail', 'Kode_Pos', 'Lintang', 'Bujur', 'Guru_L', 'Guru_P', 'Guru_Total',
        'Tendik_L', 'Tendik_P', 'Tendik_Total', 'PTK_L', 'PTK_P', 'PTK_Total', 'PD_L', 'PD_P', 'PD_Total',
        'Before_Ruang_Kelas', 'After_Ruang_Kelas', 'Before_Ruang_Perpus', 'After_Ruang_Perpus',
        'Before_Ruang_Lab', 'After_Ruang_Lab', 'Before_Ruang_Pratik', 'After_Ruang_Pratik',
        'Before_Ruang_Pimpinan', 'After_Ruang_Pimpinan', 'Before_Ruang_Guru', 'After_Ruang_Guru',
        'Before_Ruang_Ibadah', 'After_Ruang_Ibadah', 'Before_Ruang_UKS', 'After_Ruang_UKS',
        'Before_Toilet', 'After_Toilet', 'Before_Gudang', 'After_Gudang',
        'Before_Ruang_Sirkulasi', 'After_Ruang_Sirkulasi', 'Before_Tempat_Bermain_Olahraga', 'After_Tempat_Bermain_Olahraga',
        'Before_Ruang_TU', 'After_Ruang_TU', 'Before_Ruang_Konseling', 'After_Ruang_Konseling',
        'Before_Ruang_OSIS', 'After_Ruang_OSIS', 'Before_Bangunan', 'After_Bangunan', 'Rombel'
    ]

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
    except IOError as e:
        print(f"FATAL: Gagal membuat file CSV {filename}. Pastikan tidak terbuka. Error: {e}")
        sys.exit(1)


def append_to_csv(filename, school_data, school_name, province, kota, kecamatan):
    # ... (fungsi ini tetap sama seperti sebelumnya) ...
    profile = school_data.get('profile', {})
    contact = school_data.get('contact', {})
    recapitulation = school_data.get('recapitulation', {}) 

    # Extract data with default empty values
    identitas = profile.get('identitas_sekolah', {})
    pelengkap = profile.get('data_pelengkap', {})
    rinci = profile.get('data_rinci', {})
    sidebar = profile.get('sidebar_info', {})

    # Hitungan Total PTK dan PD
    Guru_L = recapitulation.get('ptk_laki', 0)
    Guru_P = recapitulation.get('ptk_perempuan', 0)
    Guru_Total = Guru_L + Guru_P
    
    Tendik_L = recapitulation.get('pegawai_laki', 0)
    Tendik_P = recapitulation.get('pegawai_perempuan', 0)
    Tendik_Total = Tendik_L + Tendik_P
    
    PTK_L = recapitulation.get('ptk_laki', 0) + recapitulation.get('pegawai_laki', 0)
    PTK_P = recapitulation.get('ptk_perempuan', 0) + recapitulation.get('pegawai_perempuan', 0)
    PTK_Total = PTK_L + PTK_P
    
    PD_L = recapitulation.get('pd_laki', 0)
    PD_P = recapitulation.get('pd_perempuan', 0)
    PD_Total = PD_L + PD_P

    # Baris Data
    row = [
        school_name,province, kota, kecamatan,
        identitas.get('NPSN', ''),
        identitas.get('Status', ''),
        identitas.get('Bentuk Pendidikan', ''),
        identitas.get('Status Kepemilikan', ''),
        identitas.get('SK Pendirian Sekolah', ''),
        identitas.get('Tanggal SK Pendirian', ''),
        identitas.get('SK Izin Operasional', ''),
        identitas.get('Tanggal SK Izin Operasional', ''),
        pelengkap.get('Kebutuhan Khusus Dilayani', ''),
        pelengkap.get('Nama Bank', ''),
        pelengkap.get('Cabang KCP/Unit', ''),
        pelengkap.get('Rekening Atas Nama', ''),
        rinci.get('Status BOS', ''),
        rinci.get('Waku Penyelenggaraan', ''),
        rinci.get('Sertifikasi ISO', ''),
        rinci.get('Sumber Listrik', ''),
        rinci.get('Daya Listrik', ''),
        rinci.get('Kecepatan Internet', ''),
        sidebar.get('Kepsek', ''),
        sidebar.get('Operator', ''),
        sidebar.get('Akreditasi', ''),
        sidebar.get('Kurikulum', ''),
        sidebar.get('Waktu', ''),
        contact.get('Alamat', ''),
        contact.get('RT / RW', ''),
        contact.get('Dusun', ''),
        contact.get('Desa / Kelurahan', ''),
        contact.get('Kecamatan', ''),
        contact.get('Kabupaten', ''),
        contact.get('Provinsi', ''),
        contact.get('Kode Pos', ''),
        contact.get('Lintang', ''),
        contact.get('Bujur', ''),
        Guru_L, Guru_P, Guru_Total,
        Tendik_L, Tendik_P, Tendik_Total,
        PTK_L, PTK_P, PTK_Total,
        PD_L, PD_P, PD_Total,
        recapitulation.get('before_ruang_kelas', 0),
        recapitulation.get('after_ruang_kelas', 0),
        recapitulation.get('before_ruang_perpus', 0),
        recapitulation.get('after_ruang_perpus', 0),
        recapitulation.get('before_ruang_lab', 0),
        recapitulation.get('after_ruang_lab', 0),
        recapitulation.get('before_ruang_praktik', 0),
        recapitulation.get('after_ruang_praktik', 0),
        recapitulation.get('before_ruang_pimpinan', 0),
        recapitulation.get('after_ruang_pimpinan', 0),
        recapitulation.get('before_ruang_guru', 0),
        recapitulation.get('after_ruang_guru', 0),
        recapitulation.get('before_ruang_ibadah', 0),
        recapitulation.get('after_ruang_ibadah', 0),
        recapitulation.get('before_ruang_uks', 0),
        recapitulation.get('after_ruang_uks', 0),
        recapitulation.get('before_toilet', 0),
        recapitulation.get('after_toilet', 0),
        recapitulation.get('before_gudang', 0),
        recapitulation.get('after_gudang', 0),
        recapitulation.get('before_ruang_sirkulasi', 0),
        recapitulation.get('after_ruang_sirkulasi', 0),
        recapitulation.get('before_tempat_bermain_olahraga', 0),
        recapitulation.get('after_tempat_bermain_olahraga', 0),
        recapitulation.get('before_ruang_tu', 0),
        recapitulation.get('after_ruang_tu', 0),
        recapitulation.get('before_ruang_konseling', 0),
        recapitulation.get('after_ruang_konseling', 0),
        recapitulation.get('before_ruang_osis', 0),
        recapitulation.get('after_ruang_osis', 0),
        recapitulation.get('before_bangunan', 0),
        recapitulation.get('after_bangunan', 0),
        recapitulation.get('rombel', 0)
    ]

    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(row)
    except IOError as e:
        print(f"ERROR: Gagal menulis ke CSV {filename}. Error: {e}")


# --- FUNGSI BARU UNTUK PROSES PARALEL ---

def process_school(school_info, csv_filename, provinsi_nama, kota_nama, kecamatan_nama):
    """
    Fungsi yang akan dijalankan oleh setiap worker thread.
    Mengambil data detail sekolah dan menambahkannya ke CSV.
    """
    sekolah, sekolah_id_enkrip = school_info
    required_keys = ['bentuk_pendidikan', 'status_sekolah', 'nama', 'sekolah_id_enkrip']

    if not all(key in sekolah for key in required_keys):
        return None # Lewati jika data API tidak lengkap
        
    sekolah_nama = sekolah['nama']
    print(f"      [STARTING] {sekolah_nama} ({sekolah['bentuk_pendidikan']} - {sekolah['status_sekolah']})") 

    try:
        # 6. HTML Scraping (Detail Sekolah)
        school_url = f"https://dapo.dikdasmen.go.id/sekolah/{sekolah_id_enkrip}"
        
        school_data = parse_html(school_url)

        append_to_csv(
            csv_filename, 
            school_data, 
            sekolah_nama, 
            provinsi_nama, 
            kota_nama, 
            kecamatan_nama
        )
        print(f"        [SUCCESS] Data {sekolah_nama} ditambahkan ke CSV.")
        return 1
        
    except Exception as e:
        print(f"        [ERROR] Gagal memproses detail sekolah {sekolah_nama}: {e}")
        return 0


# --- MAIN EXECUTION (DIUBAH) ---

def main():
    start_time = time.time()
    
    print("--- ⚙️ Memulai Eksekusi Script Dapodik Scraper dengan Threading ---")
    
    # ... (Langkah 1 & 2: Mencari Provinsi dan Setup File/Direktori tetap sama) ...
    
    # 1. Mengambil dan Memilih Provinsi Jawa Barat
    print("Mencari data provinsi...")
    response_provinsi = get_wilayah_data('0', '000000', SEMESTER_ID)
    
    if not response_provinsi:
        print("FATAL: Gagal mengambil daftar Provinsi dari API. Program dihentikan.")
        sys.exit(1)

    province_target = next((prov for prov in response_provinsi if prov['nama'] == PROVINSI_TARGET), None)
            
    if not province_target:
        print(f"FATAL: Provinsi target '{PROVINSI_TARGET}' tidak ditemukan. Program dihentikan.")
        sys.exit(1)
        
    provinsi_nama = province_target['nama']
    provinsi_level = province_target['id_level_wilayah']
    provinsi_kode = province_target['kode_wilayah'].strip()

    # 2. Setup File dan Direktori
    output_dir = './result'
    os.makedirs(output_dir, exist_ok=True) 
    csv_filename = f'{output_dir}/Data_Sekolah_{provinsi_nama.replace(" ", "_").replace(".", "")}_Bogor_Threading.csv' # Nama file diubah
    
    create_csv_header(csv_filename)
    print(f"\n✅ CSV header berhasil dibuat di: {csv_filename}")
    print(f"--- Memulai Scraping Provinsi: {provinsi_nama} ---")

    # Kumpulkan semua tugas (sekolah) dalam satu list
    all_schools_to_process = []
    
    # 3. Level 2: Kota/Kabupaten (Filter Bogor)
    response_kota = get_wilayah_data(provinsi_level, provinsi_kode, SEMESTER_ID)
    
    if not response_kota:
         print(f"ERROR: Gagal mengambil data Kota/Kabupaten untuk {provinsi_nama}.")
         return

    for kota in response_kota:
        kota_nama = kota['nama']
        
        if kota_nama not in KOTA_TARGETS:
             continue 
        
        print(f"  [DIPROSES] Kota: {kota_nama}")
        kota_kode = kota['kode_wilayah'].strip()

        # 4. Level 3: Kecamatan
        response_kecamatan = get_wilayah_data('2', kota_kode, SEMESTER_ID)
        
        if not response_kecamatan:
            print(f"    ERROR: Gagal mengambil data Kecamatan untuk {kota_nama}")
            continue

        for kecamatan in response_kecamatan:
            kecamatan_nama = kecamatan['nama']
            print(f"    Kecamatan: {kecamatan_nama}")
            kecamatan_kode = kecamatan['kode_wilayah'].strip() 
            
            # 5. Level 4: Sekolah (API Call)
            response_sekolah = get_wilayah_data('4', kecamatan_kode, SEMESTER_ID)
            
            if not response_sekolah:
                 continue
                 
            # Kumpulkan data sekolah yang akan diproses
            for sekolah in response_sekolah:
                if all(key in sekolah for key in ['bentuk_pendidikan', 'status_sekolah', 'nama', 'sekolah_id_enkrip']):
                    sekolah_id_enkrip = sekolah['sekolah_id_enkrip'].strip()
                    # Simpan tuple (data sekolah, ID enkripsi, nama provinsi, nama kota, nama kecamatan)
                    task_info = (sekolah, sekolah_id_enkrip, provinsi_nama, kota_nama, kecamatan_nama)
                    all_schools_to_process.append(task_info)


    # 6. Eksekusi Paralel Menggunakan ThreadPoolExecutor
    total_tasks = len(all_schools_to_process)
    processed_count = 0
    success_count = 0
    
    print(f"\n--- ⚡️ Memulai Eksekusi Paralel untuk {total_tasks} Sekolah (Max Threads: {MAX_WORKERS}) ---")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Siapkan argumen untuk setiap pemanggilan process_school
        future_to_school = {
            executor.submit(
                process_school, 
                task[0:2], # school_info = (sekolah, sekolah_id_enkrip)
                csv_filename, 
                task[2], task[3], task[4]
            ): task[0]['nama'] for task in all_schools_to_process
        }
        
        for future in future_to_school:
            processed_count += 1
            school_name = future_to_school[future]
            try:
                result = future.result()
                if result == 1:
                    success_count += 1
            except Exception as exc:
                print(f'    [CRITICAL ERROR] Thread untuk {school_name} menghasilkan pengecualian: {exc}')

            print(f"Progress: {processed_count}/{total_tasks} ({success_count} Berhasil) - {school_name} Selesai.")
    
    end_time = time.time()
    elapsed_time = end_time - start_time

    print("\n--- ✅ Proses Scraping Selesai ---")
    print(f"Total Sekolah Ditemukan/Ditugaskan: {total_tasks}")
    print(f"Total Sekolah Berhasil Diproses: {success_count}")
    print(f"Waktu Eksekusi Total: {elapsed_time:.2f} detik")


if __name__ == '__main__':
    main()