import requests
from bs4 import BeautifulSoup
import re
import json
import pandas as pd
import csv
import time
import urllib3
import os
import sys

# Menonaktifkan peringatan SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- KONSTANTA GLOBAL ---
BASE_URL = 'https://dapo.kemendikdasmen.go.id'
SEMESTER_ID = '20251' # Ganti sesuai semester yang diinginkan
LEVEL_WILAYAH = '0'
KODE_WILAYAH = '000000'

# --- KONSTANTA GLOBAL UNTUK BACKUP ---
BACKUP_THRESHOLD = 50 # Jumlah sekolah per backup
BACKUP_COUNTER = 0     # Counter sekolah (akan di-reset per kota)

# Membuat sesi Request Global untuk koneksi yang lebih efisien
session = requests.Session()

def save_backup(all_school_data, kota_nama, output_dir, final_write=False):
    """Menyimpan data yang ada di memori ke file CSV backup."""
    
    if final_write:
        data_to_save = all_school_data
    else:
        # Hanya simpan data dari backup counter terakhir
        start_index = max(0, len(all_school_data) - BACKUP_THRESHOLD)
        data_to_save = all_school_data[start_index:]

    if not data_to_save:
        return
        
    kota_nama_bersih = kota_nama.replace(' ', '_').replace('/', '_')
    base_filename = f"backup_run_{kota_nama_bersih}"
    backup_dir = os.path.join(output_dir, 'backup')
    os.makedirs(backup_dir, exist_ok=True)
    backup_path = os.path.join(backup_dir, base_filename)
    
    # Mencari nomor backup yang belum terpakai (incremental naming)
    i = 0
    while os.path.exists(f"{backup_path}_{i}.csv"):
        i += 1
        
    if final_write:
        file_number = 0
        final_filename = f"{backup_path}_FINAL.csv" 
        print(f"      [FINAL BACKUP] Menyimpan total {len(data_to_save)} baris ke {os.path.basename(final_filename)}")
    else:
        file_number = i
        final_filename = f"{backup_path}_{file_number}.csv"
        print(f"      [INCREMENTAL BACKUP] Menyimpan {len(data_to_save)} baris ke {os.path.basename(final_filename)}")

    try:
        df = pd.DataFrame(data_to_save)
        df.to_csv(final_filename, index=False, encoding='utf-8')

    except Exception as e:
        print(f"❌ ERROR saat menyimpan backup ke {final_filename}: {e}")


def request_api(base_url: str = BASE_URL, level_wilayah: str = LEVEL_WILAYAH, kode_wilayah: str = KODE_WILAYAH, semester_id: str = SEMESTER_ID, sekolah_id: str = None) -> list:
    """Mengambil data wilayah atau rekapitulasi sekolah dari API Dapo."""
    global session
    try:
        if sekolah_id:
            url = f'{base_url}/rekap/sekolahDetail?semester_id={semester_id}&sekolah_id={sekolah_id}'
        elif level_wilayah == 3:
            url = f'{base_url}/rekap/progresSP?id_level_wilayah={level_wilayah}&kode_wilayah={kode_wilayah}&semester_id={semester_id}&bentuk_pendidikan_id='
        else:
            url = f'{base_url}/rekap/dataSekolah?id_level_wilayah={level_wilayah}&kode_wilayah={kode_wilayah}&semester_id={semester_id}'

        req = session.get(url)
        if req.status_code != 200:
            print(f"API Error {req.status_code} for URL: {url}")
            return []
        return req.json()
    except Exception as e:
        print(f"Error requesting API for {kode_wilayah}: {e}")
        return []


def request_html(url: str, initial_backoff: float = 2.0) -> str:
    """Mengambil konten HTML dengan mekanisme retry TANPA BATAS untuk mencegah skip data."""
    global session
    backoff = initial_backoff
    attempt = 0
    
    while True:
        attempt += 1
        try:
            if not url.startswith('http'):
                raise ValueError(f"Invalid URL: {url}")
            
            res = session.get(url, verify=False, timeout=15) 
            
            if res.status_code == 404:
                print(f"         [WARNING] 404 Not Found di URL ini. Mengembalikan string kosong.")
                return ""
            
            if "User validation required" in res.text or "Checking your browser" in res.text:
                print(f"         [WARNING] Validasi Pengguna Ditemukan (Attempt {attempt}). Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
            
            return res.text
            
        except requests.RequestException as e:
            print(f"         [NETWORK ERROR] {e} (Attempt {attempt}), retrying in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            
        except ValueError as e:
            print(f"         [FATAL ERROR] {e}. Menghentikan request ini.")
            return ""


def extract_data_to_dict(school_data, school_name, province, kota, kecamatan):
    """Mengubah data sekolah hasil scrape menjadi satu dictionary (row)."""
    profile = school_data.get('profile', {})
    contact = school_data.get('contact', {})
    recapitulation = school_data.get('recapitulation', {})

    identitas = profile.get('identitas_sekolah', {})
    pelengkap = profile.get('data_pelengkap', {})
    rinci = profile.get('data_rinci', {})
    sidebar = profile.get('sidebar_info', {})

    # --- PENGHITUNGAN TOTAL REKAPITULASI ---
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

    # --- MEMBANGUN DICTIONARY (ROW) ---
    row_dict = {
        'Nama_Sekolah': school_name,
        'Provinsi': province,
        'Kota': kota,
        'Kecamatan': kecamatan,
        'NPSN': identitas.get('NPSN', ''),
        'Status': identitas.get('Status', ''),
        'Bentuk_Pendidikan': identitas.get('Bentuk Pendidikan', ''),
        'Status_Kepemilikan': identitas.get('Status Kepemilikan', ''),
        'SK_Pendirian_Sekolah': identitas.get('SK Pendirian Sekolah', ''),
        'Tanggal_SK_Pendirian': identitas.get('Tanggal SK Pendirian', ''),
        'SK_Izin_Operasional': identitas.get('SK Izin Operasional', ''),
        'Tanggal_SK_Izin_Operasional': identitas.get('Tanggal SK Izin Operasional', ''),
        'Kebutuhan_Khusus_Dilayani': pelengkap.get('Kebutuhan Khusus Dilayani', ''),
        'Nama_Bank': pelengkap.get('Nama Bank', ''),
        'Cabang_KCP_Unit': pelengkap.get('Cabang KCP/Unit', ''),
        'Rekening_Atas_Nama': pelengkap.get('Rekening Atas Nama', ''),
        'Status_BOS': rinci.get('Status BOS', ''),
        'Waku_Penyelenggaraan': rinci.get('Waku Penyelenggaraan', ''),
        'Sertifikasi_ISO': rinci.get('Sertifikasi ISO', ''),
        'Sumber_Listrik': rinci.get('Sumber Listrik', ''),
        'Daya_Listrik': rinci.get('Daya Listrik', ''),
        'Kecepatan_Internet': rinci.get('Kecepatan Internet', ''),
        'Kepsek': sidebar.get('Kepsek', ''),
        'Operator': sidebar.get('Operator', ''),
        'Akreditasi': sidebar.get('Akreditasi', ''),
        'Kurikulum': sidebar.get('Kurikulum', ''),
        'Waktu': sidebar.get('Waktu', ''),
        'Alamat': contact.get('Alamat', ''),
        'RT_RW': contact.get('RT / RW', ''),
        'Dusun': contact.get('Dusun', ''),
        'Desa_Kelurahan': contact.get('Desa / Kelurahan', ''),
        'Kecamatan_Detail': contact.get('Kecamatan', ''),
        'Kabupaten': contact.get('Kabupaten', ''),
        'Provinsi_Detail': contact.get('Provinsi', ''),
        'Kode_Pos': contact.get('Kode Pos', ''),
        'Lintang': contact.get('Lintang', ''),
        'Bujur': contact.get('Bujur', ''),
        'Guru_L': Guru_L, 'Guru_P': Guru_P, 'Guru_Total': Guru_Total,
        'Tendik_L': Tendik_L, 'Tendik_P': Tendik_P, 'Tendik_Total': Tendik_Total,
        'PTK_L': PTK_L, 'PTK_P': PTK_P, 'PTK_Total': PTK_Total,
        'PD_L': PD_L, 'PD_P': PD_P, 'PD_Total': PD_Total,
        'Before_Ruang_Kelas': recapitulation.get('before_ruang_kelas', 0),
        'After_Ruang_Kelas': recapitulation.get('after_ruang_kelas', 0),
        'Before_Ruang_Perpus': recapitulation.get('before_ruang_perpus', 0),
        'After_Ruang_Perpus': recapitulation.get('after_ruang_perpus', 0),
        'Before_Ruang_Lab': recapitulation.get('before_ruang_lab', 0),
        'After_Ruang_Lab': recapitulation.get('after_ruang_lab', 0),
        'Before_Ruang_Pratik': recapitulation.get('before_ruang_praktik', 0),
        'After_Ruang_Pratik': recapitulation.get('after_ruang_praktik', 0),
        'Before_Ruang_Pimpinan': recapitulation.get('before_ruang_pimpinan', 0),
        'After_Ruang_Pimpinan': recapitulation.get('after_ruang_pimpinan', 0),
        'Before_Ruang_Guru': recapitulation.get('before_ruang_guru', 0),
        'After_Ruang_Guru': recapitulation.get('after_ruang_guru', 0),
        'Before_Ruang_Ibadah': recapitulation.get('before_ruang_ibadah', 0),
        'After_Ruang_Ibadah': recapitulation.get('after_ruang_ibadah', 0),
        'Before_Ruang_UKS': recapitulation.get('before_ruang_uks', 0),
        'After_Ruang_UKS': recapitulation.get('after_ruang_uks', 0),
        'Before_Toilet': recapitulation.get('before_toilet', 0),
        'After_Toilet': recapitulation.get('after_toilet', 0),
        'Before_Gudang': recapitulation.get('before_gudang', 0),
        'After_Gudang': recapitulation.get('after_gudang', 0),
        'Before_Ruang_Sirkulasi': recapitulation.get('before_ruang_sirkulasi', 0),
        'After_Ruang_Sirkulasi': recapitulation.get('after_ruang_sirkulasi', 0),
        'Before_Tempat_Bermain_Olahraga': recapitulation.get('before_tempat_bermain_olahraga', 0),
        'After_Tempat_Bermain_Olahraga': recapitulation.get('after_tempat_bermain_olahraga', 0),
        'Before_Ruang_TU': recapitulation.get('before_ruang_tu', 0),
        'After_Ruang_TU': recapitulation.get('after_ruang_tu', 0),
        'Before_Ruang_Konseling': recapitulation.get('before_ruang_konseling', 0),
        'After_Ruang_Konseling': recapitulation.get('after_ruang_konseling', 0),
        'Before_Ruang_OSIS': recapitulation.get('before_ruang_osis', 0),
        'After_Ruang_OSIS': recapitulation.get('after_ruang_osis', 0),
        'Before_Bangunan': recapitulation.get('before_bangunan', 0),
        'After_Bangunan': recapitulation.get('after_bangunan', 0),
        'Rombel': recapitulation.get('rombel', 0)
    }
    
    return row_dict

def parse_html(url: str):
    """Mengurai (parse) halaman HTML detail sekolah dan mengambil data profil/kontak."""
    req = request_html(url)
    if not req: 
        return json.dumps({"profile": {}, "recapitulation": {}, "contact": {}}, ensure_ascii=False)
    
    soup = BeautifulSoup(req, 'html.parser')
    school_data = {
        "profile": {},
        "recapitulation": {},
        "contact": {}
    }
    
    profile_panels = soup.select('#profil .panel-info')
    sidebar_data = {}
    
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

    contact_panel = soup.select_one('#kontak .panel-info')
    if contact_panel:
        contact_info = {}
        for p in contact_panel.find_all('p'):
            if p.find('strong'):
                key = p.find('strong').get_text(strip=True).replace(':', '').strip()
                value = p.strong.next_sibling.strip() if p.strong.next_sibling else ''
                contact_info[key] = value
        school_data["contact"] = contact_info

    npsn = school_data["profile"].get("identitas_sekolah", {}).get("NPSN", "")
    if npsn:
        urls_latlon = f'https://referensi.data.kemdikbud.go.id/pendidikan/npsn/{npsn}'
        latitude, longitude = lat_lon_parse(urls_latlon)

        if latitude or longitude:
            school_data["contact"]["Lintang"] = latitude
            school_data["contact"]["Bujur"] = longitude

    sekolah_id = url.split('/')[-1].strip()
    recapitulation_list = request_api(sekolah_id=sekolah_id)
    if recapitulation_list and isinstance(recapitulation_list, list) and len(recapitulation_list) > 0:
        school_data["recapitulation"] = recapitulation_list[0]
    else:
        school_data["recapitulation"] = {}

    return json.dumps(school_data, indent=4, ensure_ascii=False)

def lat_lon_parse(url: str):
    """Mengambil koordinat Lintang dan Bujur dari halaman referensi."""
    req = request_html(url)
    if not req:
        return None, None
        
    soup = BeautifulSoup(req, 'html.parser')
    latitude = None
    longitude = None
    scripts = soup.find_all('script')

    for script in scripts:
        if script.string:
            if "L.map('maps').setView" in script.string:
                lat_search = re.search(r"lat:\s*(-?\d+\.\d+)", script.string)
                lon_search = re.search(r"lon:\s*(-?\d+\.\d+)", script.string)

                if lat_search:
                    latitude = lat_search.group(1)
                if lon_search:
                    longitude = lon_search.group(1)
                break
    return latitude, longitude

def clean_province_name(name):
    """Menghapus 'PROV.', spasi, dan titik dari nama provinsi untuk perbandingan yang konsisten."""
    cleaned = name.upper()
    
    # 1. Hapus 'PROV.' jika ada
    if 'PROV.' in cleaned:
        cleaned = cleaned.replace('PROV.', '')
    
    # 2. Hapus titik
    cleaned = cleaned.replace('.', '')
    
    # 3. Hapus spasi ganda dan spasi di awal/akhir
    cleaned = ' '.join(cleaned.split())
    
    return cleaned.strip()

def main():
    """Fungsi utama untuk menjalankan proses scraping."""
    global BACKUP_COUNTER
    all_school_data = []

    # 1. Pilihan Provinsi
    response_provinsi = request_api()
    if not response_provinsi:
        print("Gagal mengambil daftar Provinsi. Program dihentikan.")
        return

    # --- PENCARIAN & SELEKSI OTOMATIS JAWA BARAT ---
    JAWA_BARAT_NAME_CLEAN = "JAWA BARAT" 
    prov_num = -1 

    print("Mencari provinsi target...")
    
    found_names = [] 
    
    for i, prov in enumerate(response_provinsi):
        # Bersihkan nama provinsi dari API sebelum membandingkan
        cleaned_api_name = clean_province_name(prov['nama'])
        found_names.append(prov['nama'])
        
        # Bandingkan nama yang sudah bersih
        if cleaned_api_name == JAWA_BARAT_NAME_CLEAN:
            prov_num = i
            break
            
    if prov_num == -1:
        print(f"[FATAL] Provinsi '{JAWA_BARAT_NAME_CLEAN}' tidak ditemukan dalam daftar API.")
        print("Daftar provinsi yang ditemukan: ", found_names)
        return
        
    province_target = response_provinsi[prov_num]
    provinsi_nama = province_target['nama'].strip()
    print(f"✨ Seleksi Otomatis: Provinsi **{provinsi_nama}** (Nomor: {prov_num}) dipilih.")
    # --- END SELEKSI OTOMATIS ---
    
    # 2. Setup File dan Direktori
    output_dir = './result'
    os.makedirs(output_dir, exist_ok=True)
    
    # --- INISIASI VARIABEL PROVINSI ---
    provinsi_level = province_target['id_level_wilayah']
    provinsi_kode = province_target['kode_wilayah'].strip()

    # 3. Level 2: Kota/Kabupaten
    response_kota_all = request_api(level_wilayah=provinsi_level, kode_wilayah=provinsi_kode, semester_id=SEMESTER_ID)
    
    if not response_kota_all:
        print(f"Gagal mengambil data Kota/Kabupaten untuk {provinsi_nama}.")
        return
        
    # --- LOGIKA FILTER KOTA/KABUPATEN (Target: Kota Depok) ---
    kota_targets = []
    DEPOK_NAME = "KOTA DEPOK"
    print(f"\n✨ DETEKSI: Provinsi {provinsi_nama} dipilih. Menerapkan filter **'{DEPOK_NAME}'**...")
    
    found_depok = False
    for kota in response_kota_all:
        # Gunakan .strip().upper() untuk perbandingan nama Kota/Kabupaten
        if kota['nama'].strip().upper() == DEPOK_NAME:
            kota_targets.append(kota)
            found_depok = True
            break
            
    if not found_depok:
        print(f"[FATAL] Filter '{DEPOK_NAME}' gagal ditemukan di {provinsi_nama}. Program dihentikan.")
        return
    else:
        print(f"Filter Berhasil Diterapkan. Hanya memproses: {DEPOK_NAME}")
        # Nama file CSV berdasarkan provinsi yang sudah dibersihkan
        csv_filename = f'{output_dir}/{clean_province_name(provinsi_nama).replace(" ", "_")}_DEPOK_data.csv'

    print(f"\nCSV akan dibuat setelah scraping selesai di: {csv_filename}")
    print(f"\n--- Memulai Scraping Kota: {DEPOK_NAME} ---")


    for kota in kota_targets:
        kota_nama_bersih = kota['nama'].strip().replace('/', '_')
        print(f"  Kota: {kota['nama'].strip()}")
        
        BACKUP_COUNTER = 0
        
        kota_level = kota['id_level_wilayah']
        kota_kode = kota['kode_wilayah'].strip()

        # Level 3: Kecamatan
        response_kecamatan = request_api(level_wilayah=kota_level, kode_wilayah=kota_kode, semester_id=SEMESTER_ID)
        
        if not response_kecamatan:
            print(f"  >>> Gagal mengambil data Kecamatan untuk {kota['nama'].strip()}")
            continue

        for kecamatan in response_kecamatan:
            kecamatan_nama = kecamatan['nama'].strip()
            print(f"    Kecamatan: {kecamatan_nama}")
            kecamatan_level = kecamatan['id_level_wilayah']
            
            kecamatan_kode = kecamatan['kode_wilayah'].strip() + '&bentuk_pendidikan_id='
            
            # Level 4: Sekolah
            response_sekolah = request_api(level_wilayah=kecamatan_level, kode_wilayah=kecamatan_kode, semester_id=SEMESTER_ID)
            
            if not response_sekolah:
                print(f"    >>> Tidak ada data sekolah ditemukan atau gagal mengambil API sekolah untuk {kecamatan_nama}")
                continue

            jenjang_target = ['SD', 'SMP', 'SMA', 'SPK SD', 'SPK SMP', 'SPK SMA', 'SMK']
            jenis_target = ['Negeri','Swasta']

            for sekolah in response_sekolah:
                if sekolah['bentuk_pendidikan'] in jenjang_target and sekolah['status_sekolah'] in jenis_target:
                    
                    sekolah_nama = sekolah['nama']
                    sekolah_id_enkrip = sekolah['sekolah_id_enkrip'].strip()
                    
                    print(f"      Sekolah: {sekolah_nama}")
                    
                    try:
                        school_url = f"https://dapo.dikdasmen.go.id/sekolah/{sekolah_id_enkrip}"
                        
                        school_detail_json = parse_html(school_url)
                        school_data = json.loads(school_detail_json)

                        final_row_dict = extract_data_to_dict(
                            school_data, provinsi_nama, kota['nama'].strip(), kecamatan_nama, sekolah_nama
                        )
                        final_row_dict['Scrape_Status'] = 'SUCCESS'

                        all_school_data.append(final_row_dict)
                        print(f"         [SUCCESS] Data {sekolah_nama} berhasil diekstraksi ke memori.")
                        
                        BACKUP_COUNTER += 1
                        if BACKUP_COUNTER >= BACKUP_THRESHOLD:
                            save_backup(all_school_data, kota_nama_bersih, output_dir, final_write=False)
                            BACKUP_COUNTER = 0
                        
                        time.sleep(1)
                        
                    except Exception as e:
                        error_message = str(e).splitlines()[0][:100] 
                        
                        minimal_row_dict = {
                            'Nama_Sekolah': sekolah_nama,
                            'Provinsi': provinsi_nama,
                            'Kota': kota['nama'].strip(),
                            'Kecamatan': kecamatan_nama,
                            'NPSN': sekolah.get('npsn', ''),
                            'Status': sekolah.get('status_sekolah', ''),
                            'Scrape_Status': f"FAILED - {error_message}",
                        }
                        
                        all_school_data.append(minimal_row_dict)
                        print(f"         [ERROR] Gagal memproses sekolah {sekolah_nama}: {e}. Dicatat sebagai FAILED.")
                        
                        BACKUP_COUNTER += 1
                        if BACKUP_COUNTER >= BACKUP_THRESHOLD:
                            save_backup(all_school_data, kota_nama_bersih, output_dir, final_write=False)
                            BACKUP_COUNTER = 0
                        
                        time.sleep(3)
    
    # Simpan sisa data ke file backup final
    if all_school_data:
        save_backup(all_school_data, kota_nama_bersih, output_dir, final_write=True)

    print(f"\n--- Scraping Selesai. Total {len(all_school_data)} data ditemukan. Menulis ke CSV Final ---")
    if all_school_data:
        try:
            df = pd.DataFrame(all_school_data)
            df.to_csv(csv_filename, index=False, encoding='utf-8')
            print(f"✅ Penulisan Berhasil! Data disimpan di: {csv_filename}")
        except Exception as e:
            print(f"❌ ERROR saat menulis ke CSV menggunakan Pandas: {e}")
    else:
        print("TIDAK ADA DATA sekolah yang berhasil di-scrape.")


if __name__ == '__main__':
    main()