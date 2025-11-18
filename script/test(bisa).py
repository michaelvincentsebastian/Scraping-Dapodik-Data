import requests
from bs4 import BeautifulSoup
import re
import json
import pandas as pd
import csv
import time
import urllib3
import os
import sys # Tambahkan import sys untuk exit gracefully

# Menonaktifkan peringatan SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- KONSTANTA GLOBAL ---
BASE_URL = 'https://dapo.kemendikdasmen.go.id'
SEMESTER_ID = '20251' # Ganti sesuai semester yang diinginkan
LEVEL_WILAYAH = '0'
KODE_WILAYAH = '000000'

def request_api(base_url: str = BASE_URL, level_wilayah: str = LEVEL_WILAYAH, kode_wilayah: str = KODE_WILAYAH, semester_id: str = SEMESTER_ID, sekolah_id: str = None) -> list:
    """Mengambil data wilayah atau rekapitulasi sekolah dari API Dapo."""
    try:
        if sekolah_id:
            # API untuk detail rekapitulasi sekolah
            url = f'{base_url}/rekap/sekolahDetail?semester_id={semester_id}&sekolah_id={sekolah_id}'
        elif level_wilayah == 3:
            # API untuk progres SP (jarang dipakai di skenario ini) - Mengikuti kode asli
            url = f'{base_url}/rekap/progresSP?id_level_wilayah={level_wilayah}&kode_wilayah={kode_wilayah}&semester_id={semester_id}&bentuk_pendidikan_id='
        else:
            # API untuk data sekolah/wilayah (provinsi/kota/kecamatan) - Mengikuti kode asli
            url = f'{base_url}/rekap/dataSekolah?id_level_wilayah={level_wilayah}&kode_wilayah={kode_wilayah}&semester_id={semester_id}'

        req = requests.get(url)
        # Handle jika API mengembalikan respons non-JSON (misalnya error 500 atau 404)
        if req.status_code != 200:
            print(f"API Error {req.status_code} for URL: {url}")
            return []
        return req.json()
    except Exception as e:
        print(f"Error requesting API for {kode_wilayah}: {e}")
        return []

# Fungsi-fungsi lain (request_html, create_csv_header, append_to_csv, parse_html, lat_lon_parse)
# tidak diubah, karena tidak ada error di sana.
def request_html(url: str, backoff: float = 2.0) -> str:
    """Mengambil konten HTML dengan mekanisme retry sederhana."""
    for attempt in range(3): # Coba 3 kali
        try:
            if not url.startswith('http'):
                raise ValueError(f"Invalid URL: {url}")
            res = requests.get(url, verify=False, timeout=10) # Tambahkan timeout
            if res.status_code == 404:
                return "" # Return string kosong jika 404
            
            # Cek jika ada Human Validation (Captcha/Cloudflare)
            if "User validation required" in res.text or "Checking your browser" in res.text:
                print(f"        [WARNING] Validasi Pengguna Ditemukan. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2 # Naikkan backoff time
                continue
            return res.text
        except requests.RequestException as e:
            print(f"        [NETWORK ERROR] {e}, retrying in {backoff}s...")
            time.sleep(backoff)
            backoff *= 2
    return "" # Return string kosong jika semua percobaan gagal

def create_csv_header(filename):
    """Membuat file CSV baru dengan header."""
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

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)

def append_to_csv(filename, school_data, school_name, province, kota, kecamatan):
    """Menambahkan data sekolah ke file CSV."""
    profile = school_data.get('profile', {})
    contact = school_data.get('contact', {})
    # Pastikan mengambil dictionary pertama dari list rekapitulasi, yang sudah dilakukan di parse_html
    recapitulation = school_data.get('recapitulation', {})

    # Extract data with default empty values
    identitas = profile.get('identitas_sekolah', {})
    pelengkap = profile.get('data_pelengkap', {})
    rinci = profile.get('data_rinci', {})
    sidebar = profile.get('sidebar_info', {})

    # --- PENGHITUNGAN TOTAL REKAPITULASI ---
    Guru_L = recapitulation.get('ptk_laki', 0)
    Guru_P = recapitulation.get('ptk_perempuan', 0)
    Guru_Total = Guru_L + Guru_P
    
    # Catatan: API Dapodik kadang membedakan PTK (Pendidik dan Tenaga Kependidikan) dan Pegawai (Tendik).
    # Di sini, kita asumsikan Tendik adalah Pegawai.
    Tendik_L = recapitulation.get('pegawai_laki', 0)
    Tendik_P = recapitulation.get('pegawai_perempuan', 0)
    Tendik_Total = Tendik_L + Tendik_P
    
    # PTK (Total Pendidik dan Tenaga Kependidikan)
    PTK_L = recapitulation.get('ptk_laki', 0) + recapitulation.get('pegawai_laki', 0)
    PTK_P = recapitulation.get('ptk_perempuan', 0) + recapitulation.get('pegawai_perempuan', 0)
    PTK_Total = PTK_L + PTK_P
    
    PD_L = recapitulation.get('pd_laki', 0)
    PD_P = recapitulation.get('pd_perempuan', 0)
    PD_Total = PD_L + PD_P

    # --- MEMBANGUN BARIS CSV ---
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

    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(row)

def parse_html(url: str):
    """Mengurai (parse) halaman HTML detail sekolah dan mengambil data profil/kontak."""
    req = request_html(url)
    if not req:
        # Jika request_html gagal mengambil konten
        return json.dumps({"profile": {}, "recapitulation": {}, "contact": {}}, ensure_ascii=False)
    
    soup = BeautifulSoup(req, 'html.parser')
    school_data = {
        "profile": {},
        "recapitulation": {},
        "contact": {}
    }
    
    # 1. Mengambil data PROFIL
    profile_panels = soup.select('#profil .panel-info')
    
    # Untuk menyimpan data sidebar (Kepsek, Akreditasi, dll.)
    sidebar_data = {}
    
    for panel in profile_panels:
        heading = panel.find(class_='panel-heading').get_text(strip=True)
        body = panel.find(class_='panel-body')
        section_data = {}
        if body: # Pastikan body ada
            for p in body.find_all('p'):
                if p.find('strong'):
                    key = p.find('strong').get_text(strip=True).replace(':', '').strip()
                    # Ambil teks setelah tag <strong>
                    value = p.strong.next_sibling.strip() if p.strong.next_sibling else ''
                    section_data[key] = value
            
            if "Identitas" in heading:
                school_data["profile"]["identitas_sekolah"] = section_data
            elif "Pelengkap" in heading:
                school_data["profile"]["data_pelengkap"] = section_data
            elif "Rinci" in heading:
                school_data["profile"]["data_rinci"] = section_data

    # Mengambil data dari sidebar
    sidebar_menu = soup.find(class_='profile-usermenu')
    if sidebar_menu:
        for item in sidebar_menu.find_all('li'):
            text = item.get_text(strip=True)
            if ':' in text:
                key, value = text.split(':', 1)
                sidebar_data[key.strip()] = value.strip()
    school_data["profile"]["sidebar_info"] = sidebar_data # Masukkan sidebar data ke school_data

    # 2. Mengambil data KONTAK
    contact_panel = soup.select_one('#kontak .panel-info')
    if contact_panel:
        contact_info = {}
        for p in contact_panel.find_all('p'):
            if p.find('strong'):
                key = p.find('strong').get_text(strip=True).replace(':', '').strip()
                value = p.strong.next_sibling.strip() if p.strong.next_sibling else ''
                contact_info[key] = value
        school_data["contact"] = contact_info

    # 3. Mengambil Lintang/Bujur dari referensi kemdikbud
    npsn = school_data["profile"].get("identitas_sekolah", {}).get("NPSN", "")
    if npsn:
        urls_latlon = f'https://referensi.data.kemdikbud.go.id/pendidikan/npsn/{npsn}'
        latitude, longitude = lat_lon_parse(urls_latlon)

        if latitude or longitude:
            school_data["contact"]["Lintang"] = latitude
            school_data["contact"]["Bujur"] = longitude

    # 4. Mengambil data REKAPITULASI (API Call)
    sekolah_id = url.split('/')[-1].strip()
    recapitulation_list = request_api(sekolah_id=sekolah_id)
    if recapitulation_list and isinstance(recapitulation_list, list) and len(recapitulation_list) > 0:
        # Kita ambil elemen pertama dari list rekapitulasi
        school_data["recapitulation"] = recapitulation_list[0]
    else:
        # Jika gagal atau kosong, pastikan rekapitulasi adalah dictionary kosong
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
            # Mencari script yang berisi peta Leaflet/Mapbox
            if "L.map('maps').setView" in script.string:
                lat_search = re.search(r"lat:\s*(-?\d+\.\d+)", script.string)
                lon_search = re.search(r"lon:\s*(-?\d+\.\d+)", script.string)

                if lat_search:
                    latitude = lat_search.group(1)
                if lon_search:
                    longitude = lon_search.group(1)
                break
    return latitude, longitude

def main():
    """Fungsi utama untuk menjalankan proses scraping dengan filter Kota/Kabupaten Bogor."""
    
    # 1. Pilihan Provinsi
    response_provinsi = request_api()
    if not response_provinsi:
        print("Gagal mengambil daftar Provinsi. Program dihentikan.")
        return

    print("--- DAFTAR PROVINSI ---")
    for i, prov in enumerate(response_provinsi):
        print(f"[{i}] {prov['nama']}")
    print("------------------------")
    
    try:
        prov_num = int(input(f"Masukkan nomor provinsi (0 sampai {len(response_provinsi)-1}) yang ingin di-scrape: "))
        if not (0 <= prov_num < len(response_provinsi)):
            print("Nomor provinsi tidak valid.")
            return
    except ValueError:
        print("Input harus berupa angka.")
        return
        
    province_target = response_provinsi[prov_num]
    
    # 2. Setup File dan Direktori
    output_dir = './result'
    os.makedirs(output_dir, exist_ok=True)
    
    # --- INISIASI VARIABEL PROVINSI ---
    provinsi_nama = province_target['nama']
    provinsi_level = province_target['id_level_wilayah']
    provinsi_kode = province_target['kode_wilayah'].strip()

    # 3. Level 2: Kota/Kabupaten
    response_kota_all = request_api(level_wilayah=provinsi_level, kode_wilayah=provinsi_kode, semester_id=SEMESTER_ID)
    
    if not response_kota_all:
        print(f"Gagal mengambil data Kota/Kabupaten untuk {provinsi_nama}.")
        return
        
    # --- LOGIKA FILTER BOGOR ---
    kota_targets = []
    if provinsi_nama.upper() == "JAWA BARAT":
        print(f"\n✨ DETEKSI: Provinsi {provinsi_nama} dipilih. Menerapkan filter 'Kab. Bogor' dan 'Kota Bogor'...")
        BOGOR_NAMES = ["KABUPATEN BOGOR", "KOTA BOGOR"]
        
        for kota in response_kota_all:
            if kota['nama'].upper() in BOGOR_NAMES:
                kota_targets.append(kota)
                
        if not kota_targets:
            print("[PERINGATAN] Filter Bogor gagal menemukan Kota/Kabupaten. Memproses SEMUA kota di Jawa Barat.")
            kota_targets = response_kota_all
        else:
            kota_list_str = ", ".join([k['nama'] for k in kota_targets])
            print(f"Filter Berhasil Diterapkan. Hanya memproses: {kota_list_str}")
            csv_filename = f'{output_dir}/{provinsi_nama.replace(" ", "_")}_BOGOR_data.csv'
    else:
        # Jika bukan Jawa Barat, proses semua kota.
        print(f"\nMemproses SEMUA {len(response_kota_all)} Kota/Kabupaten di {provinsi_nama}.")
        kota_targets = response_kota_all
        csv_filename = f'{output_dir}/{provinsi_nama.replace(" ", "_")}_ALL_data.csv'


    # Buat header CSV
    create_csv_header(csv_filename)
    print(f"\nCSV header berhasil dibuat di: {csv_filename}")
    
    print(f"\n--- Memulai Scraping Provinsi: {provinsi_nama} ---")


    for kota in kota_targets:
        print(f"  Kota: {kota['nama']}")
        kota_level = kota['id_level_wilayah']
        kota_kode = kota['kode_wilayah'].strip()

        # Level 3: Kecamatan
        response_kecamatan = request_api(level_wilayah=kota_level, kode_wilayah=kota_kode, semester_id=SEMESTER_ID)
        
        if not response_kecamatan:
            print(f"  >>> Gagal mengambil data Kecamatan untuk {kota['nama']}")
            continue

        for kecamatan in response_kecamatan:
            print(f"    Kecamatan: {kecamatan['nama']}")
            kecamatan_level = kecamatan['id_level_wilayah']
            
            # Kode API untuk Sekolah di level Kecamatan (LEVEL 4)
            # Di kode asli, request_api dipanggil dengan level_wilayah=kecamatan_level (yang nilainya 3)
            # dan kode_wilayah: kode_kecamatan + '&bentuk_pendidikan_id='
            kecamatan_kode = kecamatan['kode_wilayah'].strip() + '&bentuk_pendidikan_id='
            
            # Level 4: Sekolah
            # Karena kode_wilayah sudah disesuaikan dengan kebutuhan API sekolah (level 4), 
            # kita tetap menggunakan struktur pemanggilan API yang sama dengan kode asli:
            response_sekolah = request_api(level_wilayah=kecamatan_level, kode_wilayah=kecamatan_kode, semester_id=SEMESTER_ID)
            
            if not response_sekolah:
                print(f"    >>> Tidak ada data sekolah ditemukan atau gagal mengambil API sekolah untuk {kecamatan['nama']}")
                continue

            jenjang_target = ['SD', 'SMP', 'SMA', 'SPK SD', 'SPK SMP', 'SPK SMA', 'SMK']
            jenis_target = ['Negeri','Swasta']

            for sekolah in response_sekolah:
                if sekolah['bentuk_pendidikan'] in jenjang_target and sekolah['status_sekolah'] in jenis_target:
                    
                    sekolah_nama = sekolah['nama']
                    sekolah_id_enkrip = sekolah['sekolah_id_enkrip'].strip()
                    
                    print(f"      Sekolah: {sekolah_nama}")
                    
                    try:
                        # URL detail sekolah (HTML scraping)
                        school_url = f"https://dapo.dikdasmen.go.id/sekolah/{sekolah_id_enkrip}"
                        
                        # Ambil data detail (HTML scraping + API rekapitulasi)
                        school_detail_json = parse_html(school_url)
                        school_data = json.loads(school_detail_json)

                        # Tambahkan ke CSV
                        append_to_csv(
                            csv_filename,
                            school_data,
                            sekolah_nama,
                            provinsi_nama,
                            kota['nama'],
                            kecamatan['nama']
                        )
                        print(f"        [SUCCESS] Data {sekolah_nama} ditambahkan ke CSV.")
                        time.sleep(1) # Jeda sebentar antar sekolah
                        
                    except Exception as e:
                        print(f"        [ERROR] Gagal memproses sekolah {sekolah_nama}: {e}")
                        time.sleep(3) # Jeda lebih lama jika ada error

if __name__ == '__main__':
    main()