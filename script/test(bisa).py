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

# Membuat sesi Request Global untuk koneksi yang lebih efisien
# ⚠️ PERUBAHAN 1: INISIASI GLOBAL SESSION
session = requests.Session()


def request_api(base_url: str = BASE_URL, level_wilayah: str = LEVEL_WILAYAH, kode_wilayah: str = KODE_WILAYAH, semester_id: str = SEMESTER_ID, sekolah_id: str = None) -> list:
    """Mengambil data wilayah atau rekapitulasi sekolah dari API Dapo."""
    global session # Gunakan sesi yang telah dibuat
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

        req = session.get(url) # ⚠️ PERUBAHAN 2: MENGGUNAKAN SESSION
        # Handle jika API mengembalikan respons non-JSON (misalnya error 500 atau 404)
        if req.status_code != 200:
            print(f"API Error {req.status_code} for URL: {url}")
            return []
        return req.json()
    except Exception as e:
        print(f"Error requesting API for {kode_wilayah}: {e}")
        return []


def request_html(url: str, backoff: float = 2.0) -> str:
    """Mengambil konten HTML dengan mekanisme retry sederhana."""
    global session # Gunakan sesi yang telah dibuat
    for attempt in range(3): # Coba 3 kali
        try:
            if not url.startswith('http'):
                raise ValueError(f"Invalid URL: {url}")
            res = session.get(url, verify=False, timeout=10) # ⚠️ PERUBAHAN 3: MENGGUNAKAN SESSION
            if res.status_code == 404:
                return "" # Return string kosong jika 404
            
            # Cek jika ada Human Validation (Captcha/Cloudflare)
            if "User validation required" in res.text or "Checking your browser" in res.text:
                print(f"         [WARNING] Validasi Pengguna Ditemukan. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2 # Naikkan backoff time
                continue
            return res.text
        except requests.RequestException as e:
            print(f"         [NETWORK ERROR] {e}, retrying in {backoff}s...")
            time.sleep(backoff)
            backoff *= 2
    return "" # Return string kosong jika semua percobaan gagal

# ⚠️ PERUBAHAN 4: FUNGSI create_csv_header DIHAPUS karena Pandas akan membuat header di akhir.
# ⚠️ PERUBAHAN 5: FUNGSI append_to_csv DIUBAH menjadi extract_data_to_dict yang mengembalikan Dictionary.
def extract_data_to_dict(school_data, school_name, province, kota, kecamatan):
    """Mengubah data sekolah hasil scrape menjadi satu dictionary (row)."""
    profile = school_data.get('profile', {})
    contact = school_data.get('contact', {})
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
    # ... (fungsi ini tidak diubah secara signifikan, karena logika ekstraksi sudah benar) ...
    req = request_html(url)
    if not req:
        return json.dumps({"profile": {}, "recapitulation": {}, "contact": {}}, ensure_ascii=False)
    
    soup = BeautifulSoup(req, 'html.parser')
    school_data = {
        "profile": {},
        "recapitulation": {},
        "contact": {}
    }
    
    # 1. Mengambil data PROFIL
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

    # Mengambil data dari sidebar
    sidebar_menu = soup.find(class_='profile-usermenu')
    if sidebar_menu:
        for item in sidebar_menu.find_all('li'):
            text = item.get_text(strip=True)
            if ':' in text:
                key, value = text.split(':', 1)
                sidebar_data[key.strip()] = value.strip()
    school_data["profile"]["sidebar_info"] = sidebar_data

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
        school_data["recapitulation"] = recapitulation_list[0]
    else:
        school_data["recapitulation"] = {}

    return json.dumps(school_data, indent=4, ensure_ascii=False)

def lat_lon_parse(url: str):
    """Mengambil koordinat Lintang dan Bujur dari halaman referensi."""
    # ... (fungsi ini tidak diubah, namun sekarang menggunakan request_html yang sudah di-update) ...
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
    
    # ⚠️ PERUBAHAN 6: INISIASI LIST KOSONG UNTUK MENYIMPAN SEMUA DATA DI MEMORI
    all_school_data = []

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
        
    # --- LOGIKA FILTER KOTA/KABUPATEN ---
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


    # ⚠️ PERUBAHAN 7: Hapus create_csv_header()
    # create_csv_header(csv_filename)
    # print(f"\nCSV header berhasil dibuat di: {csv_filename}")
    print(f"\nCSV akan dibuat setelah scraping selesai di: {csv_filename}") # Pemberitahuan baru
    
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
            
            kecamatan_kode = kecamatan['kode_wilayah'].strip() + '&bentuk_pendidikan_id='
            
            # Level 4: Sekolah
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

                        # ⚠️ PERUBAHAN 8: Ganti append_to_csv dengan extract_data_to_dict dan append ke list
                        final_row_dict = extract_data_to_dict(
                            school_data,
                            sekolah_nama,
                            provinsi_nama,
                            kota['nama'],
                            kecamatan['nama']
                        )
                        
                        all_school_data.append(final_row_dict)
                        print(f"        [SUCCESS] Data {sekolah_nama} berhasil diekstraksi ke memori.")
                        time.sleep(1) # Jeda sebentar antar sekolah
                        
                    except Exception as e:
                        print(f"        [ERROR] Gagal memproses sekolah {sekolah_nama}: {e}")
                        time.sleep(3) # Jeda lebih lama jika ada error
    
    # ⚠️ PERUBAHAN 9: TULIS DATA KE CSV HANYA SEKALI MENGGUNAKAN PANDAS
    print(f"\n--- Scraping Selesai. Total {len(all_school_data)} data ditemukan. Menulis ke CSV ---")
    if all_school_data:
        try:
            df = pd.DataFrame(all_school_data)
            # Menulis DataFrame ke CSV
            df.to_csv(csv_filename, index=False, encoding='utf-8')
            print(f"✅ Penulisan Berhasil! Data disimpan di: {csv_filename}")
        except Exception as e:
            print(f"❌ ERROR saat menulis ke CSV menggunakan Pandas: {e}")
    else:
        print("TIDAK ADA DATA sekolah yang berhasil di-scrape.")


if __name__ == '__main__':
    main()