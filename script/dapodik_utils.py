import requests
from bs4 import BeautifulSoup
import re 
import json
import csv
import time
import urllib3
import os

# Menonaktifkan peringatan SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# KONFIGURASI GLOBAL
# =========================
BASE_URL = 'https://dapo.kemendikdasmen.go.id'
SEMESTER_ID = '20251'
LEVEL_WILAYAH = '0'
KODE_WILAYAH = '000000'
REQUEST_TIMEOUT = 10 # detik


# =========================
# REQUEST API (INFINITE RETRY)
# =========================
def request_api(base_url: str = BASE_URL,
                level_wilayah=LEVEL_WILAYAH,
                kode_wilayah: str = KODE_WILAYAH,
                semester_id: str = SEMESTER_ID,
                sekolah_id: str = None,
                backoff: float = 0.5) -> dict:
    """
    Wrapper untuk semua endpoint rekap API dapo.
    Infinite retry dengan delay pendek (default 0.5s).
    """
    # *** ISI FUNGSI request_api() Anda di sini ***
    url = None
    while True:
        try:
            # cast level ke int kalau bisa, fallback ke value as-is
            try:
                lvl = int(level_wilayah) if level_wilayah is not None else 0
            except ValueError:
                lvl = level_wilayah

            if lvl == 3 and not sekolah_id:
                # progresSP untuk level kecamatan
                url = (
                    f'{base_url}/rekap/progresSP?'
                    f'id_level_wilayah={lvl}'
                    f'&kode_wilayah={kode_wilayah}'
                    f'&semester_id={semester_id}'
                    f'&bentuk_pendidikan_id='
                )
            elif sekolah_id:
                # detail sekolah (rekap)
                url = (
                    f'{base_url}/rekap/sekolahDetail?'
                    f'semester_id={semester_id}'
                    f'&sekolah_id={sekolah_id}'
                )
            else:
                # daftar wilayah / sekolah
                url = (
                    f'{base_url}/rekap/dataSekolah?'
                    f'id_level_wilayah={lvl}'
                    f'&kode_wilayah={kode_wilayah}'
                    f'&semester_id={semester_id}'
                )

            print(f"[API] GET {url}")
            res = requests.get(url, timeout=REQUEST_TIMEOUT, verify=False)

            if res.status_code != 200:
                print(f"[API] {res.status_code} untuk {url}, retry dalam {backoff}s...")
                time.sleep(backoff)
                continue

            text = res.text.strip()
            if text.startswith("<!DOCTYPE html") or text.startswith("<html"):
                print("[API] Server balas HTML (kemungkinan anti-bot), retry...")
                time.sleep(backoff)
                continue
            return res.json()

        except Exception as e:
            if url:
                print(f"[API ERROR] {e} untuk {url}, retry dalam {backoff}s...")
            else:
                print(f"[API ERROR] {e} (URL belum terbentuk), retry dalam {backoff}s...")
            time.sleep(backoff)


# =========================
# REQUEST HTML (INFINITE RETRY)
# =========================
def request_html(url: str, backoff: float = 0.5) -> str:
    # *** ISI FUNGSI request_html() Anda di sini ***
    while True:
        try:
            if not url.startswith('http'):
                raise ValueError(f"Invalid URL: {url}")

            print(f"[HTML] GET {url}")
            res = requests.get(url, verify=False, timeout=REQUEST_TIMEOUT)

            if res.status_code != 200:
                print(f"[HTML] {res.status_code} untuk {url}, retry dalam {backoff}s...")
                time.sleep(backoff)
                continue

            text = res.text
            if "User validation required" in text or "Checking your browser" in text:
                print("[HTML] User validation / anti-bot, retry...")
                time.sleep(backoff)
                continue
            return text

        except requests.RequestException as e:
            print(f"[HTML ERROR] Network: {e}, retry dalam {backoff}s...")
            time.sleep(backoff)
        except Exception as e:
            print(f"[HTML ERROR] Other: {e}, retry dalam {backoff}s...")
            time.sleep(backoff)


# =========================
# CSV RELATED (KONSTANTA & FUNGSI)
# =========================
CSV_HEADERS = [
    'sekolah_id_enkrip', 'Nama_Sekolah', 'Provinsi', 'Kota_Kabupaten', 'Kecamatan',
    'NPSN', 'Status', 'Bentuk_Pendidikan', 'Status_Kepemilikan',
    'SK_Pendirian_Sekolah', 'Tanggal_SK_Pendirian', 'SK_Izin_Operasional',
    'Tanggal_SK_Izin_Operasional', 'Kebutuhan_Khusus_Dilayani', 'Nama_Bank',
    'Cabang_KCP_Unit', 'Rekening_Atas_Nama', 'Status_BOS',
    'Waktu_Penyelenggaraan', 'Sertifikasi_ISO', 'Sumber_Listrik',
    'Daya_Listrik', 'Kecepatan_Internet', 'Kepsek', 'Operator',
    'Akreditasi', 'Kurikulum', 'Waktu', 'Alamat', 'RT_RW',
    'Dusun', 'Desa_Kelurahan', 'Kode_Pos', 'Lintang', 'Bujur',
    'Guru_L', 'Guru_P', 'Guru_Total',
    'Tendik_L', 'Tendik_P', 'Tendik_Total',
    'PTK_L', 'PTK_P', 'PTK_Total',
    'Peserta_Didik_L', 'Peserta_Didik_P', 'Peserta_Didik_Total',
    'Ruang_Kelas', 'Ruang_Perpus', 'Ruang_Lab', 'Ruang_Pratik',
    'Rombel'
]

def create_csv_header(filename: str) -> None:
    # *** ISI FUNGSI create_csv_header() Anda di sini ***
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    if os.path.exists(filename):
        return
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(CSV_HEADERS)


def load_processed_ids(filename: str) -> set:
    # *** ISI FUNGSI load_processed_ids() Anda di sini ***
    processed = set()
    if not os.path.exists(filename):
        return processed
    with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
        try:
            reader = csv.DictReader(csvfile)
            for row in reader:
                sid = row.get('sekolah_id_enkrip')
                if sid:
                    processed.add(sid.strip())
        except Exception:
            print("[CSV] Gagal membaca CSV (mungkin kosong atau header error).")
            pass
    return processed


def append_to_csv(filename: str,
                  sekolah_id_enkrip: str,
                  school_data: dict,
                  school_name: str,
                  province: str,
                  kota: str,
                  kecamatan: str) -> bool:
    # *** ISI FUNGSI append_to_csv() Anda di sini (termasuk flush/fsync!) ***
    profile = school_data.get('profile', {})
    contact = school_data.get('contact', {})
    recapitulation = school_data.get('recapitulation', {})
    identitas = profile.get('identitas_sekolah', {})
    pelengkap = profile.get('data_pelengkap', {})
    rinci = profile.get('data_rinci', {})
    sidebar = profile.get('sidebar_info', {})

    Guru_L = recapitulation.get('ptk_laki', 0); Guru_P = recapitulation.get('ptk_perempuan', 0); Guru_Total = Guru_L + Guru_P
    Tendik_L = recapitulation.get('pegawai_laki', 0); Tendik_P = recapitulation.get('pegawai_perempuan', 0); Tendik_Total = Tendik_L + Tendik_P
    PTK_L = Guru_L + Tendik_L; PTK_P = Guru_P + Tendik_P; PTK_Total = PTK_L + PTK_P
    PD_L = recapitulation.get('pd_laki', 0); PD_P = recapitulation.get('pd_perempuan', 0); PD_Total = PD_L + PD_P
    ruang_kelas = recapitulation.get('after_ruang_kelas', recapitulation.get('before_ruang_kelas', 0))
    ruang_perpus = recapitulation.get('after_ruang_perpus', recapitulation.get('before_ruang_perpus', 0))
    ruang_lab = recapitulation.get('after_ruang_lab', recapitulation.get('before_ruang_lab', 0))
    ruang_pratik = recapitulation.get('after_ruang_praktik', recapitulation.get('before_ruang_praktik', 0))

    row = [
        sekolah_id_enkrip, school_name, province, kota, kecamatan, identitas.get('NPSN', ''), identitas.get('Status', ''), identitas.get('Bentuk Pendidikan', ''), identitas.get('Status Kepemilikan', ''),
        identitas.get('SK Pendirian Sekolah', ''), identitas.get('Tanggal SK Pendirian', ''), identitas.get('SK Izin Operasional', ''), identitas.get('Tanggal SK Izin Operasional', ''),
        pelengkap.get('Kebutuhan Khusus Dilayani', ''), pelengkap.get('Nama Bank', ''), pelengkap.get('Cabang KCP/Unit', ''), pelengkap.get('Rekening Atas Nama', ''), rinci.get('Status BOS', ''),
        rinci.get('Waku Penyelenggaraan', ''), rinci.get('Sertifikasi ISO', ''), rinci.get('Sumber Listrik', ''), rinci.get('Daya Listrik', ''), rinci.get('Kecepatan Internet', ''),
        sidebar.get('Kepsek', ''), sidebar.get('Operator', ''), sidebar.get('Akreditasi', ''), sidebar.get('Kurikulum', ''), sidebar.get('Waktu', ''), contact.get('Alamat', ''),
        contact.get('RT / RW', ''), contact.get('Dusun', ''), contact.get('Desa / Kelurahan', ''), contact.get('Kode Pos', ''), contact.get('Lintang', ''), contact.get('Bujur', ''),
        Guru_L, Guru_P, Guru_Total, Tendik_L, Tendik_P, Tendik_Total, PTK_L, PTK_P, PTK_Total, PD_L, PD_P, PD_Total,
        ruang_kelas, ruang_perpus, ruang_lab, ruang_pratik, recapitulation.get('rombel', 0)
    ]

    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(row)
            csvfile.flush()
            os.fsync(csvfile.fileno()) 
        return True
    except PermissionError:
        print(f"      🚨 ERROR TULIS CSV: Gagal menulis ke '{filename}'. File MUNGKIN SEDANG TERBUKA DI APLIKASI LAIN. Data ini dilewati.")
        return False
    except Exception as e:
        print(f"      🚨 ERROR TULIS CSV: Error tak terduga: {e}. Data ini dilewati.")
        return False


# =========================
# PARSER HTML PROFIL SEKOLAH
# =========================
def parse_html(url: str) -> dict:
    # *** ISI FUNGSI parse_html() Anda di sini ***
    req = request_html(url)
    soup = BeautifulSoup(req, 'html.parser')
    school_data = {"profile": {}, "recapitulation": {}, "contact": {}}
    profile_panels = soup.select('#profil .panel-info')

    for panel in profile_panels:
        heading_el = panel.find(class_='panel-heading')
        if not heading_el: continue
        heading = heading_el.get_text(strip=True)
        body = panel.find(class_='panel-body')
        if not body: continue

        section_data = {}
        for p in body.find_all('p'):
            strong_tag = p.find('strong')
            if strong_tag:
                key = strong_tag.get_text(strip=True).replace(':', '').strip()
                value = strong_tag.next_sibling.strip() if strong_tag.next_sibling else ''
                section_data[key] = value

        if "Identitas" in heading: school_data["profile"]["identitas_sekolah"] = section_data
        elif "Pelengkap" in heading: school_data["profile"]["data_pelengkap"] = section_data
        elif "Rinci" in heading: school_data["profile"]["data_rinci"] = section_data

    sidebar_menu = soup.find(class_='profile-usermenu')
    if sidebar_menu:
        sidebar_data = {}
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
            strong_tag = p.find('strong')
            if strong_tag:
                key = strong_tag.get_text(strip=True).replace(':', '').strip()
                value = strong_tag.next_sibling.strip() if strong_tag.next_sibling else ''
                contact_info[key] = value
        school_data["contact"] = contact_info

    sekolah_id = url.split('/')[-1].strip()
    recapitulation = request_api(sekolah_id=sekolah_id, backoff=5)
    if recapitulation and isinstance(recapitulation, list) and len(recapitulation) > 0:
        school_data["recapitulation"] = recapitulation[0]

    return school_data