# 📊 Proyek Scraping Data Sekolah Dapodik

Proyek ini mengekstrak data detail sekolah (profil, PTK, peserta didik, dan sarana prasarana) dari situs Dapodik Kemendikbud.

Wilayah target:

- **Kabupaten Bekasi**
- **Kota Bekasi**
- **Kota Depok**

---

## 📑 Daftar Isi
- [Struktur Proyek](#-struktur-proyek)
- [Instalasi](#%EF%B8%8F-instalasi)
- [Penggunaan](#%EF%B8%8F-penggunaan)
- [Penjelasan Program](#-penjelasan-program)
- [Hasil Data CSV](#-hasil-data-csv)
- [Troubleshooting & Solusi](#-troubleshooting--solusi)

---

## 📁 Struktur Proyek

```plaintext
.
├── result/
│   ├── data_Bekasi.csv     <-- Kota & Kab. Bekasi
│   └── data_Depok.csv      <-- Kota Depok
│
├── script/
|   ├── dapodik_utils.py          <-- FUNGSI UTAMA (REQUEST, PARSING, CSV)
│   ├── kabBekasi.py              <-- Script Kabupaten Bekasi
│   ├── kotaBekasi.py             <-- Script Kota Bekasi
│   └── kotaDepok.py              <-- Script Kota Depok
├── .gitattributes
├── .gitignore
└── README.md
```

---

## ⚙️ Instalasi

### Prerequisite
- **Python 3.x** telah terpasang di sistem Anda.

### Instalasi Library

```bash
pip install requests beautifulsoup4 urllib3
```
Modul **os**, **csv**, **json**, dan **time** sudah ada di Python, tidak perlu instal.

---

## ▶️ Penggunaan

| Langkah | Perintah | Output |
| :--- | :--- | :--- |
| **1. Scraping Kota Depok** | `python script/kotaDepok.py` | `result/data_Depok.csv` |
| **2. Scraping Kota Bekasi** | `python script/kotaBekasi.py` | `result/data_Bekasi.csv` |
| **3. Scraping Kab. Bekasi** | `python script/kabBekasi.py` | Menambah ke `result/data_Bekasi.csv` |

### 📌 Catatan

- Fitur **resume otomatis** aktif—ID sekolah yang sudah di CSV akan dilewati.
- Disarankan memberi jeda **beberapa jam** antar-skrip untuk mencegah **IP ban sementara** server Dapodik.

---

## 📝 Penjelasan Program

Konsep modularitas memastikan setiap *script* utama hanya fokus pada **logika *looping* wilayah** dan **filtering**. Semua interaksi teknis (*request*, *retry*, *parsing*, *append* CSV) ditangani oleh modul **`dapodik_utils.py`**.

| Program         | Peran                                             | Filter Wilayah Spesifik                                                           | Output File               |
| :-------------- | :------------------------------------------------ | :---------------------------------------------------------------------------------- | :------------------------ |
| **`dapodik_utils.py`** | Menyediakan **fungsi inti** (`request_api`, `parse_html`, `append_to_csv`) untuk dipakai ulang oleh semua skrip. | (Tidak ada filter wilayah; hanya menyediakan fungsi dasar) | (Tidak menghasilkan output) |
| `kabBekasi.py`    | **Skrip Utama:** Menargetkan & mengoleksi data **Kabupaten Bekasi**. | Mengandung `KAB` & `BEKASI`, **tidak** mengandung `KOTA` | `data_Bekasi.csv`     |
| `kotaBekasi.py`   | **Skrip Utama:** Menargetkan & mengoleksi data **Kota Bekasi**. | Mengandung `KOTA BEKASI`                                                           | `data_Bekasi.csv`     |
| `kotaDepok.py`    | **Skrip Utama:** Menargetkan & mengoleksi data **Kota Depok**. | Mengandung `KOTA DEPOK`                                                            | `data_Depok.csv`      |

---

## 📦 Hasil Data CSV

| File                      | Isi Data                                         | Kolom   |
| :------------------------ | :----------------------------------------------- | :------ |
| data_Bekasi.csv     | Gabungan Kab. Bekasi & Kota Bekasi              | 52    |
| data_Depok.csv      | Data Kota Depok                                 | 52    |

## 🔎 Troubleshooting & Solusi

| Masalah                      | Dampak/Penyebab                                      | Solusi                                                         |
|-----------------------------|-----------------------------------------------------|---------------------------------------------------------------|
| Inkonsistensi Nama Wilayah  | API kadang mengembalikan *Kab. Bekasi*, filter gagal| Filter fleksibel: substring `KAB` & `BEKASI`                  |
| Duplikasi saat Resume       | Buffer I/O, CSV belum terbaca langsung              | Pastikan os.fsync(csvfile.fileno()) setelah tulis baris (Sudah diimplementasikan di append_to_csv dalam dapodik_utils.py)     |
| API Tidak Stabil            | Timeout / HTTP 4xx / 5xx server Dapodik            | Terapkan *retry tak terbatas* + *backoff* (while True, dsb) dalam request_apidanrequest_html. |
