### PANDUAN KOLABORASI PROYEK SCALABLE ARTICLESWAP

#### Pendahuluan

Dokumen ini berisi panduan teknis bagi seluruh anggota tim dalam mengembangkan proyek Scalable Articleswap. Infrastruktur sistem yang digunakan meliputi Nginx API Gateway dengan fungsi load balancer dan circuit breaker, aplikasi Go, RabbitMQ sebagai message broker, serta PostgreSQL dengan PgBouncer untuk connection pooling.

Setiap anggota tim diharapkan mengikuti prosedur yang tertera untuk memastikan keseragaman lingkungan pengembangan dan kelancaran integrasi.

#### Prosedur Setup Lokal

Setiap anggota tim wajib melakukan inisialisasi lingkungan pengembangan pada perangkat masing-masing dengan langkah-langkah berikut.

1.  **Kloning Repositori**
    Lakukan kloning repositori utama dari GitHub.

    ```bash
    git clone git@github.com:ravifposeur/scalable-articleswap.git
    cd scalable-articleswap
    ```

2.  **Konfigurasi Variabel Lingkungan**
    Buat file `.env` dengan menyalin berkas `.env.example` yang telah tersedia di direktori proyek.

    ```bash
    cp .env.example .env
    ```

    Pastikan file `.env` yang baru dibuat telah terisi dengan kredensial bawaan. File ini telah masuk dalam `.gitignore` sehingga tidak akan terunggah ke repositori.

3.  **Menjalankan Infrastruktur**
    Pastikan Docker Engine berjalan, kemudian jalankan semua layanan menggunakan perintah berikut.

    ```bash
    docker compose up -d --build
    ```

4.  **Verifikasi Kesehatan Sistem**
    Lakukan pengecekan untuk memastikan seluruh infrastruktur beroperasi dengan benar dengan mengakses URL berikut melalui peramban atau `curl`.

    ```
    http://localhost:8080/health
    ```

    Respon `{"status": "Gateway is healthy"}` menandakan sistem siap digunakan.

#### Pembagian Tugas dan Spesifikasi Teknis

**1. Core Backend Engineer (Go Developer)**

-   **Lokasi Kode:** `service-article/main.go`
-   **Tugas:** Mengganti kode dummy dengan logika API Gateway yang sebenarnya.
-   **Fitur yang Dikembangkan:**
    -   Rute `/api/submit` (POST): Menerima JSON artikel, menyimpan data ke PostgreSQL dengan status *pending*, dan mengirimkan ID artikel beserta teks ke antrean RabbitMQ.
    -   Rute `/api/forward` (GET): Mengambil hasil akhir teks yang telah diproses oleh *worker* dari PostgreSQL.
-   **Catatan Penting:**
    -   Gunakan `os.Getenv()` untuk mengambil kredensial dari variabel lingkungan, bukan *hardcode*.
    -   Aplikasi Go harus berjalan pada port `8080` karena Nginx dikonfigurasi untuk mengarahkan lalu lintas ke port tersebut.

**2. NLP Worker Engineer (Python Developer - Stemming)**

-   **Lokasi Kode:** `worker-stemming/worker_stemming.py`
-   **Tugas:** Membuat skrip Python sebagai konsumen antrean RabbitMQ untuk melakukan proses stemming teks menggunakan library Sastrawi.
-   **Alur Kerja:**
    1.  Menghubungkan ke RabbitMQ.
    2.  Mengambil pesan teks dari antrean.
    3.  Memproses teks untuk menghilangkan imbuhan.
    4.  Memperbarui kolom hasil di PostgreSQL.
-   **Catatan Penting:**
    -   Gunakan nama layanan internal `pgbouncer` pada port `5432` untuk koneksi database.
    -   Gunakan nama layanan `rabbitmq` pada port `5672` untuk koneksi ke broker pesan.

**3. Visual Worker Engineer (Python Developer - Word Cloud)**

-   **Lokasi Kode:** `worker-wordcloud/worker_wordcloud.py`
-   **Tugas:** Membuat skrip Python sebagai konsumen yang menghasilkan visualisasi kata dari teks yang diterima.
-   **Alur Kerja:**
    1.  Menghubungkan ke RabbitMQ (menggunakan *exchange* fanout untuk menerima pesan yang sama dengan worker lainnya).
    2.  Menghasilkan gambar *word cloud* menggunakan library WordCloud dan Matplotlib.
    3.  Menyimpan gambar ke direktori yang telah di-*mount* sebagai volume Docker.
    4.  Mencatat URL atau jalur gambar ke database PostgreSQL.
-   **Catatan Penting:**
    -   Simpan gambar pada direktori volume Docker agar data tidak hilang saat kontainer dihentikan.
    -   Direktori hasil gambar telah diatur dalam `.gitignore` sehingga tidak mengganggu repositori.

**4. QA, Stress Tester, and Tech Writer**

-   **Lokasi Kerja:** Lingkungan eksternal dan aplikasi pengolah dokumen.
-   **Tugas:** Menguji ketahanan sistem dan menyusun laporan pengujian.
-   **Alur Kerja:**
    1.  Menulis skrip pengujian beban menggunakan k6 atau Apache JMeter.
    2.  Mengirimkan permintaan ke rute `/api/submit` dengan skenario beban tinggi (contoh: 100 hingga 500 pengguna bersamaan per detik).
    3.  Memantau stabilitas sistem melalui log Nginx, terutama distribusi lalu lintas ke `article_app_1` dan `article_app_2`.
    4.  Mendokumentasikan metrik kinerja (RPS, tingkat kesalahan, waktu respons) dalam laporan akhir maksimal dua halaman.
-   **Catatan Penting:**
    -   Lakukan pengujian *circuit breaker* dengan menghentikan salah satu instance Go (`docker stop article_app_1`) saat pengujian beban. Dokumentasikan bagaimana Nginx mengalihkan lalu lintas ke instance lainnya tanpa menyebabkan sistem gagal total.

#### Alur Kerja Git

Seluruh anggota tim diwajibkan mengikuti alur kerja Git berikut.

-   Buat *branch* baru untuk setiap pengembangan fitur.
    ```bash
    git checkout -b feat/nama-fitur
    ```
-   Lakukan *commit* dan *push* hanya setelah fitur berhasil diuji di lingkungan lokal.
-   Ajukan *Pull Request* (PR) untuk proses *review* dan penggabungan ke *branch* `main`.
