Đây là bản tài liệu Project Overview (Đặc tả dự án) chi tiết và chuyên nghiệp, được thiết kế để bạn gửi ngay cho nhóm 5 người. Nó phân chia rõ ràng trách nhiệm kỹ thuật và luồng đi của dữ liệu, đảm bảo mọi thành viên đều hiểu mình phải làm gì.

PROJECT CHARTER: CINEMA 360 - DATA INTELLIGENCE PLATFORM
1. Tổng quan dự án (Overview)
Tên dự án: Cinema 360 - Hệ thống phân tích dữ liệu điện ảnh đa nguồn.
Mục tiêu: Xây dựng một pipeline xử lý dữ liệu lớn (Big Data Pipeline) tự động hoá quy trình ETL (Extract - Transform - Load). Hệ thống sẽ thu thập dữ liệu phim từ nhiều định dạng khác nhau (File nén, API JSON, SQL Database), xử lý phân tán bằng công nghệ Big Data và hiển thị Dashboard phân tích kinh doanh.
Mô hình áp dụng: Tuân thủ mô hình Business Intelligence (BI) tiêu chuẩn: Nguồn -> Tích hợp -> Kho dữ liệu -> Trực quan hóa.
2. Kiến trúc hệ thống (System Architecture)
Chúng ta sẽ triển khai mô hình Modern Data Lakehouse thu nhỏ, chạy trên hạ tầng Docker.
Sơ đồ luồng dữ liệu (Data Flow):
Ingestion Layer: Python Scripts thu thập dữ liệu.
Storage Layer (Data Lake): Hadoop HDFS (Lưu trữ file thô).
Processing Layer: Apache Spark (Xử lý, làm sạch, join dữ liệu phân tán).
Serving Layer (Data Warehouse): PostgreSQL (Lưu trữ dữ liệu sạch có cấu trúc).
Visualization Layer: Streamlit + Plotly (Dashboard tương tác).

3. Chi tiết từng luồng xử lý (Detailed Workflow)
Dưới đây là đặc tả kỹ thuật cho từng giai đoạn để các thành viên nắm bắt:
Giai đoạn 1: Data Ingestion (Thu thập dữ liệu)
Nhiệm vụ: Đưa dữ liệu từ thế giới bên ngoài vào hệ thống lưu trữ thô (HDFS).
Luồng 1 - IMDb Dataset (Big Data Source):
Nguồn: File title.basics.tsv.gz và title.ratings.tsv.gz từ IMDb Interfaces.
Đặc điểm: File text nén, dung lượng lớn (triệu dòng).
Công việc: Viết Script Python download định kỳ, không giải nén (để tiết kiệm chỗ), upload thẳng lên HDFS folder /data/raw/imdb/.
Luồng 2 - TMDB API (Enrichment Source):
Nguồn: The Movie Database (TMDB) API.
Chiến lược: Sử dụng **TMDB Daily ID Export** để lấy danh sách ID hợp lệ trước (File JSON nén, cập nhật lúc 8AM UTC), sau đó mới gọi API lấy chi tiết để tối ưu hạn ngạch (Rate Limit).
Đặc điểm: Dữ liệu JSON (chứa Budget, Revenue, Poster).
Công việc: Viết Script Python tải file export, lọc danh sách ID, gọi API theo batch (xử lý Rate Limit 50 req/s), lưu file JSON vào HDFS folder /data/raw/tmdb/.
Luồng 3 - Internal Logs (Structured Source):
Nguồn: Một MySQL Container giả lập (chứa bảng user_reviews).
Công việc: Viết Script Python kết nối JDBC, query dữ liệu mới trong ngày, export ra CSV và đẩy lên HDFS folder /data/raw/internal/.
Giai đoạn 2: Data Processing (Xử lý trung tâm)
Nhiệm vụ: Biến dữ liệu thô thành thông tin sạch. Sử dụng PySpark.
Bước 1: Loading & Cleaning:
Dùng Spark đọc file TSV (IMDb) và JSON (TMDB) từ HDFS.
Xử lý Clean: Loại bỏ phim người lớn, phim thiếu năm phát hành, chuẩn hóa kiểu dữ liệu (String -> Int/Float).
Bước 2: Transformation & Integration (Khó nhất):
JOIN: Ghép bảng IMDb (Thông tin chung) với TMDB (Tài chính) và Internal Logs (Đánh giá nội bộ).
Logic:
Tính Profit = Revenue - Budget.
Tính ROI (Tỷ suất lợi nhuận).
Tính độ lệch điểm số: Rating_Diff = IMDb_Score - Internal_Score.
Bước 3: Writing:
Ghi kết quả cuối cùng xuống PostgreSQL qua JDBC Driver.
Giai đoạn 3: Data Visualization (Hiển thị)
Nhiệm vụ: Xây dựng Dashboard cho người dùng cuối (Business User).
Công cụ: Python Streamlit (Backend) + Plotly (Chart).
Yêu cầu hiển thị:
Biểu đồ Scatter: Tương quan giữa Ngân sách và Doanh thu.
Biểu đồ Line: Xu hướng thể loại phim theo năm.
Bảng xếp hạng: Top 10 phim lãi cao nhất nhưng bị đánh giá thấp (Overrated).

4. Phân chia công việc (Team Roles) - Cho nhóm 5 người
Để tối ưu hóa sức mạnh của từng thành viên, đề xuất phân chia như sau:
Vị trí
Nhiệm vụ chi tiết
Yêu cầu kỹ năng
1. DevOps & Architect

(Trưởng nhóm - Bạn)
- Dựng file docker-compose.yml (Hadoop, Spark, Postgres).

- Cấu hình giới hạn RAM/CPU chặt chẽ trong `docker-compose.yml` để đảm bảo tổng dung lượng không vượt quá 12GB (chừa 4GB cho OS).
  + Namenode: 1GB, Datanode: 1GB
  + Spark Master: 1GB, Spark Worker: 2-3GB
  + Postgres: 1GB

- Quản lý GitHub Repo và Merge code.
Docker, Linux, System Design.
2. Data Engineer 1

(Ingestion Specialist)
- Viết Python Script cào API TMDB và download IMDb.

- Đảm bảo file được đẩy lên đúng thư mục HDFS.

- Tạo data giả cho MySQL (Internal Logs).
Python (Requests/Pandas), SQL.
3. Data Engineer 2

(Spark Core)
- Viết code PySpark chính.

- Xử lý Logic JOIN các DataFrame (JSON + CSV).

- Tối ưu code để chạy được trên máy RAM 16GB.
Python, PySpark (Dataframe API).
4. Data Modeler

(Warehouse & Quality)
- Thiết kế Schema cho PostgreSQL (Bảng Fact/Dim).

- Kiểm tra chất lượng dữ liệu (Data Quality Check) sau khi Spark chạy xong.

- Viết document về ý nghĩa các trường dữ liệu.
SQL (Advanced), Database Design.
5. BI Developer

(Visualization)
- Code giao diện Streamlit.

- Kết nối tới Postgres để lấy data sạch.

- Vẽ biểu đồ Plotly đẹp, trực quan.
Python (Streamlit), Data Viz mindset.


5. Tech Stack & Môi trường phát triển
Hạ tầng: Docker & Docker Compose (Chạy trên máy Host: 6 Cores, 16GB RAM).
Ngôn ngữ lập trình: Python 3.9+.
Công nghệ lõi:
Storage: Hadoop HDFS (Namenode + Datanode).
Processing: Apache Spark (Master + Worker).
Warehouse: PostgreSQL 13.
Frontend: Streamlit.
IDE: VS Code (Khuyên dùng Extension Remote - Containers hoặc Dev Container).
Lưu ý quan trọng: Vì chạy giả lập Big Data trên máy cá nhân (1 Node), cần set `JAVA_OPTS` và `SPARK_WORKER_MEMORY` cẩn thận để tránh lỗi OutOfMemory hoặc treo máy host.

6. Lộ trình triển khai (Timeline dự kiến)
Tuần 1: Setup & Ingestion
Chốt Docker Compose chạy ổn định.
Viết xong script lấy dữ liệu từ IMDb và API TMDB.
Tuần 2: Processing (Spark)
Data Engineer viết code PySpark để đọc file từ HDFS.
Thực hiện Join dữ liệu và xử lý logic nghiệp vụ.
Tuần 3: Warehousing & Viz
Đẩy data vào Postgres.
BI Developer dựng biểu đồ lên Streamlit.
Tuần 4: Testing & Document
Chạy thử toàn bộ luồng (End-to-End Test).
Viết báo cáo và Slide thuyết trình.

Project này tuy chạy Local nhưng sử dụng kiến trúc Big Data chuẩn công nghiệp (Hadoop/Spark). Đây là điểm cộng rất lớn so với các nhóm khác chỉ dùng thư viện Pandas thông thường. Yêu cầu mọi người tuân thủ đúng kiến trúc Docker đã đề ra để tránh lỗi môi trường ("Code chạy máy tôi nhưng không chạy máy bạn").


