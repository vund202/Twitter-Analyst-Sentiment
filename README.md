# Twitter-Analyst-Sentiment
Tiểu luận chuyên ngành KTDL  
# Bước 1: Sử dụng lệnh gitclone “https://github.com/vund202/Twitter-Analyst-Sentiment.git ”
- Chạy lệnh “Docker-compose up -d” cho file docker-compose.yml  
- Tại giao diện của Docker ta nhấn nút màu xanh để chạy các container đã cài đặt  
# Bước 2:Thiết lập workflow Nifi
- Ta nhập localhost:2080 để truy cập Apache Nifi sau đó chọn nút Up Load Template
- Chọn file newest.xml  đây là file đã tạo và setting workflow  
- Thiết lập workflow trên Nifi  
- Chọn nút Template trên đầu giao diện và chọn template vừa upload  
# Bước 3:Truy cập Bash trong Kafka
- Mở cmd trên windows nhập “docker ps” xem tên của container sau đó nhập lệnh: “docker exec -i -t customdockerfile-kafka-1 bash” để truy cập kafka  
# Bước 4:Khởi tạo topic và chạy Consumer
- Ta nhập lệnh “kafka-topics.sh --create --topic twitter_demo --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181” để tạo topic 
- Khởi chạy consumer ta nhập lệnh “kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter_demo --from-beginning --max-messages 100”
# Bước 5: Thu nhập dữ liệu và khởi tạo Producer
- Thiết lập Kafka Brokers dựa theo địa chỉ IP của máy tính và topic name tương ứng với tên topic tạo trên Kafka trước đó
- Ta khởi động tất cả các Processors trên Nifi bằng button này bên trái thanh giao diện
- Tại đây dữ liệu sẽ được thu nhập và streaming lên Kafka
# Bước 6: Tạo mô hình LTSM và Logicstic Regression
- Chạy localhost:4888 truy cập Jupyter Notebook ta chạy file LTSM và Logicstic Regression để huấn luyện 2 mô hình.
- Đọc ghi dữ liệu từ Kafka và biến đổi dữ liệu sang tệp JSON 
- Ta chạy file “schemagenerator.ipynb” để ghi dữ liệu vào thư mục schema/in từ đây dữ liệu sẽ được xử lý và chuyển sang dạng tệp JSON ở thư mục schema/out
- Dự đoán dữ liệu và lưu trữ vào MongoDB
- Ta chạy file “streamlistener.ipynb” để đọc tệp JSON đồng thời tiền xử lý dữ liệu lấy các cột cần thiết sau đó ta sẽ load mô hình và ghi kết quả vào MongoDB kết quả ta sẽ thu được tại localhost:4141 truy cập vào database và collection
# Bước 7: Trực quan hoá dữ liệu và phân tích 
- Ta sử dụng file “streamvisualize.ipynb” để trực quan hoá dữ liệu và phân tích dữ liệu từ MongoDB.
