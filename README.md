# 📸 Locket Mini - Web Client

> Trải nghiệm Locket Widget ngay trên trình duyệt web! Một web client không chính thức (unofficial) cho Locket, được tối ưu hóa đặc biệt để chạy mượt mà trên các thiết bị cũ (như iPhone 6 / iOS 12) và máy tính.

![Python](https://img.shields.io/badge/Python-3.8+-blue?logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-2.x-black?logo=flask&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

---

## ✨ Tính năng nổi bật

- 🔐 **Đăng nhập an toàn:** Sử dụng Firebase Authentication chính chủ của Locket. Hỗ trợ "Ghi nhớ đăng nhập" trong 30 ngày.
- 📷 **Camera trực tiếp & Chụp ảnh 1:1:**
  - Tự động mở camera khi vào web (tùy chọn).
  - Khung nhìn camera vuông vắn (1:1), tự động cuộn đến vị trí camera để chụp tiện lợi.
  - Hỗ trợ đổi camera (trước/sau) và bật đèn flash.
  - Tối ưu hóa crop ảnh không bị méo trên iOS 12.
- 🖼️ **Tải ảnh & Xử lý ảnh:**
  - Crop ảnh trực tiếp trên web bằng Cropper.js.
  - Tự động nén ảnh và ép chuẩn vuông 1080x1080 bằng Pillow trước khi gửi.
  - Hỗ trợ dán ảnh trực tiếp (Ctrl+V) vào trang.
- 🔄 **Hàng đợi Offline (Offline Queue):** Lưu ảnh vào IndexedDB khi mất mạng và tự động đăng lại khi có kết nối.
- ⚡ **Moments Feed siêu tốc:**
  - Cache dữ liệu thông minh (Local Storage + Server Memory).
  - Cập nhật realtime qua WebSocket mà không cần F5.
  - Tải ảnh luồng (Concurrency Queue) chống giật/lag trên thiết bị yếu.
- 👥 **Bạn bè & Streak:** Xem danh sách bạn bè, streak hiện tại và avatar tương thích (tự convert sang JPEG cho iOS 12).

---

## 🛠️ Công nghệ sử dụng

- **Backend:** Python, Flask, Requests, WebSocket-Client, Pillow.
- **Frontend:** HTML5, CSS3, Vanilla JavaScript, Cropper.js, Bootstrap Icons.
- **API:** Firebase Auth, `locket.binhake.dev` (Action-API proxy).

---

## 🚀 Cài đặt và Chạy dự án

### 1. Yêu cầu hệ thống
- Python 3.8 trở lên.
- Pip.

### 2. Cài đặt thư viện
Tạo môi trường ảo (khuyến khích) và cài đặt các gói cần thiết:

```bash
python -m venv venv
source venv/bin/activate  # Trên Windows: venv\Scripts\activate

pip install flask requests pillow websocket-client
```

### 3. Chạy ứng dụng
Chạy file chính (ví dụ: `locket.py` hoặc `app.py`):

```bash
python locket.py
```

Server sẽ chạy ở cổng mặc định `5000`. Mở trình duyệt và truy cập:
👉 `http://127.0.0.1:5000` (hoặc IP máy chủ của bạn nếu deploy lên VPS).

---

## ⚙️ Cấu hình Environment (Tùy chọn)

Bạn có thể cấu hình bằng biến môi trường:

| Biến | Mặc định | Mô tả |
| :--- | :--- | :--- |
| `PORT` | `5000` | Cổng chạy server Flask |
| `SECRET_KEY` | Random | Khóa bảo mật session của Flask |

---

## 📱 Tối ưu cho thiết bị cũ (iPhone 6 / iOS 12)

Dự án này được tinh chỉnh đặc biệt để chạy trên các dòng máy cũ:
- Thay thế `aspect-ratio` CSS bằng `padding-bottom` trick.
- Xử lý mutation & intersection observer an toàn.
- Ép kiểu ảnh WebP từ server sang JPEG để trình duyệt Safari cũ có thể hiển thị.
- Giới hạn số lượng ảnh tải cùng lúc (Concurrency = 2) để tránh tràn RAM (Out of Memory).
- Cấm phóng to/thuắt trang (`user-scalable=no`).

---

## 🌍 Deploy lên Production (Heroku / Render / VPS)

Để deploy lên các nền tảng PaaS, hãy tạo file `requirements.txt`:

```txt
flask
requests
pillow
websocket-client
gunicorn
```

Và chạy bằng Gunicorn:
```bash
gunicorn locket:app --bind 0.0.0.0:$PORT
```

*(Lưu ý: Đổi tên hàm `app = Flask(__name__)` trong code thành `app = ...` để Gunicorn nhận diện).*

---

## 🤝 Đóng góp & Liên hệ

Dự án mang tính cá nhân và học tập. Nếu bạn có ý tưởng cải thiện, hãy tạo Pull Request!

- **Instagram:** [@anhztuan.1710](https://www.instagram.com/anhztuan.1710)
- **Threads:** [@anhztuan.1710](https://www.threads.net/@anhztuan.1710)

---

## ⚠️ Bản quyền & Miễn trừ trách nhiệm

Đây là dự án mã nguồn mở không chính thức (unofficial). Không liên kết với Locket, Inc. Mọi logo, thương hiệu và API thuộc về chủ sở hữu tương ứng. Vui lòng sử dụng với mục đích cá nhân và tôn trọng điều khoản dịch vụ của Locket.


### Hướng dẫn đẩy lên GitHub:
1. Tạo một repository mới trên GitHub.
2. Tại thư mục project của bạn trên máy, khởi tạo git:
   ```bash
   git init

3. Thêm tất cả file (đảm bảo bạn đã lưu file `README.md` ở thư mục gốc):
   ```bash
   git add .
   git commit -m "Initial commit: Locket Web Client"

4. Kết nối với GitHub và đẩy code lên:
   ```bash
   git remote add origin https://github.com/itskevinz/locket-mini.git
   git branch -M main
   git push -u origin main
