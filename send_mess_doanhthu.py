import json
import pandas as pd
from datetime import datetime, timedelta
import requests
import time
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
from matplotlib import font_manager
import base64
from io import BytesIO

# ==================== CẤU HÌNH ====================
app_id = "cli_a8620f964a38d02f"
app_secret = "G3FdlSvmTAXZYX8SBZtfpckHUiWUCO4h"
app_token = "AVY3bPgpja7Xwks2ht6lNGsnglc"
table_id = "tblwHEox2atpjNkp"

# DANH SÁCH CÁC WEBHOOK - GỬI VÀO 2 GROUPS
webhook_urls = [
    "https://open.larksuite.com/open-apis/bot/v2/hook/ec2a7b8c-197a-42a9-8125-870d7f602ccb",
    "https://open.larksuite.com/open-apis/bot/v2/hook/bf24d3f9-68f6-4fd3-9b0f-35e75c0b6c87"
]

# LẤY THÁNG HIỆN TẠI VÀ NGÀY HÔM QUA
now = datetime.now()
yesterday = now - timedelta(days=1)
CURRENT_MONTH = now.month
CURRENT_YEAR = now.year
YESTERDAY_DAY = now.day

class LarkBaseAPI:
    """Class để làm việc với Lark Base API"""
    
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = None
        self.token_expire = 0
    
    def get_access_token(self):
        """Lấy access token từ Lark API"""
        if self.access_token and time.time() < self.token_expire:
            return self.access_token
        
        url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json"}
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        response = requests.post(url, headers=headers, json=data)
        result = response.json()
        
        if result.get('code') == 0:
            self.access_token = result['tenant_access_token']
            self.token_expire = time.time() + result['expire'] - 60
            return self.access_token
        else:
            raise Exception(f"Lỗi lấy access token: {result}")
    
    def get_records(self, app_token, table_id, page_size=500):
        """Lấy tất cả records từ Lark Base"""
        token = self.get_access_token()
        url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        all_records = []
        page_token = None
        
        while True:
            params = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            
            response = requests.get(url, headers=headers, params=params)
            result = response.json()
            
            if result.get('code') == 0:
                data = result.get('data', {})
                records = data.get('items', [])
                all_records.extend(records)
                
                page_token = data.get('page_token')
                if not data.get('has_more'):
                    break
            else:
                raise Exception(f"Lỗi lấy dữ liệu: {result}")
        
        return all_records

def parse_lark_records(records, month=None, year=None):
    """Parse records từ Lark API và lọc theo tháng"""
    if month is None:
        month = CURRENT_MONTH
    if year is None:
        year = CURRENT_YEAR
    
    parsed_records = []
    
    for record in records:
        fields = record.get('fields', {})
        
        # Lấy tháng và năm
        thang = fields.get('Tháng')
        nam = fields.get('Năm')
        
        if thang != month or nam != year:
            continue
        
        # Chuyển đổi timestamp
        timestamp = fields.get('Thời gian phát live')
        if timestamp:
            date = datetime.fromtimestamp(timestamp / 1000)
            ngay = date.strftime('%d/%m')
        else:
            continue
        
        # Parse tên nhân viên
        ten_nv = fields.get('Tên nhân viên live', [])
        if isinstance(ten_nv, list) and len(ten_nv) > 0:
            ten_nv = ten_nv[0].get('text', '')
        elif isinstance(ten_nv, str):
            ten_nv = ten_nv
        else:
            ten_nv = ''
        
        # Tạo record dict
        record_dict = {
            'Ngày': ngay,
            'Tên nhân viên live': ten_nv,
            'Duration (p)': float(fields.get('Duration (p)', 0) or 0),
            'Doanh thu thực tế': float(fields.get('Doanh thu thực tế', 0) or 0),
            'GMV': float(fields.get('GMV', 0) or 0),
            'GMV trực tiếp': float(fields.get('GMV trực tiếp', 0) or 0),
            'Số món bán ra': int(fields.get('Số món bán ra', 0) or 0),
            'Lượt xem': int(fields.get('Lượt xem', 0) or 0),
            'CTR (%)': float(fields.get('CTR (%)', 0) or 0),
            'CTOR (%)': float(fields.get('CTOR (%)', 0) or 0),
        }
        parsed_records.append(record_dict)
    
    return pd.DataFrame(parsed_records)

def create_summary_table(df):
    """Tạo bảng tổng hợp theo ngày, pivot tên nhân viên thành cột"""
    if df.empty:
        return pd.DataFrame()
    
    # Tổng hợp theo ngày và nhân viên (dùng Doanh thu thực tế)
    summary = df.groupby(['Ngày', 'Tên nhân viên live']).agg({
        'Doanh thu thực tế': 'sum'
    }).reset_index()
    
    # Pivot: Ngày thành index, Tên nhân viên thành cột
    pivot_table = summary.pivot(
        index='Ngày',
        columns='Tên nhân viên live',
        values='Doanh thu thực tế'
    ).fillna(0)
    
    # Thêm cột tổng (tổng theo hàng ngang)
    pivot_table['Tổng'] = pivot_table.sum(axis=1)
    
    # Sắp xếp theo ngày
    try:
        pivot_table['Ngày_sort'] = pd.to_datetime(f'{CURRENT_YEAR}/' + pivot_table.index, format='%Y/%d/%m', errors='coerce')
        pivot_table = pivot_table.sort_values('Ngày_sort').drop('Ngày_sort', axis=1)
    except:
        pass
    
    # Reset index để Ngày thành cột
    pivot_table = pivot_table.reset_index()
    
    # Thêm hàng tổng ở cuối (tổng theo cột dọc)
    total_row = {}
    total_row['Ngày'] = 'TỔNG'
    for col in pivot_table.columns:
        if col != 'Ngày':
            total_row[col] = pivot_table[col].sum()
    
    # Append hàng tổng
    pivot_table = pd.concat([pivot_table, pd.DataFrame([total_row])], ignore_index=True)
    
    return pivot_table

def create_table_image(df):
    """Tạo ảnh bảng từ DataFrame - HÀNG THẤP, CỘT RỘNG"""
    
    # Tạo tiêu đề động - ĐẾN NGÀY HÔM QUA
    title = f'DOANH THU THỰC TẾ TỪ 1/{CURRENT_MONTH} - {YESTERDAY_DAY}/{CURRENT_MONTH}'
    
    # KÍCH THƯỚC - RỘNG HƠN ĐỂ TÊN CỘT KHÔNG XUỐNG DÒNG
    num_cols = len(df.columns)
    num_rows = len(df)
    fig_width = 14  # Tăng từ 10 lên 14
    fig_height = 2 + num_rows * 0.25  # Giảm chiều cao hàng
    
    # Tạo figure
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    ax.axis('tight')
    ax.axis('off')
    
    # Tạo title
    fig.text(0.5, 0.96, title, 
             ha='center', va='top', 
             fontsize=12, fontweight='bold',
             color='#1f4788')
    
    # Format dữ liệu để hiển thị
    df_display = df.copy()
    
    
    # Format các cột số (trừ cột Ngày)
    for col in df_display.columns:
        if col != 'Ngày':
            df_display[col] = df_display[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) and x != 0 else "-")
    
    # Tạo bảng
    table = ax.table(cellText=df_display.values,
                     colLabels=df_display.columns,
                     cellLoc='center',
                     loc='center',
                     bbox=[0, 0, 1, 0.92])
    
    # Style cho bảng - HÀNG THẤP
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.5)  # Giảm từ 1.8 xuống 1.5
    
    # Style cho header
    for i in range(len(df_display.columns)):
        cell = table[(0, i)]
        cell.set_facecolor('#1f4788')
        cell.set_text_props(weight='bold', color='white', fontsize=9)
        cell.set_height(0.08)  # Chiều cao header
    
    # Style cho các row
    for i in range(1, len(df_display) + 1):
        is_total_row = (i == len(df_display))
        
        for j in range(len(df_display.columns)):
            cell = table[(i, j)]
            cell.set_height(0.06)  # Chiều cao cell thấp hơn
            
            if is_total_row:
                cell.set_facecolor('#d4edda')
                cell.set_text_props(weight='bold', color='#155724', fontsize=9)
            else:
                if i % 2 == 0:
                    cell.set_facecolor('#f0f0f0')
                else:
                    cell.set_facecolor('white')
            
            if j == len(df_display.columns) - 1:
                if is_total_row:
                    cell.set_facecolor('#28a745')
                    cell.set_text_props(weight='bold', color='white', fontsize=9)
                else:
                    cell.set_text_props(weight='bold', color='#d9534f', fontsize=8)
                    cell.set_facecolor('#fff3cd')
            
            # Border
            cell.set_edgecolor('#34495e')
            cell.set_linewidth(1)
    
    plt.tight_layout()
    
    # Lưu ra file
    output_file = 'lark_summary_table.png'
    plt.savefig(output_file, dpi=150, bbox_inches='tight', facecolor='white', pad_inches=0.05)
    plt.close()
    
    return output_file

def upload_image_to_lark(image_path):
    """Upload ảnh lên Lark và lấy image_key"""
    api = LarkBaseAPI(app_id, app_secret)
    token = api.get_access_token()
    
    url = "https://open.larksuite.com/open-apis/im/v1/images"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    with open(image_path, 'rb') as f:
        files = {
            'image': f
        }
        data = {
            'image_type': 'message'
        }
        
        response = requests.post(url, headers=headers, files=files, data=data)
        result = response.json()
        
        if result.get('code') == 0:
            return result['data']['image_key']
        else:
            raise Exception(f"Lỗi upload ảnh: {result}")

def send_image_to_webhooks(image_key, webhook_urls):
    """Gửi ảnh vào NHIỀU group chats qua webhooks"""
    
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "template": "blue",
                "title": {"tag": "plain_text", "content": f"📊 Doanh thu Thực tế từ 1/{CURRENT_MONTH} - {YESTERDAY_DAY}/{CURRENT_MONTH}"}
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**Tổng hợp doanh thu thực tế theo ngày và nhân viên live - Tháng {CURRENT_MONTH}/{CURRENT_YEAR}**"
                    }
                },
                {
                    "tag": "img",
                    "img_key": image_key,
                    "mode": "fit_horizontal"
                },
                {
                    "tag": "hr"
                },
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {
                                "tag": "plain_text",
                                "content": "📋 Xem chi tiết trong Lark Base"
                            },
                            "type": "primary",
                            "url": "https://atino-vietnam.sg.larksuite.com/base/AVY3bPgpja7Xwks2ht6lNGsnglc?table=tblwHEox2atpjNkp&view=vew0Cl5yD7"
                        }
                    ]
                }
            ]
        }
    }
    
    # Gửi vào từng webhook
    success_count = 0
    for idx, webhook_url in enumerate(webhook_urls, 1):
        try:
            response = requests.post(webhook_url, json=payload)
            result = response.json()
            
            if result.get('StatusCode') == 0 or result.get('code') == 0:
                print(f"   ✓ Webhook {idx}: Gửi thành công!")
                success_count += 1
            else:
                print(f"   ✗ Webhook {idx}: Lỗi - {result}")
        except Exception as e:
            print(f"   ✗ Webhook {idx}: Exception - {e}")
    
    print(f"\n   → Đã gửi thành công vào {success_count}/{len(webhook_urls)} group chats")

def main():
    print("="*60)
    print(f"BÁO CÁO LIVESTREAM THÁNG {CURRENT_MONTH}/{CURRENT_YEAR}")
    print(f"Từ 1/{CURRENT_MONTH} đến {YESTERDAY_DAY}/{CURRENT_MONTH}")
    print("="*60)
    
    # Lấy dữ liệu từ Lark API
    print("\n1. Đang kết nối với Lark API...")
    api = LarkBaseAPI(app_id, app_secret)
    
    print(f"2. Đang lấy dữ liệu tháng {CURRENT_MONTH}/{CURRENT_YEAR}...")
    records = api.get_records(app_token, table_id)
    print(f"   Đã lấy {len(records)} records")
    
    # Parse dữ liệu
    print("3. Đang xử lý dữ liệu...")
    df = parse_lark_records(records, month=CURRENT_MONTH, year=CURRENT_YEAR)
    print(f"   Tìm thấy {len(df)} records tháng {CURRENT_MONTH}/{CURRENT_YEAR}")
    
    # Tạo bảng tổng hợp
    print("4. Đang tạo bảng tổng hợp...")
    summary = create_summary_table(df)
    print(f"   Tổng hợp: {len(summary)} dòng")
    print("\n" + "="*60)
    print(summary.to_string(index=False))
    print("="*60)
    
    # Tạo ảnh
    print("\n5. Đang tạo ảnh bảng...")
    image_path = create_table_image(summary)
    print(f"   ✓ Đã tạo ảnh: {image_path}")
    
    # Upload ảnh
    print("6. Đang upload ảnh lên Lark...")
    image_key = upload_image_to_lark(image_path)
    print(f"   ✓ Image key: {image_key}")
    
    # Gửi webhook
    print(f"7. Đang gửi vào {len(webhook_urls)} group chats...")
    send_image_to_webhooks(image_key, webhook_urls)
    
    print("\n" + "="*60)
    print("✓ HOÀN THÀNH!")
    print("="*60)

if __name__ == "__main__":
    main()
