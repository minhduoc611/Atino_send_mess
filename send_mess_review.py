import requests
import pandas as pd
from datetime import datetime
import time
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

# ==================== CẤU HÌNH ====================
app_id = "cli_a8620f964a38d02f"
app_secret = "G3FdlSvmTAXZYX8SBZtfpckHUiWUCO4h"
app_token = "AVY3bPgpja7Xwks2ht6lNGsnglc"
table_id = "tblNupUZa8oe9WZm"

# DANH SÁCH CÁC WEBHOOK - GỬI VÀO 2 GROUPS
webhook_urls = [
    "https://open.larksuite.com/open-apis/bot/v2/hook/ec2a7b8c-197a-42a9-8125-870d7f602ccb",
    "https://open.larksuite.com/open-apis/bot/v2/hook/bf24d3f9-68f6-4fd3-9b0f-35e75c0b6c87"
]

THRESHOLD = 0.4 

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

def extract_field_value(field_value):
    """Trích xuất giá trị từ Lark field format"""
    if isinstance(field_value, list) and len(field_value) > 0:
        first_item = field_value[0]
        if isinstance(first_item, dict) and 'text' in first_item:
            return first_item['text']
        return first_item
    
    if isinstance(field_value, dict) and 'text' in field_value:
        return field_value['text']
    
    return field_value if field_value is not None else ""

def parse_lark_records(records):
    """Parse records từ Lark API"""
    parsed_records = []
    
    for record in records:
        fields = record.get('fields', {})
        
        # Lấy giá trị Tỉ lệ và convert sang float
        ti_le = fields.get('Tỉ lệ', 0)
        if isinstance(ti_le, str):
            # Nếu là string, loại bỏ % và convert
            ti_le = ti_le.replace('%', '').replace(',', '.').strip()
            try:
                ti_le = float(ti_le)
            except:
                ti_le = 0.0
        elif ti_le is None:
            ti_le = 0.0
        else:
            ti_le = float(ti_le)
        
        # Lấy giá trị Số lượng review thấp và convert sang int
        so_luong = fields.get('Số lượng review thấp', 0)
        try:
            so_luong = int(so_luong) if so_luong else 0
        except:
            so_luong = 0
        
        record_dict = {
            'ID sản phẩm': extract_field_value(fields.get('ID sản phẩm', '')),
            'Tên sản phẩm': extract_field_value(fields.get('Tên sản phẩm', '')),
            'Số lượng review thấp': so_luong,
            'Tỉ lệ': ti_le
        }
        
        parsed_records.append(record_dict)
    
    df = pd.DataFrame(parsed_records)
    
    # Debug: In ra kiểu dữ liệu
    print(f"   📊 Kiểu dữ liệu:")
    print(f"      - Số lượng review thấp: {df['Số lượng review thấp'].dtype}")
    print(f"      - Tỉ lệ: {df['Tỉ lệ'].dtype}")
    
    return df

def filter_high_rate_products(df, threshold=0.4):
    """Lọc sản phẩm có tỉ lệ > threshold"""
    filtered_df = df[df['Tỉ lệ'] > threshold].copy()
    filtered_df = filtered_df.sort_values('Tỉ lệ', ascending=False)
    filtered_df = filtered_df.reset_index(drop=True)
    return filtered_df

def truncate_text(text, max_length=None):
    """Không cắt ngắn text nữa - hiển thị đầy đủ"""
    if pd.isna(text) or text == '':
        return ''
    return str(text)

def create_table_image(df, threshold):
    """Tạo ảnh bảng từ DataFrame"""
    
    if df.empty:
        # Tạo bảng trống với thông báo
        fig, ax = plt.subplots(figsize=(12, 3))
        ax.axis('off')
        ax.text(0.5, 0.5, f'Không có sản phẩm nào có tỉ lệ review tiêu cực > {threshold}',
                ha='center', va='center', fontsize=14, color='#666')
        output_file = 'negative_reviews_table.png'
        plt.savefig(output_file, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        return output_file
    
    # Tạo tiêu đề
    title = f'SẢN PHẨM CÓ TỈ LẾ REVIEW TIÊU CỰC > {threshold} ({len(df)} sản phẩm)'
    
    # Tạo DataFrame hiển thị
    df_display = df.copy()
    
    # Thêm cột STT
    df_display.insert(0, 'STT', range(1, len(df_display) + 1))
    
    # KHÔNG cắt ngắn tên sản phẩm - hiển thị đầy đủ
    df_display['Tên sản phẩm'] = df_display['Tên sản phẩm'].apply(lambda x: truncate_text(x))
    
    # Format các cột số
    df_display['Số lượng review thấp'] = df_display['Số lượng review thấp'].apply(
        lambda x: f"{int(x)}" if pd.notna(x) else "-"
    )
    df_display['Tỉ lệ'] = df_display['Tỉ lệ'].apply(
        lambda x: f"{x:.2f}%" if pd.notna(x) else "-"
    )
    
    # KÍCH THƯỚC - TĂNG CHIỀU RỘNG
    num_rows = len(df_display)
    fig_height = 2.5 + num_rows * 0.45  # Tăng chiều cao mỗi hàng
    fig_width = 20  # Tăng chiều rộng
    
    # Tạo figure
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    ax.axis('tight')
    ax.axis('off')
    
    # Tạo title
    fig.text(0.5, 0.97, title, 
             ha='center', va='top', 
             fontsize=13, fontweight='bold',
             color='#e74c3c')
    
    # Tạo bảng
    table = ax.table(cellText=df_display.values,
                     colLabels=df_display.columns,
                     cellLoc='left',
                     loc='center',
                     bbox=[0, 0, 1, 0.93])
    
    # Style cho bảng
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    
    # ĐẶT ĐỘ RỘNG CỘT RIÊNG BIỆT
    # Cột 0: STT - 0.05 (rất nhỏ)
    # Cột 1: ID sản phẩm - 0.15 (thu nhỏ)
    # Cột 2: Tên sản phẩm - 0.60 (MỞ RỘNG)
    # Cột 3: Số lượng - 0.10 (thu nhỏ)
    # Cột 4: Tỉ lệ - 0.10 (thu nhỏ)
    col_widths = [0.05, 0.15, 0.60, 0.10, 0.10]
    
    for i in range(len(df_display.columns)):
        for j in range(len(df_display) + 1):  # +1 cho header
            cell = table[(j, i)]
            cell.set_width(col_widths[i])
    
    # Style cho header
    for i in range(len(df_display.columns)):
        cell = table[(0, i)]
        cell.set_facecolor('#e74c3c')
        cell.set_text_props(weight='bold', color='white', fontsize=9)
        cell.set_height(0.08)
        
        # Căn giữa cho STT và các cột số
        if i == 0 or i == 1 or i == 3 or i == 4:  # STT, ID, Số lượng, Tỉ lệ
            cell.set_text_props(ha='center')
    
    # Style cho các row
    for i in range(1, len(df_display) + 1):
        for j in range(len(df_display.columns)):
            cell = table[(i, j)]
            cell.set_height(0.09)  # Tăng chiều cao để text không bị dồn
            
            # Màu nền xen kẽ
            if i % 2 == 0:
                cell.set_facecolor('#f9f9f9')
            else:
                cell.set_facecolor('white')
            
            # Highlight cột Tỉ lệ
            if j == len(df_display.columns) - 1:  # Cột Tỉ lệ
                cell.set_facecolor('#fff3cd')
                cell.set_text_props(weight='bold', color='#e74c3c', ha='center')
            
            # Căn giữa cho cột số
            if j == 0 or j == 1 or j == 3 or j == 4:  # STT, ID, Số lượng, Tỉ lệ
                cell.set_text_props(ha='center')
            
            # Cột tên sản phẩm: wrap text và căn trái
            if j == 2:
                cell.set_text_props(ha='left', wrap=True, fontsize=7.5)
            
            # Border
            cell.set_edgecolor('#34495e')
            cell.set_linewidth(0.8)
    
    plt.tight_layout()
    
    # Lưu ra file
    output_file = 'negative_reviews_table.png'
    plt.savefig(output_file, dpi=150, bbox_inches='tight', facecolor='white', pad_inches=0.1)
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

def send_image_to_webhooks(image_key, webhook_urls, df, threshold):
    """Gửi ảnh vào NHIỀU group chats qua webhooks"""
    
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "template": "red",
                "title": {
                    "tag": "plain_text", 
                    "content": f"7. Báo cáo Review Tiêu cực sản phẩm TikTok Shop"
                }
            },
            "elements": [
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
                                "content": "Xem chi tiết"
                            },
                            "type": "primary",
                            "url": "https://atino-vietnam.sg.larksuite.com/base/AVY3bPgpja7Xwks2ht6lNGsnglc?table=tblNupUZa8oe9WZm&view=vewhdbQrVp"
                        }
                    ]
                },
                {
                    "tag": "hr"
                },
                {
                    "tag": "note",
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
    print("="*70)
    print("BÁO CÁO REVIEW TIÊU CỰC TIKTOK SHOP")
    print("="*70)
    
    # Lấy dữ liệu từ Lark API
    print("\n1. Đang kết nối với Lark API...")
    api = LarkBaseAPI(app_id, app_secret)
    
    print("2. Đang lấy dữ liệu negative reviews...")
    records = api.get_records(app_token, table_id)
    print(f"   ✓ Đã lấy {len(records)} records")
    
    # Parse dữ liệu
    print("3. Đang xử lý dữ liệu...")
    df = parse_lark_records(records)
    print(f"   ✓ Đã parse {len(df)} sản phẩm")
    
    # Lọc sản phẩm có tỉ lệ cao
    print(f"4. Đang lọc sản phẩm có tỉ lệ > {THRESHOLD}...")
    filtered_df = filter_high_rate_products(df, threshold=THRESHOLD)
    print(f"   ✓ Tìm thấy {len(filtered_df)} sản phẩm")
    
    if not filtered_df.empty:
        print("\n" + "="*70)
        print(f"TOP 5 SẢN PHẨM CÓ TỈ LỆ CAO NHẤT:")
        for idx, row in filtered_df.head(5).iterrows():
            print(f"  {idx+1}. {row['Tên sản phẩm'][:50]}...")
            print(f"     Tỉ lệ: {row['Tỉ lệ']:.2f}% | Reviews: {row['Số lượng review thấp']}")
        print("="*70)
    
    # Tạo ảnh
    print("\n5. Đang tạo ảnh bảng...")
    image_path = create_table_image(filtered_df, THRESHOLD)
    print(f"   ✓ Đã tạo ảnh: {image_path}")
    
    # Upload ảnh
    print("6. Đang upload ảnh lên Lark...")
    image_key = upload_image_to_lark(image_path)
    print(f"   ✓ Image key: {image_key}")
    
    # Gửi webhook
    print(f"7. Đang gửi vào {len(webhook_urls)} group chats...")
    send_image_to_webhooks(image_key, webhook_urls, filtered_df, THRESHOLD)
    
    print("\n" + "="*70)
    print("✓ HOÀN THÀNH!")
    print("="*70)

if __name__ == "__main__":
    main()
