import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import imgkit

# ==================== CẤU HÌNH ====================
app_id = "cli_a8620f964a38d02f"
app_secret = "G3FdlSvmTAXZYX8SBZtfpckHUiWUCO4h"
app_token = "AVY3bPgpja7Xwks2ht6lNGsnglc"
table_id = "tblwHEox2atpjNkp"
webhook_url = "https://open.larksuite.com/open-apis/bot/v2/hook/175214ad-f698-45a6-89d3-45ff7453429d"

# Tháng hiện tại
now = datetime.now()
TARGET_MONTH = now.month
TARGET_YEAR = now.year
CURRENT_DAY = now.day - 1

# Bảng màu pastel nhẹ nhàng
COLORS = [
    '#A8DADC', '#E5989B', '#B8C5D6', '#C9ADA7', '#D4E09B',
    '#B5D99C', '#D4A5A5', '#9BABBF', '#C5B9AC', '#A8C5DD',
    '#E5C1C5', '#B2C9AB', '#D4B5C1', '#A5BFD4', '#C9D4A5'
]

# ==================== LẤY DỮ LIỆU ====================
def get_tenant_access_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    response = requests.post(url, json={"app_id": app_id, "app_secret": app_secret})
    result = response.json()
    return result.get("tenant_access_token") if result.get("code") == 0 else None

def get_all_records():
    print("Đang lấy dữ liệu từ Lark Base...")
    token = get_tenant_access_token()
    if not token:
        return None
    
    all_records = []
    page_token = None
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    
    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        
        response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params)
        result = response.json()
        
        if result.get("code") != 0:
            break
        
        data = result.get("data", {})
        all_records.extend(data.get("items", []))
        
        if not data.get("has_more"):
            break
        page_token = data.get("page_token")
    
    print(f"Đã lấy {len(all_records)} records")
    return all_records

# ==================== XỬ LÝ DỮ LIỆU ====================
def extract_text(value):
    if isinstance(value, list) and len(value) > 0:
        if isinstance(value[0], dict) and 'text' in value[0]:
            return value[0]['text']
    return value

def get_short_name(full_name):
    if not full_name or str(full_name).strip() == "":
        return ""
    parts = str(full_name).strip().split()
    if len(parts) >= 2:
        return " ".join(parts[-2:])
    return parts[0] if parts else ""

def format_revenue(revenue):
    if not revenue or revenue == 0:
        return "0"
    
    revenue = float(revenue)
    if revenue >= 1_000_000:
        return f"{revenue / 1_000_000:.1f}M"
    elif revenue >= 1_000:
        return f"{revenue / 1_000:.0f}K"
    else:
        return f"{revenue:.0f}"

def excel_date_to_datetime(excel_date):
    excel_epoch = datetime(1899, 12, 30)
    return excel_epoch + timedelta(days=excel_date)

def process_livestream_data(records):
    data_list = []
    for record in records:
        fields = record.get('fields', {})
        
        timestamp_start = fields.get('Thời gian phát live')
        if not timestamp_start:
            continue
        
        live_start = datetime.fromtimestamp(timestamp_start / 1000)
        
        excel_date_end = fields.get('Thời gian kết thúc')
        if not excel_date_end:
            continue
        
        live_end = excel_date_to_datetime(excel_date_end)
        
        if live_start.month != TARGET_MONTH or live_start.year != TARGET_YEAR:
            continue
        if live_start.day > CURRENT_DAY:
            continue
        
        full_name = extract_text(fields.get('Tên nhân viên live'))
        short_name = get_short_name(full_name)
        
        revenue = fields.get('Doanh thu thực tế', 0)
        if revenue:
            revenue = float(revenue)
        else:
            revenue = 0
        
        start_hour = live_start.hour + live_start.minute / 60.0 + live_start.second / 3600.0
        
        if live_end.day > live_start.day:
            end_hour = 24.0
            duration = end_hour - start_hour
        else:
            end_hour = live_end.hour + live_end.minute / 60.0 + live_end.second / 3600.0
            duration = end_hour - start_hour
        
        data_list.append({
            'Kênh': extract_text(fields.get('Kênh')),
            'Tên đầy đủ': full_name,
            'Tên ngắn': short_name,
            'Ngày': live_start.day,
            'Bắt đầu': live_start,
            'Kết thúc': live_end,
            'Giờ bắt đầu': start_hour,
            'Giờ kết thúc': end_hour,
            'Thời lượng (giờ)': duration,
            'Doanh thu': revenue,
            'Doanh thu (format)': format_revenue(revenue)
        })
    
    return pd.DataFrame(data_list)

def check_overlap(bar1, bar2):
    return not (bar1['Giờ kết thúc'] <= bar2['Giờ bắt đầu'] or bar2['Giờ kết thúc'] <= bar1['Giờ bắt đầu'])

def arrange_bars_in_rows(df_day):
    bars = df_day.to_dict('records')
    rows = []
    
    for bar in bars:
        placed = False
        for row in rows:
            has_overlap = False
            for existing_bar in row:
                if check_overlap(bar, existing_bar):
                    has_overlap = True
                    break
            
            if not has_overlap:
                row.append(bar)
                placed = True
                break
        
        if not placed:
            rows.append([bar])
    
    return rows

# ==================== TẠO HTML ====================
def create_html_gantt(df, channel_name):
    all_days = sorted(df['Ngày'].unique())
    
    employees = sorted(df['Tên ngắn'].unique())
    employee_colors = {emp: COLORS[i % len(COLORS)] for i, emp in enumerate(employees)}
    
    html = f"""
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lịch Livestream - {channel_name}</title>
    <link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&display=swap" rel="stylesheet">
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Nunito', -apple-system, BlinkMacSystemFont, sans-serif;
            background: #f8f9fa;
            padding: 30px 20px;
        }}
        
        .container {{
            background: white;
            border-radius: 16px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.08);
            padding: 0;
            max-width: 1900px;
            margin: 0 auto;
            overflow: hidden;
        }}
        
        .header {{
            background: white;
            color: #2d3436;
            padding: 30px 40px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            border-bottom: 3px solid #2d3436;
        }}
        
        .header-left {{
            display: flex;
            align-items: center;
            gap: 20px;
        }}
        
        .logo {{
            width: 70px;
            height: 70px;
            background: white;
            border-radius: 12px;
            padding: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        
        .logo img {{
            width: 100%;
            height: 100%;
            object-fit: contain;
        }}
        
        .header-text h1 {{
            font-size: 32px;
            font-weight: 800;
            margin-bottom: 5px;
            color: #2d3436;
            letter-spacing: -0.5px;
        }}
        
        .header-text .subtitle {{
            font-size: 16px;
            color: #636e72;
            font-weight: 600;
        }}
        
        .gantt-content {{
            padding: 35px 40px;
            background: white;
        }}
        
        .gantt-wrapper {{
            overflow-x: auto;
            border-radius: 12px;
            background: white;
            border: 2px solid #e9ecef;
        }}
        
        .gantt-container {{
            min-width: 1400px;
        }}
        
        .gantt-header {{
            display: flex;
            border-bottom: 2px solid #2d3436;
            background: white;
        }}
        
        .gantt-label {{
            width: 90px;
            padding: 16px 10px;
            font-weight: 800;
            color: #2d3436;
            text-align: center;
            border-right: 2px solid #e9ecef;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 1px;
            background: #f8f9fa;
        }}
        
        .gantt-hours {{
            flex: 1;
            display: flex;
            background: white;
        }}
        
        .hour-cell {{
            flex: 1;
            padding: 16px 2px;
            text-align: center;
            font-size: 11px;
            font-weight: 700;
            color: #2d3436;
            border-right: 1px solid #e9ecef;
            letter-spacing: 0.5px;
        }}
        
        .gantt-body {{
            background: white;
        }}
        
        .gantt-day-group {{
            border-bottom: 2px solid #e9ecef;
        }}
        
        .gantt-day-group:last-child {{
            border-bottom: none;
        }}
        
        .gantt-row {{
            display: flex;
            min-height: 65px;
        }}
        
        .gantt-row:not(:last-child) {{
            border-bottom: 1px solid #f1f3f5;
        }}
        
        .day-label {{
            width: 90px;
            padding: 16px;
            font-weight: 800;
            color: #2d3436;
            text-align: center;
            border-right: 2px solid #e9ecef;
            display: flex;
            align-items: center;
            justify-content: center;
            background: #f8f9fa;
            font-size: 15px;
        }}
        
        .day-label-empty {{
            width: 90px;
            border-right: 2px solid #e9ecef;
            background: #f8f9fa;
        }}
        
        .timeline {{
            flex: 1;
            position: relative;
        }}
        
        .timeline-bg {{
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            display: flex;
        }}
        
        .timeline-hour {{
            flex: 1;
            border-right: 1px solid #f1f3f5;
        }}
        
        .timeline-hour:nth-child(even) {{
            background: #fafbfc;
        }}
        
        .live-bar {{
            position: absolute;
            height: 52px;
            top: 6px;
            border-radius: 8px;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            font-size: 12px;
            font-weight: 700;
            color: #2d3436;
            box-shadow: 0 2px 6px rgba(0,0,0,0.12);
            overflow: hidden;
            padding: 7px 12px;
            border: 1px solid rgba(0,0,0,0.08);
            gap: 4px;
        }}
        
        .bar-name {{
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            letter-spacing: 0.2px;
            font-size: 12px;
            font-weight: 800;
            width: 100%;
            text-align: center;
            line-height: 1.3;
        }}
        
        .bar-revenue {{
            font-size: 10px;
            font-weight: 800;
            background: rgba(255,255,255,0.75);
            padding: 2px 8px;
            border-radius: 4px;
            white-space: nowrap;
            line-height: 1.3;
        }}
        
        .legend {{
            display: flex;
            flex-wrap: wrap;
            gap: 18px;
            margin-top: 30px;
            padding: 28px;
            background: #f8f9fa;
            border-radius: 12px;
            border: 2px solid #e9ecef;
        }}
        
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px 18px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.06);
            border: 1px solid #e9ecef;
        }}
        
        .legend-color {{
            width: 26px;
            height: 26px;
            border-radius: 6px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border: 1px solid rgba(0,0,0,0.08);
        }}
        
        .legend-name {{
            font-size: 14px;
            color: #2d3436;
            font-weight: 700;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="header-left">
                <div class="logo">
                    <img src="https://pos.nvncdn.com/f4d87e-8901/store/20151119_gvSbVoXmL33ZboSGXwazkWXV.jpg?v=1673230469" alt="ATINO Logo">
                </div>
                <div class="header-text">
                    <h1>Lịch Livestream - {channel_name}</h1>
                    <div class="subtitle">Tháng {TARGET_MONTH}/{TARGET_YEAR} • Từ ngày 1/{TARGET_MONTH} đến {CURRENT_DAY}/{TARGET_MONTH}</div>
                </div>
            </div>
        </div>
        
        <div class="gantt-content">
            <div class="gantt-wrapper">
                <div class="gantt-container">
                    <div class="gantt-header">
                        <div class="gantt-label">Ngày</div>
                        <div class="gantt-hours">
"""
    
    for hour in range(24):
        html += f'                            <div class="hour-cell">{hour:02d}h</div>\n'
    
    html += """                        </div>
                    </div>
                    
                    <div class="gantt-body">
"""
    
    for day in all_days:
        df_day = df[df['Ngày'] == day]
        rows = arrange_bars_in_rows(df_day)
        
        html += f'                        <div class="gantt-day-group">\n'
        
        for row_idx, row_bars in enumerate(rows):
            html += f'                            <div class="gantt-row">\n'
            
            if row_idx == 0:
                html += f'                                <div class="day-label">{day}/{TARGET_MONTH}</div>\n'
            else:
                html += f'                                <div class="day-label-empty"></div>\n'
            
            html += """                                <div class="timeline">
                                    <div class="timeline-bg">
"""
            
            for hour in range(24):
                html += f'                                        <div class="timeline-hour"></div>\n'
            
            html += '                                    </div>\n'
            
            for bar in row_bars:
                start = bar['Giờ bắt đầu']
                duration = bar['Thời lượng (giờ)']
                name = bar['Tên ngắn']
                color = employee_colors.get(name, '#B8C5D6')
                revenue_format = bar['Doanh thu (format)']
                
                left_percent = (start / 24) * 100
                width_percent = (duration / 24) * 100
                
                html += f"""                                    <div class="live-bar" 
                                         style="left: {left_percent:.2f}%; width: {width_percent:.2f}%; background: {color};">
                                        <span class="bar-name">{name}</span>
                                        <span class="bar-revenue">{revenue_format}</span>
                                    </div>
"""
            
            html += """                                </div>
                            </div>
"""
        
        html += '                        </div>\n'
    
    html += """                    </div>
                </div>
            </div>
            
            <div class="legend">
"""
    
    for emp, color in sorted(employee_colors.items()):
        html += f"""                <div class="legend-item">
                    <div class="legend-color" style="background: {color};"></div>
                    <div class="legend-name">{emp}</div>
                </div>
"""
    
    html += """            </div>
        </div>
    </div>
</body>
</html>
"""
    
    return html

# ==================== CHỤP ẢNH MÀN HÌNH ====================
import imgkit

def capture_html_screenshot(html_file, output_image):
    """Chụp ảnh màn hình từ file HTML"""
    print(f"Đang chụp ảnh: {html_file}")
    
    try:
        options = {
            'format': 'png',
            'width': 1920,
            'quality': 100,
            'enable-local-file-access': None,
            'encoding': 'UTF-8',
        }
        
        imgkit.from_file(html_file, output_image, options=options)
        print(f"✓ Đã lưu ảnh: {output_image}")
        return True
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return False

# ==================== GỬI VÀO LARK ====================
def upload_image_to_lark(image_path):
    """Upload ảnh lên Lark"""
    print(f"Upload: {image_path}")
    
    token = get_tenant_access_token()
    if not token:
        return None
    
    url = "https://open.larksuite.com/open-apis/im/v1/images"
    
    with open(image_path, 'rb') as f:
        files = {'image': (os.path.basename(image_path), f, 'image/png')}
        data = {'image_type': 'message'}
        headers = {'Authorization': f'Bearer {token}'}
        
        response = requests.post(url, headers=headers, data=data, files=files)
        result = response.json()
        
        if result.get('code') == 0:
            image_key = result['data']['image_key']
            print(f"✓ Image key: {image_key}")
            return image_key
        else:
            print(f"❌ Upload thất bại: {result}")
            return None

def send_all_to_lark_webhook(image_keys_data, total_df):
    """Gửi tất cả ảnh cùng lúc vào Lark"""
    print("Đang gửi tin nhắn vào Lark...")
    
    total_revenue = total_df['Doanh thu'].sum()
    total_sessions = len(total_df)
    
    # Tạo content
    content = [
        [{"tag": "text", "text": f"Lịch Livestream - Tháng {TARGET_MONTH}/{TARGET_YEAR}\n"}],
        [{"tag": "text", "text": f"Từ ngày 1/{TARGET_MONTH} đến {CURRENT_DAY}/{TARGET_MONTH}\n"}],
        [{"tag": "text", "text": f"Tổng doanh thu: {total_revenue:,.0f} VNĐ\n"}],
        [{"tag": "text", "text": f"Tổng phiên live: {total_sessions}\n\n"}]
    ]
    
    # Thêm tất cả ảnh
    for data in image_keys_data:
        content.append([{"tag": "text", "text": f"\n{data['channel']}\n"}])
        content.append([{
            "tag": "img",
            "image_key": data['image_key'],
            "width": 1920,
            "height": 800
        }])
    
    message = {
        "msg_type": "post",
        "content": {
            "post": {
                "vi_vn": {
                    "title": f"Lịch Livestream - Tháng {TARGET_MONTH}/{TARGET_YEAR}",
                    "content": content
                }
            }
        }
    }
    
    response = requests.post(webhook_url, json=message)
    result = response.json()
    
    if result.get('code') == 0:
        print("✓ Gửi tin nhắn thành công!")
        return True
    else:
        print(f"❌ Gửi thất bại: {result}")
        return False

# ==================== MAIN ====================
if __name__ == "__main__":
    print("=" * 80)
    print("TẠO GANTT CHART VÀ GỬI VÀO LARK")
    print("=" * 80)
    
    # 1. Lấy dữ liệu
    records = get_all_records()
    if not records:
        print("Không thể lấy dữ liệu!")
        exit()
    
    # 2. Xử lý dữ liệu
    df = process_livestream_data(records)
    
    if len(df) == 0:
        print("\n❌ Không có dữ liệu!")
        exit()
    
    print(f"\n✓ Tổng phiên live: {len(df)}")
    print(f"✓ Số kênh: {df['Kênh'].nunique()}")
    print(f"✓ Tổng doanh thu: {df['Doanh thu'].sum():,.0f} VNĐ")
    
    # 3. Tạo HTML và screenshot cho tất cả kênh
    print("\n" + "=" * 80)
    print("TẠO HTML VÀ SCREENSHOT")
    print("=" * 80)
    
    image_keys_data = []
    
    for channel in sorted(df['Kênh'].unique()):
        print(f"\n--- Kênh: {channel} ---")
        
        df_channel = df[df['Kênh'] == channel]
        
        # Tạo HTML
        html_content = create_html_gantt(df_channel, channel)
        html_filename = f"gantt_{channel.replace('.', '_').replace(' ', '_')}.html"
        
        with open(html_filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"✓ HTML: {html_filename}")
        
        # Chụp ảnh
        image_filename = html_filename.replace('.html', '.png')
        if capture_html_screenshot(html_filename, image_filename):
            # Upload ảnh
            image_key = upload_image_to_lark(image_filename)
            
            if image_key:
                image_keys_data.append({
                    'channel': channel,
                    'image_key': image_key,
                    'revenue': df_channel['Doanh thu'].sum()
                })
            
            # Xóa file tạm
            try:
                os.remove(image_filename)
            except:
                pass
    
    # 4. Gửi tất cả ảnh cùng lúc
    if image_keys_data:
        print("\n" + "=" * 80)
        print("GỬI TIN NHẮN VÀO LARK")
        print("=" * 80)
        send_all_to_lark_webhook(image_keys_data, df)
    
    print("\n" + "=" * 80)
    print("HOÀN THÀNH!")
    print("=" * 80)
