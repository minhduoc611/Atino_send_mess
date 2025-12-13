import requests
import json
from datetime import datetime
import time

# Lark Base credentials
app_id = "cli_a8620f964a38d02f"
app_secret = "G3FdlSvmTAXZYX8SBZtfpckHUiWUCO4h"
app_token = "AVY3bPgpja7Xwks2ht6lNGsnglc"
table_id = "tblv8eFkYSqUNqRG"
webhook_url = "https://open.larksuite.com/open-apis/bot/v2/hook/ec2a7b8c-197a-42a9-8125-870d7f602ccb"

def get_tenant_access_token():
    """Lấy tenant access token từ Lark API"""
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    headers = {
        "Content-Type": "application/json; charset=utf-8"
    }
    payload = {
        "app_id": app_id,
        "app_secret": app_secret
    }
    
    response = requests.post(url, headers=headers, json=payload)
    result = response.json()
    
    if result.get("code") == 0:
        return result.get("tenant_access_token")
    else:
        print(f"❌ Lỗi lấy token: {result}")
        return None

def read_lark_base_records(token):
    """Đọc tất cả records từ Lark Base"""
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    all_records = []
    page_token = None
    
    while True:
        params = {
            "page_size": 100
        }
        if page_token:
            params["page_token"] = page_token
        
        response = requests.get(url, headers=headers, params=params)
        result = response.json()
        
        if result.get("code") == 0:
            data = result.get("data", {})
            records = data.get("items", [])
            all_records.extend(records)
            
            print(f"  ✓ Đã lấy {len(records)} records (Tổng: {len(all_records)})")
            
            # Kiểm tra còn page nữa không
            has_more = data.get("has_more", False)
            if has_more:
                page_token = data.get("page_token")
            else:
                break
        else:
            print(f"❌ Lỗi đọc records: {result}")
            break
    
    return all_records

def filter_upcoming_programs(records):
    """Lọc ra các chương trình có ngày bắt đầu lớn hơn ngày hôm nay"""
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_timestamp = int(today.timestamp() * 1000)  # Convert to milliseconds
    
    upcoming_records = []
    
    for record in records:
        fields = record.get('fields', {})
        start_date_ts = fields.get('Ngày bắt đầu')
        
        # Chỉ lấy những chương trình có ngày bắt đầu > hôm nay
        if start_date_ts and start_date_ts > today_timestamp:
            upcoming_records.append(record)
    
    return upcoming_records

def convert_timestamp(ts):
    """Chuyển đổi timestamp thành DD/MM/YYYY"""
    if ts:
        return datetime.fromtimestamp(ts / 1000).strftime('%d/%m/%Y')
    return "N/A"

def create_table_message(records):
    """Tạo message dạng bảng từ records"""
    
    # Loại bỏ trùng lặp dựa trên tên chiến dịch phụ
    unique_campaigns = {}
    for record in records:
        fields = record.get('fields', {})
        name = fields.get('Chiến dịch phụ', '')
        if name and name not in unique_campaigns:
            unique_campaigns[name] = fields
    
    # Sắp xếp theo ngày bắt đầu (gần nhất trước)
    sorted_campaigns = sorted(
        unique_campaigns.items(),
        key=lambda x: x[1].get('Ngày bắt đầu', 0)
    )
    
    # Tạo các dòng cho bảng (mỗi dòng là một list các cell)
    table_rows = []
    
    for idx, (name, fields) in enumerate(sorted_campaigns, 1):
        start_date = convert_timestamp(fields.get('Ngày bắt đầu'))
        end_date = convert_timestamp(fields.get('Ngày kết thúc'))
        link = fields.get('Link chi tiết', '')
        
        # Rút ngắn tên nếu quá dài
        short_name = name[:50] + "..." if len(name) > 50 else name
        
        # Tạo row với các columns
        row = [
            str(idx),                    # STT
            short_name,                  # Chương trình
            start_date,                  # Ngày bắt đầu
            end_date,                    # Ngày kết thúc
            f"[Chi tiết]({link})" if link else "N/A"  # Link
        ]
        table_rows.append(row)
    
    # Tạo message card với column_set (dạng bảng)
    elements = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**📅 Các chương trình sắp diễn ra**\n\n**Tổng số:** {len(unique_campaigns)} chương trình\n**Cập nhật:** {datetime.now().strftime('%d/%m/%Y %H:%M')}"
            }
        },
        {
            "tag": "hr"
        },
        # Header row
        {
            "tag": "column_set",
            "flex_mode": "none",
            "background_style": "grey",
            "columns": [
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 1,
                    "vertical_align": "top",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "content": "**STT**",
                                "tag": "lark_md"
                            }
                        }
                    ]
                },
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 4,
                    "vertical_align": "top",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "content": "**Chương trình**",
                                "tag": "lark_md"
                            }
                        }
                    ]
                },
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 2,
                    "vertical_align": "top",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "content": "**Ngày bắt đầu**",
                                "tag": "lark_md"
                            }
                        }
                    ]
                },
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 2,
                    "vertical_align": "top",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "content": "**Ngày kết thúc**",
                                "tag": "lark_md"
                            }
                        }
                    ]
                },
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 1,
                    "vertical_align": "top",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "content": "**Link**",
                                "tag": "lark_md"
                            }
                        }
                    ]
                }
            ]
        }
    ]
    
    # Thêm các data rows
    for row_data in table_rows:
        elements.append({
            "tag": "column_set",
            "flex_mode": "none",
            "background_style": "default",
            "columns": [
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 1,
                    "vertical_align": "top",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "content": row_data[0],
                                "tag": "plain_text"
                            }
                        }
                    ]
                },
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 4,
                    "vertical_align": "top",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "content": row_data[1],
                                "tag": "plain_text"
                            }
                        }
                    ]
                },
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 2,
                    "vertical_align": "top",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "content": row_data[2],
                                "tag": "plain_text"
                            }
                        }
                    ]
                },
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 2,
                    "vertical_align": "top",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "content": row_data[3],
                                "tag": "plain_text"
                            }
                        }
                    ]
                },
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 1,
                    "vertical_align": "top",
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "content": row_data[4],
                                "tag": "lark_md"
                            }
                        }
                    ]
                }
            ]
        })
    
    # Thêm footer với nút xem chi tiết
    elements.append({
        "tag": "hr"
    })
    elements.append({
        "tag": "action",
        "actions": [
            {
                "tag": "button",
                "text": {
                    "tag": "plain_text",
                    "content": "📋 Xem tất cả trong Lark Base"
                },
                "type": "primary",
                "url": f"https://atino-vietnam.sg.larksuite.com/base/{app_token}?table={table_id}"
            }
        ]
    })
    
    # Tạo message card
    message = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {
                    "content": "📢 CHƯƠNG TRÌNH TIKTOK SẮP DIỄN RA",
                    "tag": "plain_text"
                },
                "template": "blue"
            },
            "elements": elements
        }
    }
    
    return message

def send_to_webhook(message):
    """Gửi message đến Lark webhook"""
    try:
        response = requests.post(
            webhook_url,
            json=message,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('code') == 0:
                print("✅ Gửi thành công!")
                return True
            else:
                print(f"❌ Lỗi: {result}")
                return False
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def main():
    print("=" * 60)
    print("GỬI THÔNG BÁO CHƯƠNG TRÌNH TIKTOK SẮP DIỄN RA")
    print("=" * 60)
    
    # Bước 1: Lấy access token
    print("\n[1] Đang lấy access token...")
    token = get_tenant_access_token()
    if not token:
        print("❌ Không thể lấy access token!")
        return
    print("✅ Đã lấy access token")
    
    # Bước 2: Đọc dữ liệu từ Lark Base
    print("\n[2] Đang đọc dữ liệu từ Lark Base...")
    records = read_lark_base_records(token)
    if not records:
        print("❌ Không có dữ liệu!")
        return
    print(f"✅ Đã đọc {len(records)} records")
    
    # Bước 3: Lọc chương trình sắp diễn ra
    print("\n[3] Đang lọc chương trình sắp diễn ra...")
    today_str = datetime.now().strftime('%d/%m/%Y')
    print(f"  ℹ️  Ngày hôm nay: {today_str}")
    
    upcoming_records = filter_upcoming_programs(records)
    print(f"✅ Tìm thấy {len(upcoming_records)} chương trình sắp diễn ra")
    
    if not upcoming_records:
        print("⚠️  Không có chương trình nào sắp diễn ra!")
        return
    
    # Bước 4: Tạo message
    print("\n[4] Đang tạo message...")
    message = create_table_message(upcoming_records)
    print("✅ Đã tạo message")
    
    # Bước 5: Gửi đến webhook
    print("\n[5] Đang gửi đến webhook...")
    success = send_to_webhook(message)
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 HOÀN THÀNH!")
    else:
        print("⚠️  CÓ LỖI XẢY RA!")
    print("=" * 60)

if __name__ == "__main__":
    main()
