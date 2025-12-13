import requests
import json
from datetime import datetime, timedelta
import os

class LarkBaseAlertSender:
    def __init__(self, app_id, app_secret, app_token, table_id, webhook_url):
        self.app_id = app_id
        self.app_secret = app_secret
        self.app_token = app_token
        self.table_id = table_id
        self.webhook_url = webhook_url
        self.base_url = "https://open.larksuite.com/open-apis"
        self.access_token = None
    
    def get_tenant_access_token(self):
        """Lấy tenant access token"""
        url = f"{self.base_url}/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        try:
            response = requests.post(url, json=payload)
            result = response.json()
            
            if result.get("code") == 0:
                self.access_token = result["tenant_access_token"]
                print("✅ Đã lấy Lark access token")
                return True
            else:
                print(f"❌ Lỗi lấy token: {result}")
                return False
        except Exception as e:
            print(f"❌ Lỗi: {e}")
            return False
    
    def convert_timestamp_to_datetime(self, timestamp):
        """Chuyển timestamp (milliseconds) thành datetime object"""
        if timestamp:
            try:
                return datetime.fromtimestamp(timestamp / 1000)
            except:
                return None
        return None
    
    def format_datetime(self, dt):
        """Format datetime thành string"""
        if dt:
            return dt.strftime("%d/%m/%Y %H:%M")
        return "N/A"
    
    def read_all_records(self):
        """Đọc tất cả records từ Lark Base"""
        if not self.access_token:
            return []
        
        print(f"\n📥 Đang đọc dữ liệu từ Lark Base...")
        
        url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        all_records = []
        page_token = None
        
        while True:
            params = {
                "page_size": 500
            }
            if page_token:
                params["page_token"] = page_token
            
            try:
                response = requests.get(url, headers=headers, params=params)
                result = response.json()
                
                if result.get("code") == 0:
                    data = result.get("data", {})
                    items = data.get("items", [])
                    
                    all_records.extend(items)
                    print(f"  ✓ Đã đọc {len(items)} records (Tổng: {len(all_records)})")
                    
                    has_more = data.get("has_more", False)
                    if has_more:
                        page_token = data.get("page_token")
                    else:
                        break
                else:
                    print(f"  ❌ Lỗi: {result}")
                    break
                    
            except Exception as e:
                print(f"  ❌ Lỗi: {e}")
                break
        
        print(f"✅ Đã đọc tổng cộng {len(all_records)} records")
        return all_records
    
    def filter_yesterday_records(self, records):
        """Lọc records của ngày hôm qua"""
        print(f"\n🔍 Đang lọc dữ liệu ngày hôm qua...")
        
        # Lấy ngày hôm qua (từ 00:00:00 đến 23:59:59)
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_start = today - timedelta(days=1)
        yesterday_end = today - timedelta(seconds=1)
        
        print(f"  📅 Lọc từ: {yesterday_start.strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"  📅 Đến: {yesterday_end.strftime('%d/%m/%Y %H:%M:%S')}")
        
        filtered = []
        
        for record in records:
            fields = record.get("fields", {})
            timestamp = fields.get("Ngày giờ vi phạm")
            
            if timestamp:
                dt = self.convert_timestamp_to_datetime(timestamp)
                if dt and yesterday_start <= dt <= yesterday_end:
                    filtered.append(record)
        
        print(f"✅ Tìm thấy {len(filtered)} records của ngày hôm qua")
        return filtered
    
    def extract_link_text(self, link_field):
        """Trích xuất text từ link field"""
        if not link_field:
            return ""
        
        if isinstance(link_field, list) and len(link_field) > 0:
            return link_field[0].get("text", "")
        
        return str(link_field)
    
    def create_table_message(self, records):
        """Tạo message dạng bảng"""
        if not records:
            return None
        
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime("%d/%m/%Y")
        
        # Tạo header và summary
        elements = [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**Số vi phạm:** {len(records)}\n**Ngày:** {yesterday_str}"
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
                        "weight": 3,
                        "vertical_align": "top",
                        "elements": [
                            {
                                "tag": "div",
                                "text": {
                                    "content": "**Tiêu đề**",
                                    "tag": "lark_md"
                                }
                            }
                        ]
                    },
                    {
                        "tag": "column",
                        "width": "weighted",
                        "weight": 3,
                        "vertical_align": "top",
                        "elements": [
                            {
                                "tag": "div",
                                "text": {
                                    "content": "**Lý do**",
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
                                    "content": "**Ngày giờ**",
                                    "tag": "lark_md"
                                }
                            }
                        ]
                    },
                    {
                        "tag": "column",
                        "width": "weighted",
                        "weight": 3,
                        "vertical_align": "top",
                        "elements": [
                            {
                                "tag": "div",
                                "text": {
                                    "content": "**Tên sản phẩm**",
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
        
        # Thêm data rows
        for record in records:
            fields = record.get("fields", {})
            
            # Lấy dữ liệu
            tieu_de = fields.get("Tiêu đề vi phạm", "")[:50]
            ly_do = fields.get("Lý do (bảng)", "")[:60]
            ngay_gio_ts = fields.get("Ngày giờ vi phạm")
            ngay_gio = self.format_datetime(self.convert_timestamp_to_datetime(ngay_gio_ts))
            ten_sp = fields.get("Tên sản phẩm", "")[:50]
            link_chi_tiet = self.extract_link_text(fields.get("Link chi tiết"))
            
            # Truncate text
            if len(tieu_de) >= 50:
                tieu_de = tieu_de[:47] + "..."
            if len(ly_do) >= 60:
                ly_do = ly_do[:57] + "..."
            if len(ten_sp) >= 50:
                ten_sp = ten_sp[:47] + "..."
            
            elements.append({
                "tag": "column_set",
                "flex_mode": "none",
                "background_style": "default",
                "columns": [
                    {
                        "tag": "column",
                        "width": "weighted",
                        "weight": 3,
                        "vertical_align": "top",
                        "elements": [
                            {
                                "tag": "div",
                                "text": {
                                    "content": tieu_de,
                                    "tag": "plain_text"
                                }
                            }
                        ]
                    },
                    {
                        "tag": "column",
                        "width": "weighted",
                        "weight": 3,
                        "vertical_align": "top",
                        "elements": [
                            {
                                "tag": "div",
                                "text": {
                                    "content": ly_do,
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
                                    "content": ngay_gio,
                                    "tag": "plain_text"
                                }
                            }
                        ]
                    },
                    {
                        "tag": "column",
                        "width": "weighted",
                        "weight": 3,
                        "vertical_align": "top",
                        "elements": [
                            {
                                "tag": "div",
                                "text": {
                                    "content": ten_sp,
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
                                    "content": f"[Chi tiết]({link_chi_tiet})" if link_chi_tiet else "N/A",
                                    "tag": "lark_md"
                                }
                            }
                        ]
                    }
                ]
            })
        
        # Tạo message card
        message = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "content": f"⚠️ BÁO CÁO VI PHẠM NGÀY {yesterday_str}",
                        "tag": "plain_text"
                    },
                    "template": "red"
                },
                "elements": elements
            }
        }
        
        return message
    
    def send_to_webhook(self, message):
        """Gửi message đến webhook"""
        if not message:
            print("⚠️  Không có message để gửi")
            return False
        
        print(f"\n📤 Đang gửi message đến webhook...")
        
        try:
            response = requests.post(
                self.webhook_url,
                json=message,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    print("✅ Đã gửi thành công đến Lark!")
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
    
    def run(self):
        """Chạy chương trình"""
        print("\n" + "="*70)
        print("LARK BASE → WEBHOOK ALERT (YESTERDAY)")
        print("="*70)
        
        # 1. Lấy access token
        print(f"\n[1] Đang lấy access token...")
        if not self.get_tenant_access_token():
            print("❌ Không thể lấy access token!")
            return
        
        # 2. Đọc tất cả records
        print(f"\n[2] Đang đọc dữ liệu...")
        all_records = self.read_all_records()
        
        if not all_records:
            print(f"\n⚠️  Không có dữ liệu!")
            return
        
        # 3. Lọc records ngày hôm qua
        print(f"\n[3] Đang lọc dữ liệu...")
        yesterday_records = self.filter_yesterday_records(all_records)
        
        if not yesterday_records:
            print(f"\n⚠️  Không có vi phạm nào trong ngày hôm qua!")
            print(f"✅ Chương trình hoàn thành (không có gì để gửi)")
            return
        
        # 4. Tạo message
        print(f"\n[4] Đang tạo message...")
        message = self.create_table_message(yesterday_records)
        
        # 5. Gửi webhook
        print(f"\n[5] Đang gửi webhook...")
        success = self.send_to_webhook(message)
        
        # Tổng kết
        print(f"\n{'='*70}")
        if success:
            print(f"✅ HOÀN THÀNH!")
            print(f"📊 Đã gửi {len(yesterday_records)} vi phạm của ngày hôm qua")
        else:
            print(f"⚠️  CÓ LỖI XẢY RA!")
        print(f"{'='*70}")


def main():
    # Lark credentials
    app_id = "cli_a8620f964a38d02f"
    app_secret = "G3FdlSvmTAXZYX8SBZtfpckHUiWUCO4h"
    app_token = "AVY3bPgpja7Xwks2ht6lNGsnglc"
    table_id = "tbluOxVXn0oyPNKa"
    webhook_url = "https://open.larksuite.com/open-apis/bot/v2/hook/175214ad-f698-45a6-89d3-45ff7453429d"
    
    sender = LarkBaseAlertSender(
        app_id=app_id,
        app_secret=app_secret,
        app_token=app_token,
        table_id=table_id,
        webhook_url=webhook_url
    )
    
    sender.run()


if __name__ == "__main__":
    main()
