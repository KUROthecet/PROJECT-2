import pandas as pd
import requests
import json
import time
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging
from typing import Dict, List, Optional, Tuple

class TikiProductScraper:
    def __init__(self, 
                 excel_path: str = "C:\\Users\\ThinkPad\\Downloads\\products-0-200000.xlsx",
                 output_dir: str = "tiki_products",
                 products_per_file: int = 1000,
                 max_workers: int = 20,
                 request_timeout: int = 10,
                 retry_attempts: int = 3):
        """        
        Args:
            excel_path: Đường dẫn file Excel chứa product_id
            output_dir: Thư mục lưu kết quả
            products_per_file: Số sản phẩm mỗi file JSON
            max_workers: Số luồng xử lý song song
            request_timeout: Timeout cho request (giây)
            retry_attempts: Số lần thử lại khi lỗi
        """
        self.excel_path = excel_path
        self.output_dir = output_dir
        self.products_per_file = products_per_file
        self.max_workers = max_workers
        self.request_timeout = request_timeout
        self.retry_attempts = retry_attempts
        
        # Tạo thư mục output nếu chưa tồn tại
        os.makedirs(output_dir, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(output_dir, 'scraper.log')),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Thread-safe counters và collections
        self.lock = Lock()
        self.successful_products = []
        self.failed_products = []
        self.processed_count = 0
        
        # Headers cho request
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
        }

    def clean_description(self, description: str) -> str:

        if not description:
            return ""
        
        # Loại bỏ HTML tags
        description = re.sub(r'<[^>]+>', '', description)
        
        # Loại bỏ các ký tự đặc biệt và khoảng trắng thừa
        description = re.sub(r'\s+', ' ', description)
        
        # Loại bỏ các ký tự không mong muốn
        description = re.sub(r'[^\w\s\-.,!?()%]', '', description)
        
        return description.strip()

    def extract_product_data(self, api_response: Dict) -> Dict:

        try:
            # Lấy images URLs
            images = []
            if 'images' in api_response:
                for img in api_response['images']:
                    if isinstance(img, dict) and 'base_url' in img:
                        images.append(img['base_url'])
                    elif isinstance(img, str):
                        images.append(img)
            
            product_data = {
                'id': api_response.get('id'),
                'name': api_response.get('name', ''),
                'url_key': api_response.get('url_key', ''),
                'price': api_response.get('price', 0),
                'description': self.clean_description(api_response.get('description', '')),
                'images': images
            }
            
            return product_data
            
        except Exception as e:
            self.logger.error(f"Error extracting product data: {e}")
            return None

    def fetch_product_details(self, product_id: str) -> Tuple[Optional[Dict], Optional[Dict]]:
        
        # Lấy thông tin chi tiết sản phẩm từ API

        url = f"https://api.tiki.vn/product-detail/api/v1/products/{product_id}"
        
        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(
                    url, 
                    headers=self.headers, 
                    timeout=self.request_timeout
                )
                
                if response.status_code == 200:
                    api_data = response.json()
                    product_data = self.extract_product_data(api_data)
                    
                    if product_data:
                        return product_data, None
                    else:
                        error_info = {
                            'product_id': product_id,
                            'error': 'Failed to extract product data',
                            'attempt': attempt + 1,
                            'status_code': response.status_code
                        }
                        return None, error_info
                        
                elif response.status_code == 404:
                    error_info = {
                        'product_id': product_id,
                        'error': 'Product not found',
                        'status_code': 404
                    }
                    return None, error_info
                    
                else:
                    if attempt == self.retry_attempts - 1:
                        error_info = {
                            'product_id': product_id,
                            'error': f'HTTP {response.status_code}',
                            'status_code': response.status_code
                        }
                        return None, error_info
                    time.sleep(1)  # Đợi trước khi retry
                    
            except requests.exceptions.RequestException as e:
                if attempt == self.retry_attempts - 1:
                    error_info = {
                        'product_id': product_id,
                        'error': str(e),
                        'attempt': attempt + 1
                    }
                    return None, error_info
                time.sleep(1)  # Đợi trước khi retry
                
        return None, None

    def process_batch(self, product_ids: List[str]) -> None:

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_id = {
                executor.submit(self.fetch_product_details, pid): pid 
                for pid in product_ids
            }
            
            # Process completed tasks
            for future in as_completed(future_to_id):
                product_id = future_to_id[future]
                
                try:
                    product_data, error_info = future.result()
                    
                    with self.lock:
                        self.processed_count += 1
                        
                        if product_data:
                            self.successful_products.append(product_data)
                        elif error_info:
                            self.failed_products.append(error_info)
                        
                        # Log progress
                        if self.processed_count % 100 == 0:
                            self.logger.info(f"Processed {self.processed_count} products")
                            
                except Exception as e:
                    with self.lock:
                        self.processed_count += 1
                        error_info = {
                            'product_id': product_id,
                            'error': f'Processing error: {str(e)}'
                        }
                        self.failed_products.append(error_info)

    def save_products_to_files(self) -> None:
        # Lưu dữ liệu sản phẩm vào các file JSON
        if not self.successful_products:
            self.logger.warning("No successful products to save")
            return
            
        # Chia thành các batch để lưu file
        total_products = len(self.successful_products)
        file_count = 0
        
        for i in range(0, total_products, self.products_per_file):
            file_count += 1
            batch = self.successful_products[i:i + self.products_per_file]
            
            filename = os.path.join(self.output_dir, f"products_part{file_count}.json")
            
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(batch, f, ensure_ascii=False, indent=2)
                    
                self.logger.info(f"Saved {len(batch)} products to {filename}")
                
            except Exception as e:
                self.logger.error(f"Error saving file {filename}: {e}")

    def save_failed_products(self) -> None:
        # Lưu danh sách sản phẩm bị lỗi
        if not self.failed_products:
            self.logger.info("No failed products to save")
            return
            
        filename = os.path.join(self.output_dir, "failed_products.json")
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.failed_products, f, ensure_ascii=False, indent=2)
                
            self.logger.info(f"Saved {len(self.failed_products)} failed products to {filename}")
            
        except Exception as e:
            self.logger.error(f"Error saving failed products: {e}")

    def load_product_ids(self) -> List[str]:
    
        # Đọc danh sách product IDs từ file Excel

        try:
            if self.excel_path.endswith('.xlsx') or self.excel_path.endswith('.xls'):
                df = pd.read_excel(self.excel_path)
            elif self.excel_path.endswith('.csv'):
                df = pd.read_csv(self.excel_path)
            else:
                raise ValueError("Unsupported file format")
            
            # Tìm cột chứa product ID
            id_columns = ['id', 'product_id', 'ID', 'Product_ID']
            id_column = None
            
            for col in id_columns:
                if col in df.columns:
                    id_column = col
                    break
            
            if id_column is None:
                # Sử dụng cột đầu tiên nếu không tìm thấy
                id_column = df.columns[0]
                self.logger.warning(f"Using first column '{id_column}' as product ID")
            
            # Lấy danh sách product IDs và loại bỏ NaN
            product_ids = df[id_column].dropna().astype(str).tolist()
            
            self.logger.info(f"Loaded {len(product_ids)} product IDs from {self.excel_path}")
            return product_ids
            
        except Exception as e:
            self.logger.error(f"Error loading product IDs: {e}")
            raise

    def run(self) -> None:

        start_time = time.time()
        
        try:
            # Load product IDs
            self.logger.info("Loading product IDs...")
            product_ids = self.load_product_ids()
            
            if not product_ids:
                self.logger.error("No product IDs found")
                return
            
            # Giới hạn số lượng nếu cần
            total_to_process = min(len(product_ids), 200000)
            product_ids = product_ids[:total_to_process]
            
            self.logger.info(f"Starting to process {total_to_process} products...")
            
            # Process in batches để tránh quá tải memory
            batch_size = 5000
            for i in range(0, len(product_ids), batch_size):
                batch = product_ids[i:i + batch_size]
                self.logger.info(f"Processing batch {i//batch_size + 1}: products {i+1} to {min(i+batch_size, len(product_ids))}")
                
                self.process_batch(batch)
                
                # Thêm delay nhỏ giữa các batch
                time.sleep(2)
            
            # Save results
            self.logger.info("Saving results...")
            self.save_products_to_files()
            self.save_failed_products()
            
            # Summary
            elapsed_time = time.time() - start_time
            success_rate = len(self.successful_products) / total_to_process * 100
            
            self.logger.info(f"""
=== SCRAPING COMPLETED ===
Total processed: {self.processed_count}
Successful: {len(self.successful_products)}
Failed: {len(self.failed_products)}
Success rate: {success_rate:.2f}%
Time elapsed: {elapsed_time:.2f} seconds
Average time per product: {elapsed_time/self.processed_count:.3f} seconds
Files created: {len(self.successful_products)//self.products_per_file + (1 if len(self.successful_products)%self.products_per_file > 0 else 0)}
""")
            
        except Exception as e:
            self.logger.error(f"Error during scraping: {e}")
            raise

def main():
    scraper = TikiProductScraper(
        excel_path="C:\\Users\\ThinkPad\\Downloads\\products-0-200000.xlsx",
        output_dir="tiki_products_output",
        products_per_file=1000,
        max_workers=15,  # Giảm số workers để tránh bị ban
        request_timeout=15,
        retry_attempts=3
    )
    
    scraper.run()

if __name__ == "__main__":
    main()