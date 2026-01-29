import requests
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor
import logging

class DripInvestingScraper:
    BASE_URL = "https://www.dripinvesting.org"
    STOCKS_URL = "https://www.dripinvesting.org/stocks/"

    def __init__(self, max_workers=10):
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_tickers(self):
        """
        Scrapes the main stocks page to find all ticker URLs.
        Returns a list of dictionaries: [{'symbol': 'JNJ', 'url': '...'}]
        """
        self.logger.info(f"Fetching tickers from {self.STOCKS_URL}...")
        try:
            response = self.session.get(self.STOCKS_URL)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            tickers = []
            # Find links ending with -dividend-history-calculator-returns/
            # and containing text that looks like a ticker (uppercase, 1-5 chars)
            links = soup.find_all('a', href=re.compile(r'-dividend-history-calculator-returns/$'))
            
            seen_symbols = set()
            
            for link in links:
                url = link['href']
                text = link.get_text(strip=True)
                
                # Simple heuristic for ticker symbol
                if text and text.isupper() and len(text) <= 5 and text not in seen_symbols:
                    if not url.startswith("http"):
                        url = self.BASE_URL + url if url.startswith("/") else self.BASE_URL + "/" + url
                    
                    tickers.append({
                        "symbol": text,
                        "url": url
                    })
                    seen_symbols.add(text)
            
            self.logger.info(f"Found {len(tickers)} tickers.")
            return tickers
        except Exception as e:
            self.logger.error(f"Error fetching tickers: {e}")
            return []

    def get_stock_data(self, stock_info):
        """
        Fetches and parses data for a single stock.
        """
        symbol = stock_info["symbol"]
        url = stock_info["url"]
        
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                self.logger.warning(f"Failed to fetch data for {symbol}: {response.status_code}")
                return None
                
            soup = BeautifulSoup(response.content, 'html.parser')
            data = {"Symbol": symbol}
            
            # Helper to safely get text
            def get_text_safe(element):
                return element.get_text(strip=True) if element else None

            # 1. Parse Data Rows
            data_rows = soup.find_all("div", class_="data-row")
            for row in data_rows:
                label_elem = row.find("span", class_="data-label")
                value_elem = row.find("span", class_="data-value")
                
                if label_elem and value_elem:
                    label = get_text_safe(label_elem)
                    value = get_text_safe(value_elem)
                    
                    # Store raw value, cleaning can happen later or here
                    # Map known labels to DB columns if needed, or just store as is
                    # The DB columns match many of these labels exactly or closely
                    
                    # Handle known mappings
                    if label == "Payouts/Year":
                        data["Payouts/ Year"] = value
                    elif label == "High (52W)":
                        data["High"] = value
                    elif label == "Low (52W)":
                        data["Low"] = value
                    else:
                        data[label] = value

            # 2. Parse Years/Streak
            years_tag = soup.find("span", class_="years-tag")
            if years_tag:
                years_text = get_text_safe(years_tag)
                # Extract number from "63 Years"
                match = re.search(r'(\d+)', years_text)
                if match:
                    data["No Years"] = match.group(1)
            
            # 3. Ensure essential columns exist (fill with None if missing)
            # This helps avoid KeyErrors later
            essential_cols = [
                "Company", "Sector", "Industry", "Price", "Div Yield", "5Y Avg Yield", 
                "Current Div", "Annualized", "DGR 1Y", "DGR 3Y", "DGR 5Y", 
                "DGR 10Y", "TTR 1Y", "TTR 3Y", "Chowder Number", "PEG", "P/E"
            ]
            for col in essential_cols:
                if col not in data:
                    data[col] = None

            return data

        except Exception as e:
            self.logger.error(f"Error processing {symbol}: {e}")
            return None

    def scrape_all_data(self):
        """
        Main method to orchestrate scraping.
        """
        tickers = self.get_tickers()
        all_data = []
        
        self.logger.info(f"Starting scrape for {len(tickers)} stocks with {self.max_workers} threads...")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = executor.map(self.get_stock_data, tickers)
            
            for res in results:
                if res:
                    all_data.append(res)
        
        self.logger.info(f"Scraping complete. Collected data for {len(all_data)} stocks.")
        return all_data

if __name__ == "__main__":
    # Simple test run
    scraper = DripInvestingScraper(max_workers=5)
    tickers = scraper.get_tickers()
    print(f"Found {len(tickers)} tickers")
    if tickers:
        print("Fetching first stock:", tickers[0])
        data = scraper.get_stock_data(tickers[0])
        print("Data:", data)
