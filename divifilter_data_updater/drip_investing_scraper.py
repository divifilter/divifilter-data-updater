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
        Scrapes the main stocks page and all pagination pages to find all ticker URLs.
        Returns a list of dictionaries: [{'symbol': 'JNJ', 'url': '...'}]
        """
        all_tickers = []
        seen_symbols = set()
        page = 1
        
        while True:
            # Construct URL for current page
            if page == 1:
                url = self.STOCKS_URL
            else:
                url = f"{self.STOCKS_URL}?stocks_page={page}"
            
            self.logger.info(f"Fetching tickers from page {page}: {url}")
            
            try:
                response = self.session.get(url)
                
                # If we get a 404, we might have reached the end (though usually it just returns empty or same page)
                if response.status_code == 404:
                    self.logger.info(f"Reached end of pagination at page {page} (404)")
                    break
                    
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find links ending with -dividend-history-calculator-returns/
                links = soup.find_all('a', href=re.compile(r'-dividend-history-calculator-returns/$'))
                
                # Track if we found any new tickers on this page
                found_any_new_on_this_page = False
                
                for link in links:
                    url_path = link['href']
                    text = link.get_text(strip=True)
                    
                    # Simple heuristic for ticker symbol
                    if text and text.isupper() and len(text) <= 5 and text not in seen_symbols:
                        if not url_path.startswith("http"):
                            url_path = self.BASE_URL + url_path if url_path.startswith("/") else self.BASE_URL + "/" + url_path
                        
                        all_tickers.append({
                            "symbol": text,
                            "url": url_path
                        })
                        seen_symbols.add(text)
                        found_any_new_on_this_page = True
                
                # If we didn't find any NEW tickers on this page, and it's not the first page, we stop.
                # Usually if you go past the last page, it either shows an empty list or the first page.
                if not found_any_new_on_this_page and page > 1:
                    self.logger.info(f"No new tickers found on page {page}, stopping pagination")
                    break
                
                # If the page had NO stock links at all, we also stop
                if not links:
                    self.logger.info(f"No stock links found on page {page}, stopping pagination")
                    break

                page += 1
                
                # Safety limit to prevent infinite loops (user said 7 pages, but we'll go up to 20)
                if page > 20:
                    self.logger.warning("Reached safety limit of 20 pages")
                    break
                    
            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {e}")
                break
        
        self.logger.info(f"Found {len(all_tickers)} unique tickers across {page-1} pages.")
        return all_tickers


    def clean_numeric_value(self, value):
        """
        Cleans a string value and converts it to a numeric type.
        Handles percentages, dollar signs, commas, and other formatting.
        Returns None if conversion fails.
        """
        if value is None or value == '' or value == 'N/A':
            return None
        
        # Remove common formatting characters
        cleaned = str(value).strip()
        cleaned = cleaned.replace('$', '').replace(',', '').replace('M', '').replace('B', '')
        cleaned = cleaned.replace('%', '').replace('x', '')
        
        # Handle special cases
        if cleaned in ['', '-', 'N/A', 'None']:
            return None
        
        try:
            # Try to convert to float
            return float(cleaned)
        except (ValueError, AttributeError):
            return None

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
            
            # 3. Clean numeric fields
            # These fields need to be numeric for filtering/comparison
            numeric_fields = [
                "Price", "Div Yield", "5Y Avg Yield", "Current Div", "Annualized",
                "DGR 1Y", "DGR 3Y", "DGR 5Y", "DGR 10Y", "TTR 1Y", "TTR 3Y",
                "Chowder Number", "PEG", "P/E", "P/BV", "ROE", "NPM", "ROTC",
                "Debt/Capital", "CF/Share", "Revenue 1Y", "EPS 1Y",
                "High", "Low", "Payout Ratio", "Market Cap"
            ]
            
            for field in numeric_fields:
                if field in data:
                    data[field] = self.clean_numeric_value(data[field])
            
            # 4. Ensure essential columns exist (fill with None if missing)
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
