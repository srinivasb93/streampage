import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import Dict, Optional


# Function to scrape Groww
def scrape_groww(url: str) -> Dict:
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    data = {}
    try:
        # Extract key metrics
        data['Fund Name'] = soup.find('h1', class_='mfh239FundName').text.strip()
        data['NAV'] = float(soup.find('div', class_='mfh239NavVal').text.replace('₹', '').replace(',', ''))
        rating = soup.find('div', class_='mfh239StarRating')
        data['Rating'] = float(rating['data-rating']) if rating else None
        # Extract returns
        returns = soup.find_all('div', class_='mfh239ReturnsValue')
        data['1Y Return'] = float(returns[0].text.replace('%', '')) if returns else None
        data['3Y Return'] = float(returns[1].text.replace('%', '')) if returns else None
    except Exception as e:
        st.error(f"Error scraping Groww: {e}")
    return data


# Function to scrape Moneycontrol
def scrape_moneycontrol(url: str) -> Dict:
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    data = {}
    try:
        data['Fund Name'] = soup.find('h1', class_='page_title').text.strip()
        nav = soup.find('span', class_='amt')
        data['NAV'] = float(nav.text.replace(',', '')) if nav else None
        returns_table = soup.find('table', class_='tblfund')
        if returns_table:
            rows = returns_table.find_all('tr')
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) > 1:
                    period = cols[0].text.strip()
                    if '1 Year' in period:
                        data['1Y Return'] = float(cols[1].text.replace('%', ''))
                    elif '3 Years' in period:
                        data['3Y Return'] = float(cols[1].text.replace('%', ''))
    except Exception as e:
        st.error(f"Error scraping Moneycontrol: {e}")
    return data


# Function to scrape ET Money
def scrape_etmoney(url: str) -> Dict:
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    data = {}
    try:
        data['Fund Name'] = soup.find('h1').text.strip()
        nav = soup.find('div', class_='val txt-bold')
        data['NAV'] = float(nav.text.replace('₹', '').replace(',', '')) if nav else None
        returns = soup.find_all('div', class_='mf-fund-performance__returns-value')
        data['1Y Return'] = float(returns[0].text.replace('%', '')) if returns else None
        data['3Y Return'] = float(returns[2].text.replace('%', '')) if len(returns) > 2 else None
    except Exception as e:
        st.error(f"Error scraping ET Money: {e}")
    return data


# Function to suggest best fund
def suggest_best_fund(funds_data: list) -> str:
    if not funds_data:
        return "No data available to compare."

    # Simple scoring: Higher 3Y return gets more weight, then 1Y return, then NAV
    best_fund = max(
        funds_data,
        key=lambda x: (
                (x.get('3Y Return', 0) or 0) * 0.6 +
                (x.get('1Y Return', 0) or 0) * 0.3 +
                (x.get('NAV', 0) or 0) * 0.1 / 1000  # Scale down NAV impact
        ),
        default=None
    )
    return best_fund['Fund Name'] if best_fund else "No clear winner."


# Streamlit App
st.title("Mutual Fund Analyzer")

# Predefined URLs
urls = {
    "Canara Robeco Small Cap (Groww)": "https://groww.in/mutual-funds/canara-robeco-small-cap-fund-direct-growth",
    "Canara Robeco Small Cap (Moneycontrol)": "https://www.moneycontrol.com/mutual-funds/nav/canara-robeco-small-cap-fund-direct-plan-growth/MCA312",
    "Bandhan Small Cap (ET Money)": "https://www.etmoney.com/mutual-funds/bandhan-small-cap-fund-direct-growth/41037"
}

# Allow user to input custom URL
custom_url = st.text_input("Enter a custom mutual fund URL (optional):")

# Scrape data
if st.button("Analyze Funds"):
    funds_data = []

    # Scrape predefined URLs
    with st.spinner("Scraping data..."):
        for name, url in urls.items():
            if "groww.in" in url:
                data = scrape_groww(url)
            elif "moneycontrol.com" in url:
                data = scrape_moneycontrol(url)
            elif "etmoney.com" in url:
                data = scrape_etmoney(url)
            if data:
                funds_data.append(data)

        # Scrape custom URL if provided
        if custom_url:
            if "groww.in" in custom_url:
                data = scrape_groww(custom_url)
            elif "moneycontrol.com" in custom_url:
                data = scrape_moneycontrol(custom_url)
            elif "etmoney.com" in custom_url:
                data = scrape_etmoney(custom_url)
            else:
                st.warning("Custom URL not recognized. Supported sites: Groww, Moneycontrol, ET Money.")
            if data:
                funds_data.append(data)

    # Display results
    if funds_data:
        df = pd.DataFrame(funds_data)
        st.subheader("Fund Comparison")
        st.dataframe(df)

        # Suggest best fund
        best_fund = suggest_best_fund(funds_data)
        st.subheader("Recommendation")
        st.write(f"Based on returns and NAV, the suggested fund is: **{best_fund}**")
    else:
        st.error("No data could be scraped. Please check the URLs or try again later.")

# Instructions
st.sidebar.title("How to Use")
st.sidebar.write("""
1. Click 'Analyze Funds' to scrape data from predefined URLs.
2. Optionally, enter a custom URL from Groww, Moneycontrol, or ET Money.
3. View the comparison table and recommendation.
""")