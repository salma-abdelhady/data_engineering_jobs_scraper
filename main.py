import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import mysql.connector

BASE_URL = "https://findajob.dwp.gov.uk/search?loc=86383&q=data%20engineering&page={}"

titles, links, dates, companies, locations, salaries = [], [], [], [], [], []

page = 1
max_pages = 50

while page <= max_pages:
    url = BASE_URL.format(page)
    response = requests.get(url)
    assert response.status_code == 200

    soup = BeautifulSoup(response.content, "html.parser")
    job_listings = soup.find_all("div", class_="search-result")

    if not job_listings:
        print(f"No more job listings found after page {page-1}")
        break

    print(f"Scraping page {page}... ({len(job_listings)} jobs)")

    for job in job_listings:
        title = job.find("a", class_="govuk-link")
        if title:
            titles.append(title.text.strip())
            links.append("https://findajob.dwp.gov.uk" + title["href"])

        element = job.find_all("li")
        if element:
            dates.append(element[0].text.strip())

            company = element[1].find("strong")
            companies.append(company.text.strip() if company else None)

            location = element[1].find("span")
            locations.append(location.text.strip() if location else None)

            salary_tag = element[2].find("strong")
            salaries.append(salary_tag.text.strip() if salary_tag else None)

    time.sleep(1)
    page += 1

df = pd.DataFrame({
    "title": titles,
    "link": links,
    "date_posted": dates,
    "company_name": companies,
    "location": locations,
    "salary": salaries
})

print(f"\nTotal jobs scraped: {len(df)}")

df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')

def experience_level(title):
    title_lower = title.lower()
    if 'senior' in title_lower:
        return 'Senior'
    elif 'lead' in title_lower:
        return 'Lead'
    elif 'junior' in title_lower or 'entry' in title_lower:
        return 'Entry'
    else:
        return 'Other'

df['experience_level'] = df['title'].apply(experience_level)

def parse_salary(s):
    if not s or s == "None" or pd.isna(s):
        return np.nan, np.nan

    s = s.replace('£', '').replace(',', '').lower().strip()
    if 'to' in s:
        parts = s.split('to')
    elif '-' in s:
        parts = s.split('-')
    else:
        parts = [s, s]

    try:
        min_s = float(parts[0].split()[0].replace('k', '')) * (1000 if 'k' in parts[0] else 1)
        max_s = float(parts[1].split()[0].replace('k', '')) * (1000 if 'k' in parts[1] else 1)
        return min_s, max_s
    except:
        return np.nan, np.nan

df[['min_salary', 'max_salary']] = df['salary'].apply(lambda x: pd.Series(parse_salary(x)))
df.drop(columns=['salary'], inplace=True)

df.to_csv('job_listings.csv', index=False)

# Save to MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='YOUR_PASSWORD',
    database='jobs_db'
)
cursor = conn.cursor()

sql = """
INSERT INTO jobs_data
(title, link, date_posted, company_name, location, min_salary, max_salary, experience_level)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

data = [tuple(x) for x in df[[
    "title", "link", "date_posted", "company_name", "location", "min_salary", "max_salary", "experience_level"
]].values]

cursor.executemany(sql, data)
conn.commit()
cursor.close()
conn.close()
