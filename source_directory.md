
Reliability Scores/Weights:
* 1 - Official Company Reports
  * 
* 2 - Highly Trusted Industry Sources, usually cite the official document:
  * Billboard
  * Poll Star
  * Luminate
* 3 - Aggregate data sources, original source is often unclear
  * Statista
  * Business of Apps
  * Duetti

**Poll Star Weekly Magazines**
* Original File Type: pdf
* Acquisition: Manuallly downloaded and uploaded to S3 bucket
* URL: https://worldradiohistory.com/Archive-All-Music/Pollstar.htm
* Topics:
  * "Concert Pulse" table ranks top 50 artists by their average box office growth per city for the last 3 months
  * Boxoffice Summary provides dates, artist, support artist, tickets sold, and gross revenue of recent concerts
* Years Covered: 1993-1999, 2006, 2008-2010
* Challenges:
  * Had to extract the tables from PDF to CSV using python (pdfplumber and OCR)
  * Top tour data included comedians but no column
* Output File Type: CSV (later converted to parquet)

**Poll Star Reports**
* Original File Type: pdf
* Acquisition: Manually downloaded and uploaded to S3 bucket
* URL: https://data.pollstar.com/chart/yyyy/mm/file_name.pdf
* Topics:
  * Top 50-200 Worldwide/North American Tours of the year
  * End of Year Business Analysis with charts for top tours of last x years, average ticket price for last x years, total tickets sold for last x years
  * Top 200-300 Concert Grosses for a given year
  * 2024 Average Ticket Prices Around the World
  * Top Touring Artists by Gross and Tickets Sold from 1980-2022
* Challenges:
  * Included non-musical events, had to filter them out
  * Some reports were scans so text could not be extracted with pdfplumber (python library), had to use OCR and clean text
* Output File Type: CSV (later converted to parquet)

**RIAA**:
* Original File Type: CSV
* Acquisition: Manually downloaded from RIAA and uploaded to S3
* URL: https://www.riaa.com/u-s-sales-database/
* Topics:
  * US Recorded Music Revenues by Format
  * US Recorded Music Sales Volumes by Format
* Challenges:
  * Lots of empty rows, CSV's had to be cleaned
* Output format: CSV

**Statista**:
* File Type: JPEG Screenshot
* Acquisition: Manually read and typed chart/graph data to CSV
* URL: statista.com
* Topics:
  * Idek
  * Idek
  * Idek
* Challenges:
  * Requires paid subscription to download chart data/view sources
* Output: CSV

**Billboard Weekly Magazines**
* Original File Type: pdf
* Acquisition: Manually downloaded and uploaded to S3 bucket
* URL: https://worldradiohistory.com/Archive-All-Music/Billboard-Magazine.htm
* Topics:
  * Billboard Top Boxoffice displays top Arena and Auditorium events with Artist(s), Promoter, Facility, Dates, Total Ticket Sales, Ticket Price Scale, and Gross Receipts
  * Articles about music industry financials and changing trends to fill in financial estimates before digital financial statements were available
* Years Covered: 1976-2021
* Challenges:
  * Had to extract the tables from PDF to CSV using python (pdfplumber and OCR)
  * Top tour data included non musical events/artists
* Output File Type: CSV (later converted to parquet)

**Billboard Website**
* Type: Web Article
* Acquisition: Manually extracted data to music_industry_facts.csv
* Topics:
  * Spotify Financial Facts Pre-IPO
* URLs:
  * https://www.billboard.com/music/music-news/spotifys-big-losses-in-2010-are-evidence-freemium-models-need-time-to-grow-1164139/
  * https://www.billboard.com/music/music-news/report-spotify-lost-265-million-in-2009-1197248/

**SEC Edgar**
* Original File Type: PDF
* Acquisition: Manually downloaded and uploaded to S3 bucket
* URL: https://www.sec.gov/search-filings
* Challenges:
  * Due to companies changing business segment names, metrics, table formats the data was manually extracted to CSV
* Output File Type: CSV

**UK Government Financial Documents**
* Original File Type: PDF
* Acquisition: Manually downloaded and uploaded to S3 bucket
* URL: https://find-and-update.company-information.service.gov.uk/
* Specific File(s):
  * Spotify Limited Financial Statements 2010-2024
* Output File Type: CSV

**SCRIBD**
* Original File Type: PDF
* Acquisition: Manually downloaded and uploaded to S3 bucket
* URL: https://www.scribd.com/doc/44620123/Spotify-2009-Financials
* Output File Type: CSV

**Warner Music Group**
* Original File Type: PDF
* Acquisition: Manually downloaded and uploaded to S3 bucket
* URL: https://investors.wmg.com/financials/sec-filings/default.aspx
* Specific Files: Form 10-K (2005-2024)

**Sony Group Corporation**:
* Original File Type: PDF/txt
* Acquisition: Manually downloaded and uploaded to S3 bucket
* URL: https://www.sony.com/en/SonyInfo/IR/library/sec.html
* Specific Files: Form 20-F (1995-2024)

**Universal Music Group**
* Original File Type: PDF
* Acquisition: Manually downloaded and uploaded to S3
* URL: https://assets.ctfassets.net/e66ejtqbaazg/3lVdyJmpf8DQTPMRcxChSQ/80752d027f61e3846f4b5e2a5a62a958/UMG_YYYY_Annual_Report.pdf
* https://investors.universalmusic.com/reports/
* Specific Files:
  * Universal Music Group Annual Reports (years 2021, 2022, 2023, 2024)
  * Audited Combined Financial Statements for the years ended December 31st, 2020, 2019, 2018

**Apple**
* Original File Type: CSV
* Acquisition: Manually downloaded and uploaded to S3
* URL: https://investor.apple.com/sec-filings/default.aspx
* Specific Files: Apple Form 10-K (1994-2024)

**New York Times**
* Type: Web Article
* URL's:
  * https://archive.nytimes.com/mediadecoder.blogs.nytimes.com/2012/08/23/digital-notes-spotify-revenue-grew-fast-in-2011-but-losses-mounted-too/
  * https://archive.nytimes.com/mediadecoder.blogs.nytimes.com/2012/07/31/digital-notes-spotify-offers-a-bit-more-information-about-its-users/

**Music Ally**
* Type: Web Article
* URL's
  * https://musically.com/2014/11/25/spotify-2013-revenues-operating-loss/